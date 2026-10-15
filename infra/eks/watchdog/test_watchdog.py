"""Offline test of watchdog.py with fake AWS clients (no AWS calls): python3 infra/eks/watchdog/test_watchdog.py"""
import datetime
import importlib
import os
import sys
import types

HERE = os.path.dirname(os.path.abspath(__file__))
T0 = datetime.datetime(2026, 10, 12, 0, 0, tzinfo=datetime.timezone.utc)


class NotFound(Exception):
    pass


class FakeEKS:
    exceptions = types.SimpleNamespace(ResourceNotFoundException=NotFound)

    def __init__(self):
        self.nodes, self.updates = 4, []

    def describe_cluster(self, name):
        return {"cluster": {"arn": "arn:x", "createdAt": T0}}

    def list_nodegroups(self, clusterName):
        return {"nodegroups": ["workers"]}

    def describe_nodegroup(self, clusterName, nodegroupName):
        return {"nodegroup": {"scalingConfig": {"desiredSize": self.nodes, "maxSize": 4}}}

    def update_nodegroup_config(self, clusterName, nodegroupName, scalingConfig):
        self.updates.append(scalingConfig)
        self.nodes = scalingConfig["desiredSize"]


class FakeSSM:
    exceptions = types.SimpleNamespace(ParameterNotFound=KeyError)

    def __init__(self):
        self.value = "{}"

    def get_parameter(self, Name):
        return {"Parameter": {"Value": self.value}}

    def put_parameter(self, Name, Value, Type, Overwrite):
        self.value = Value


class FakeSNS:
    def __init__(self):
        self.sent = []

    def publish(self, TopicArn, Subject, Message):
        self.sent.append(Subject)


def main():
    os.environ.update(CLUSTER="dqm-eks", TOPIC_ARN="arn:topic", COST_CAP_USD="2", MAX_HOURS="10",
                      NODE_HOUR_USD="0.10", CONTROL_PLANE_HOUR_USD="0.10", STATE_PARAM="/dqm/watchdog")
    eks, ssm, snsc = FakeEKS(), FakeSSM(), FakeSNS()
    fake_boto3 = types.SimpleNamespace(client=lambda n: {"eks": eks, "ssm": ssm, "sns": snsc}[n])
    sys.modules["boto3"] = fake_boto3
    sys.path.insert(0, HERE)
    w = importlib.import_module("watchdog")
    clock = {"t": T0}
    w.now = lambda: clock["t"]
    out = []
    for h in [1, 2, 3, 4, 5, 11, 17, 18]:       # hours after creation; burn = 0.10 + 4 x 0.10 = $0.50/h
        clock["t"] = T0 + datetime.timedelta(hours=h)
        r = w.handler({}, None)
        out.append((h, r["spent"], r["nodes"], list(snsc.sent)))
    for row in out:
        print(row)
    # $0.50/h with 4 nodes: $1.50 at 3 h (below the 80% warning), $2.00 at 4 h = cap -> nodes to 0;
    # afterwards only the EKS fee ($0.10/h) accrues and a reminder goes out every 6 h
    assert out[2][1] == 1.5 and not out[2][3], "no alert below 80%"
    assert any("STOPPED" in s for s in out[3][3]) and out[4][2] == 0, "nodes scaled to 0 at the cap"
    assert eks.updates and eks.updates[0]["desiredSize"] == 0
    # after stop: only the EKS fee accrues; reminder every 6 h while the cluster exists
    assert sum("still exists" in s for s in snsc.sent) >= 1
    print("watchdog test: PASS")


if __name__ == "__main__":
    main()
