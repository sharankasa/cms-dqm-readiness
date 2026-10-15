"""
watchdog.py — cost guard for the short-lived p2 EKS cluster (runs in AWS Lambda every 10 minutes,
independent of the laptop). Created and destroyed with the cluster (infra/eks/watchdog.tf).

Each run:
  1. adds the cost since the last run to a running total, at the CURRENT node count:
       (minutes since last run / 60) x (control-plane $/h + nodes x node $/h)
     The total and the time of the last run are kept in an SSM parameter created and destroyed with
     the stack (STATE_PARAM), so the Lambda holds no state and Terraform sees no drift.
  2. warns by SNS (dqm-alerts -> email) when the total passes WARN_FRACTION of the cap, once per cap;
  3. at the cap, or when the cluster is older than MAX_HOURS, scales every node group to 0 nodes
     (compute cost stops; running jobs pause and resume later from their saved results) and alerts.
     It does NOT delete the cluster (Terraform owns it): the alert says to destroy it, and repeats
     every 6 hours while the cluster still exists.
The estimate counts EC2 nodes and the EKS fee only (not S3, Athena or data transfer, which are cents).
"""
import datetime
import json
import os

import boto3

eks = boto3.client("eks")
sns = boto3.client("sns")
ssm = boto3.client("ssm")

CLUSTER = os.environ["CLUSTER"]
TOPIC = os.environ["TOPIC_ARN"]
CAP = float(os.environ["COST_CAP_USD"])
MAX_HOURS = float(os.environ["MAX_HOURS"])
NODE_H = float(os.environ["NODE_HOUR_USD"])
CP_H = float(os.environ.get("CONTROL_PLANE_HOUR_USD", "0.10"))
WARN = float(os.environ.get("WARN_FRACTION", "0.8"))
STATE_PARAM = os.environ["STATE_PARAM"]
REPEAT_H = 6.0


def now():
    return datetime.datetime.now(datetime.timezone.utc)


def publish(subject, lines):
    sns.publish(TopicArn=TOPIC, Subject=subject[:99], Message="\n".join(lines))


def handler(event, context):
    try:
        c = eks.describe_cluster(name=CLUSTER)["cluster"]
    except eks.exceptions.ResourceNotFoundException:
        return {"status": "no cluster"}
    try:
        tags = json.loads(ssm.get_parameter(Name=STATE_PARAM)["Parameter"]["Value"] or "{}")
    except (ssm.exceptions.ParameterNotFound, ValueError):
        tags = {}
    if tags.get("cluster_created") not in (None, c["createdAt"].isoformat()):
        tags = {}   # a new cluster with the same name: start a new total
    t = now()
    last = datetime.datetime.fromisoformat(tags["WatchdogLast"]) if "WatchdogLast" in tags else c["createdAt"]
    spent = float(tags.get("WatchdogSpentUSD", "0"))
    groups = eks.list_nodegroups(clusterName=CLUSTER).get("nodegroups", [])
    nodes = 0
    for g in groups:
        nodes += eks.describe_nodegroup(clusterName=CLUSTER, nodegroupName=g)["nodegroup"]["scalingConfig"].get("desiredSize", 0)
    hours = max(0.0, (t - last).total_seconds() / 3600)
    spent += hours * (CP_H + nodes * NODE_H)
    age_h = (t - c["createdAt"]).total_seconds() / 3600
    new_tags = dict(tags, WatchdogSpentUSD=f"{spent:.4f}", WatchdogLast=t.isoformat(),
                    cluster_created=c["createdAt"].isoformat())
    info = [f"cluster {CLUSTER}: estimated spend ${spent:.2f} of cap ${CAP:.2f}; age {age_h:.1f} h of max {MAX_HOURS:.1f} h;",
            f"{nodes} node(s) now at ${NODE_H:.4f}/h each + EKS fee ${CP_H:.2f}/h.",
            "Estimate covers EC2 nodes and the EKS fee only. Check: Billing console > Bills, and infra/eks/check_leftovers.sh."]

    if spent >= CAP or age_h >= MAX_HOURS:
        reason = "cost cap reached" if spent >= CAP else "maximum cluster age reached"
        if nodes > 0:
            for g in groups:
                sc = eks.describe_nodegroup(clusterName=CLUSTER, nodegroupName=g)["nodegroup"]["scalingConfig"]
                eks.update_nodegroup_config(clusterName=CLUSTER, nodegroupName=g,
                                            scalingConfig={"minSize": 0, "desiredSize": 0, "maxSize": max(1, sc.get("maxSize", 1))})
            publish(f"[p2 watchdog] STOPPED nodes of {CLUSTER}: {reason}",
                    [f"ACTION TAKEN: all node groups scaled to 0 ({reason})."] + info +
                    ["Jobs pause and resume from their saved results if nodes come back.",
                     "Next: decide, then destroy the cluster (infra/eks README, step 4) or raise the cap deliberately."])
            new_tags["WatchdogStopped"] = t.isoformat()
            new_tags["WatchdogNotified"] = t.isoformat()
        else:
            last_note = tags.get("WatchdogNotified")
            if not last_note or (t - datetime.datetime.fromisoformat(last_note)).total_seconds() / 3600 >= REPEAT_H:
                publish(f"[p2 watchdog] {CLUSTER} still exists after stop",
                        ["Nodes are at 0 but the cluster still costs the EKS fee. Destroy it (infra/eks README, step 4)."] + info)
                new_tags["WatchdogNotified"] = t.isoformat()
    elif spent >= WARN * CAP and tags.get("WatchdogWarnedCap") != f"{CAP:.2f}":
        # once per cap: raising the cap re-arms the warning (2026-10-12: no warning came at the raised cap)
        publish(f"[p2 watchdog] WARNING {CLUSTER} at {100 * spent / CAP:.0f}% of its cost cap", info)
        new_tags["WatchdogWarned"] = t.isoformat()
        new_tags["WatchdogWarnedCap"] = f"{CAP:.2f}"

    ssm.put_parameter(Name=STATE_PARAM, Value=json.dumps(new_tags), Type="String", Overwrite=True)
    return {"spent": round(spent, 4), "age_h": round(age_h, 2), "nodes": nodes}
