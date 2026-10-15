#!/bin/bash
# check_leftovers.sh — read-only: is anything from the EKS stack still running (and billing)?
# Run after every destroy, and whenever in doubt. Uses whatever AWS credentials the shell has.
#   bash infra/eks/check_leftovers.sh
R=${AWS_DEFAULT_REGION:-us-west-2}
echo "region $R"
echo "EKS clusters:            $(aws eks list-clusters --region "$R" --query 'clusters' --output text)"
echo "EC2 instances (cms-dqm):  $(aws ec2 describe-instances --region "$R" --filters Name=tag:Project,Values=cms-dqm \
  Name=instance-state-name,Values=pending,running,stopping,stopped --query 'Reservations[].Instances[].[InstanceId,InstanceType,State.Name]' --output text)"
echo "NAT gateways:            $(aws ec2 describe-nat-gateways --region "$R" --filter Name=state,Values=pending,available --query 'NatGateways[].NatGatewayId' --output text)"
echo "Load balancers:          $(aws elbv2 describe-load-balancers --region "$R" --query 'LoadBalancers[].LoadBalancerName' --output text)"
echo "Unattached EBS volumes:  $(aws ec2 describe-volumes --region "$R" --filters Name=status,Values=available --query 'Volumes[].[VolumeId,Size]' --output text)"
echo "Watchdog Lambda:         $(aws lambda list-functions --region "$R" --query "Functions[?starts_with(FunctionName, 'dqm-eks')].FunctionName" --output text)"
echo "Watchdog schedule:       $(aws events list-rules --region "$R" --name-prefix dqm-eks --query 'Rules[].Name' --output text)"
echo "Watchdog state:          $(aws ssm get-parameters-by-path --region "$R" --path /dqm/watchdog --query 'Parameters[].Name' --output text)"
echo "(empty lines = nothing left; anything listed is still billing or should be gone)"
