# --- Cost watchdog (created and destroyed with the cluster) --------------------------------------
# A Lambda run every 10 minutes by EventBridge, independent of the laptop. It keeps a running estimate
# of this cluster's cost (EKS fee + nodes x the live on-demand price), warns by SNS at 80% of
# var.cost_cap_usd, and at the cap or at var.max_hours scales every node group to 0 and alerts again
# (every 6 h while the cluster still exists). Alerts go to the account's existing topic
# (dqm-alerts -> email). It never deletes the cluster: Terraform owns it.

# Live on-demand price of the node type (AWS Price List API; only in us-east-1). Skipped when
# var.node_hour_usd is set.
data "aws_pricing_product" "node" {
  count        = var.node_hour_usd > 0 ? 0 : 1
  provider     = aws.pricing
  service_code = "AmazonEC2"
  filters {
    field = "instanceType"
    value = var.node_instance_type
  }
  filters {
    field = "location"
    value = "US West (Oregon)"
  }
  filters {
    field = "operatingSystem"
    value = "Linux"
  }
  filters {
    field = "tenancy"
    value = "Shared"
  }
  filters {
    field = "preInstalledSw"
    value = "NA"
  }
  filters {
    field = "capacitystatus"
    value = "Used"
  }
  filters {
    field = "licenseModel"
    value = "No License required"
  }
}

locals {
  node_hour_usd = var.node_hour_usd > 0 ? var.node_hour_usd : tonumber(
    values(values(jsondecode(data.aws_pricing_product.node[0].result).terms.OnDemand)[0].priceDimensions)[0].pricePerUnit.USD
  )
}

data "aws_sns_topic" "alerts" {
  name = var.alerts_topic_name
}

data "aws_kms_alias" "alerts" {
  name = "alias/dqm-sns-alerts"
}

data "archive_file" "watchdog" {
  type        = "zip"
  source_file = "${path.module}/watchdog/watchdog.py"
  output_path = "${path.module}/.build/watchdog.zip"
}

resource "aws_ssm_parameter" "watchdog_state" {
  name  = "/dqm/watchdog/${var.cluster_name}"
  type  = "String"
  value = "{}"
  lifecycle {
    ignore_changes = [value] # the Lambda writes its running total here
  }
}

resource "aws_iam_role" "watchdog" {
  name = "${var.cluster_name}-watchdog"
  assume_role_policy = jsonencode({
    Version   = "2012-10-17"
    Statement = [{ Effect = "Allow", Principal = { Service = "lambda.amazonaws.com" }, Action = "sts:AssumeRole" }]
  })
}

resource "aws_iam_role_policy_attachment" "watchdog_logs" {
  role       = aws_iam_role.watchdog.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "watchdog" {
  name = "watchdog"
  role = aws_iam_role.watchdog.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ThisClusterOnly"
        Effect   = "Allow"
        Action   = ["eks:DescribeCluster", "eks:ListNodegroups", "eks:DescribeNodegroup", "eks:UpdateNodegroupConfig"]
        Resource = [aws_eks_cluster.this.arn, "arn:aws:eks:${var.region}:${data.aws_caller_identity.me.account_id}:nodegroup/${var.cluster_name}/*"]
      },
      {
        Sid      = "State"
        Effect   = "Allow"
        Action   = ["ssm:GetParameter", "ssm:PutParameter"]
        Resource = aws_ssm_parameter.watchdog_state.arn
      },
      {
        Sid      = "Alert"
        Effect   = "Allow"
        Action   = "sns:Publish"
        Resource = data.aws_sns_topic.alerts.arn
      },
      {
        Sid      = "AlertTopicKey"
        Effect   = "Allow"
        Action   = ["kms:GenerateDataKey*", "kms:Decrypt"]
        Resource = data.aws_kms_alias.alerts.target_key_arn
      }
    ]
  })
}

resource "aws_lambda_function" "watchdog" {
  function_name    = "${var.cluster_name}-watchdog"
  role             = aws_iam_role.watchdog.arn
  runtime          = "python3.12"
  handler          = "watchdog.handler"
  filename         = data.archive_file.watchdog.output_path
  source_code_hash = data.archive_file.watchdog.output_base64sha256
  timeout          = 60
  environment {
    variables = {
      CLUSTER                = aws_eks_cluster.this.name
      TOPIC_ARN              = data.aws_sns_topic.alerts.arn
      COST_CAP_USD           = tostring(var.cost_cap_usd)
      MAX_HOURS              = tostring(var.max_hours)
      NODE_HOUR_USD          = tostring(local.node_hour_usd)
      CONTROL_PLANE_HOUR_USD = "0.10"
      WARN_FRACTION          = "0.8"
      STATE_PARAM            = aws_ssm_parameter.watchdog_state.name
    }
  }
  depends_on = [aws_iam_role_policy_attachment.watchdog_logs]
}

resource "aws_cloudwatch_event_rule" "watchdog" {
  name                = "${var.cluster_name}-watchdog"
  schedule_expression = "rate(10 minutes)"
}

resource "aws_cloudwatch_event_target" "watchdog" {
  rule = aws_cloudwatch_event_rule.watchdog.name
  arn  = aws_lambda_function.watchdog.arn
}

resource "aws_lambda_permission" "watchdog" {
  statement_id  = "AllowEventBridge"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.watchdog.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.watchdog.arn
}

# If the watchdog itself fails (e.g. a permission problem), the guard is off: Lambda sends the failed
# event to the alert topic (same execution role, so no change to the topic's or key's policies).
resource "aws_lambda_function_event_invoke_config" "watchdog" {
  function_name          = aws_lambda_function.watchdog.function_name
  maximum_retry_attempts = 2
  destination_config {
    on_failure {
      destination = data.aws_sns_topic.alerts.arn
    }
  }
}
