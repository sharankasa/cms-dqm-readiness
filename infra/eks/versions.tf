terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.50" # same as aws-foundation; EKS access entries need >= 5.33
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.4" # packages the cost watchdog Lambda (watchdog.tf)
    }
  }
  # Local state: this stack lives for about an hour and is destroyed after each run.
  # State files are gitignored. If state is ever lost, run check_leftovers.sh.
}

provider "aws" {
  region  = var.region
  profile = var.aws_profile

  default_tags {
    tags = {
      Project      = "cms-dqm"
      Owner        = "sai"
      ManagedBy    = "terraform"
      Stack        = "infra/eks"
      Purpose      = var.purpose
      AutoShutdown = "true" # short-lived: destroy after the run
    }
  }
}

# The AWS Price List API is only served in us-east-1 (and ap-south-1): used to read the node price.
provider "aws" {
  alias   = "pricing"
  region  = "us-east-1"
  profile = var.aws_profile
}
