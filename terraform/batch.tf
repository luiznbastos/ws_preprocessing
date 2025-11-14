# AWS Batch Job Definition for Preprocessing
resource "aws_batch_job_definition" "this" {
  name                  = "${var.project_name}-${var.job_name}"
  type                  = "container"
  platform_capabilities = ["EC2"]

  container_properties = jsonencode({
    image            = "${data.aws_ssm_parameter.ecr_url.value}:latest"
    vcpus            = var.vcpus
    memory           = var.memory
    jobRoleArn       = aws_iam_role.job_role.arn
    executionRoleArn = aws_iam_role.execution_role.arn

    environment = [
      {
        name  = "S3_BUCKET"
        value = data.aws_ssm_parameter.analytics_bucket.value
      },
      {
        name  = "DB_NAME"
        value = data.aws_ssm_parameter.db_name.value
      },
      {
        name  = "JOB_NAME"
        value = var.job_name
      }
    ]

    secrets = [
      {
        name      = "DB_HOST"
        valueFrom = data.aws_ssm_parameter.db_host.arn
      },
      {
        name      = "DB_USERNAME"
        valueFrom = data.aws_ssm_parameter.db_username.arn
      },
      {
        name      = "DB_PASSWORD"
        valueFrom = data.aws_ssm_parameter.db_password.arn
      }
    ]

    logConfiguration = {
      logDriver = "awslogs"
      options = {
        "awslogs-group"         = "/aws/batch/${var.project_name}"
        "awslogs-region"        = var.aws_region
        "awslogs-stream-prefix" = var.job_name
      }
    }
  })

  retry_strategy {
    attempts = 3
  }

  timeout {
    attempt_duration_seconds = var.timeout_seconds
  }

  tags = {
    Name    = "${var.project_name}-${var.job_name}-job"
    Project = var.project_name
    Job     = var.job_name
  }
}

# Store job ARN for orchestrator
resource "aws_ssm_parameter" "job_arn" {
  name  = "/${var.project_name}/batch/jobs/${var.job_name}/arn"
  type  = "String"
  value = aws_batch_job_definition.this.arn

  tags = {
    Name    = "${var.project_name}-${var.job_name}-job-arn-param"
    Project = var.project_name
  }
}



