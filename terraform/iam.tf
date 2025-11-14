# IAM Role for Preprocessing Job
resource "aws_iam_role" "job_role" {
  name = "${var.project_name}-${var.job_name}-job-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = {
    Project = var.project_name
    Job     = var.job_name
  }
}

# S3 permissions (full bucket access)
resource "aws_iam_role_policy" "s3_access" {
  name = "${var.job_name}-s3-access"
  role = aws_iam_role.job_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ]
      Resource = [
        "arn:aws:s3:::${data.aws_ssm_parameter.analytics_bucket.value}",
        "arn:aws:s3:::${data.aws_ssm_parameter.analytics_bucket.value}/*"
      ]
    }]
  })
}

# Attach shared policies from central repo
resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.job_role.name
  policy_arn = data.aws_iam_policy.ssm_policy.arn
}

resource "aws_iam_role_policy_attachment" "cloudwatch" {
  role       = aws_iam_role.job_role.name
  policy_arn = data.aws_iam_policy.cloudwatch_policy.arn
}

# IAM Role for ECS Task Execution (pulls secrets and logs)
resource "aws_iam_role" "execution_role" {
  name = "${var.project_name}-${var.job_name}-execution-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "ecs-tasks.amazonaws.com"
      }
    }]
  })

  tags = {
    Project = var.project_name
    Job     = var.job_name
  }
}

# Allow execution role to pull secrets from SSM
resource "aws_iam_role_policy" "execution_secrets" {
  name = "${var.job_name}-execution-secrets"
  role = aws_iam_role.execution_role.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "ssm:GetParameters",
          "ssm:GetParameter"
        ]
        Resource = [
          data.aws_ssm_parameter.db_host.arn,
          data.aws_ssm_parameter.db_username.arn,
          data.aws_ssm_parameter.db_password.arn
        ]
      },
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:GetAuthorizationToken"
        ]
        Resource = "*"
      },
      {
        Effect = "Allow"
        Action = [
          "ecr:BatchCheckLayerAvailability",
          "ecr:GetDownloadUrlForLayer",
          "ecr:BatchGetImage"
        ]
        Resource = "arn:aws:ecr:${var.aws_region}:*:repository/ws_preprocessing"
      }
    ]
  })
}

