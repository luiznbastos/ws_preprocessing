variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "ws-analytics"
}

variable "job_name" {
  description = "Job name"
  type        = string
  default     = "preprocessing"
}

variable "vcpus" {
  description = "Number of vCPUs for the job"
  type        = number
  default     = 2
}

variable "memory" {
  description = "Memory in MB for the job"
  type        = number
  default     = 6144
}

variable "timeout_seconds" {
  description = "Job timeout in seconds"
  type        = number
  default     = 7200
}



