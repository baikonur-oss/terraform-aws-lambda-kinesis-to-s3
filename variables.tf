variable "memory" {
  description = "Lambda Function memory in megabytes"
  default     = 256
}

variable "timeout" {
  description = "Lambda Function timeout in seconds"
  default     = 60
}

variable "max_batch_size" {
  description = "Maximum number of records passed for a single Lambda invocation"
}

variable "kinesis_stream_arn" {
  description = "Source Kinesis Data Streams stream name"
}

variable "log_bucket" {
  description = "Target S3 bucket to save data to"
}

variable "log_path_prefix" {
  description = "Log file path prefix"
}

variable "lambda_package_url" {
  description = "Lambda package URL (see Usage in README)"
}

variable "handler" {
  description = "Lambda Function handler (entrypoint)"
  default     = "main.handler"
}

variable "runtime" {
  description = "Lambda Function runtime"
  default     = "python3.6"
}

variable "name" {
  description = "Resource name"
}

variable "starting_position" {
  description = "Kinesis ShardIterator type (see: https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html )"
  default     = "TRIM_HORIZON"
}

variable "log_id_field" {
  description = "Key name for unique log ID"
  default     = "log_id"
}

variable "log_type_field" {
  description = "Key name for log type"
  default     = "log_type"
}

variable "log_timestamp_field" {
  description = "Key name for log timestamp"
  default     = "time"
}

variable "tracing_mode" {
  description = "X-Ray tracing mode (see: https://docs.aws.amazon.com/lambda/latest/dg/API_TracingConfig.html )"
  default     = "PassThrough"
}

variable "tags" {
  description = "Resource tags"
  type        = "map"
  default     = {}
}
