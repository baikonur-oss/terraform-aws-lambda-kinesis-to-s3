locals {
  package_filename = "package.zip"
}

data "external" "package" {
  program = ["curl", "-O", "${local.package_filename}", "${var.lambda_package_url}"]
}

data "aws_s3_bucket" "logs" {
  bucket = "${var.log_bucket}"
}

resource "aws_cloudwatch_log_group" "logs" {
  name = "/aws/lambda/${var.name}"

  retention_in_days = "${var.log_retention_in_days}"
}

resource "aws_lambda_function" "function" {
  function_name = "${var.name}"
  handler       = "${var.handler}"
  role          = "${module.iam.arn}"
  runtime       = "${var.runtime}"
  memory_size   = "${var.memory}"
  timeout       = "${var.timeout}"

  filename = "${local.package_filename}"

  tracing_config {
    mode = "${var.tracing_mode}"
  }

  environment {
    variables {
      TZ = "${var.timezone}"

      LOG_ID_FIELD        = "${var.log_id_field}"
      LOG_TYPE_FIELD      = "${var.log_type_field}"
      LOG_TIMESTAMP_FIELD = "${var.log_timestamp_field}"

      LOG_S3_BUCKET = "${var.log_bucket}"
      LOG_S3_PREFIX = "${var.log_path_prefix}"
    }
  }

  tags = "${var.tags}"
}

resource "aws_lambda_event_source_mapping" "kinesis_mapping" {
  batch_size        = "${var.max_batch_size}"
  event_source_arn  = "${var.kinesis_stream_arn}"
  enabled           = true
  function_name     = "${aws_lambda_function.function.arn}"
  starting_position = "${var.starting_position}"
}

resource "aws_iam_role_policy_attachment" "xray_access" {
  policy_arn = "arn:aws:iam::aws:policy/AWSXrayWriteOnlyAccess"
  role       = "${module.iam.name}"
}

module "iam" {
  source  = "baikonur-oss/iam-nofile/aws"
  version = "v1.0.1"

  type = "lambda"
  name = "${var.name}"

  policy_json = <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "kinesis:DescribeStream",
                "kinesis:GetShardIterator",
                "kinesis:GetRecords",
                "kinesis:ListStreams"
            ],
            "Resource": [
                "${var.kinesis_stream_arn}"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogStream",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:PutLogEvents"
            ],
            "Resource": [
                "arn:aws:logs:*:*:*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject"
            ],
            "Resource": [
                "${data.aws_s3_bucket.logs.arn}/*"
            ]
        }
    ]
}
EOF
}
