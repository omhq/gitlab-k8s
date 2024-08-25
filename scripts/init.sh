#!/bin/bash

credentials=$(aws sts assume-role \
  --role-arn "${AWS_ROLE_ARN}" \
  --role-session-name "GitLabRunner-${CI_PROJECT_ID}-${CI_PIPELINE_ID}" \
  --duration-seconds 14400 \
  --query 'Credentials.[AccessKeyId,SecretAccessKey,SessionToken]' \
  --output text)

read -r AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN <<< "$credentials"

export AWS_ACCESS_KEY_ID
export AWS_SECRET_ACCESS_KEY
export AWS_SESSION_TOKEN

aws configure set default.region "$AWS_DEFAULT_REGION"
