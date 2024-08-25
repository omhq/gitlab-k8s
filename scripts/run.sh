#!/bin/bash

set -e

source ./scripts/init.sh
aws sts get-caller-identity
exec  python3 ./src/main.py -m /workspace/job.yml
