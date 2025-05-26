#!/usr/bin/env bash
echo running post install scripts..;

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

export AWS_ACCESS_KEY_ID=localstack
export AWS_SECRET_ACCESS_KEY=localstack
export AWS_ENDPOINT="http://localhost:4566"

aws --endpoint-url=$AWS_ENDPOINT s3 mb s3://regulatory-dape

aws --endpoint-url=$AWS_ENDPOINT s3 cp $SCRIPT_DIR/files/bet.csv s3://regulatory-dape/input/bet.csv
aws --endpoint-url=$AWS_ENDPOINT s3 cp $SCRIPT_DIR/files/client.csv s3://regulatory-dape/input/client.csv
aws --endpoint-url=$AWS_ENDPOINT s3 cp $SCRIPT_DIR/files/ticket.csv s3://regulatory-dape/input/ticket.csv
aws --endpoint-url=$AWS_ENDPOINT s3 cp $SCRIPT_DIR/files/ticket_bet.csv s3://regulatory-dape/input/ticket_bet.csv
