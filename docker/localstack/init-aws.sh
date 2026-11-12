#!/bin/bash
# Cria o tópico SNS, as filas SQS (com DLQ após 3 recebimentos) e as assinaturas do fan-out.
set -euo pipefail

REGIAO=us-east-1
CONTA=000000000000
TOPICO_ARN=$(awslocal sns create-topic --name decisao-credito --query TopicArn --output text)

for FILA in fila-email fila-push; do
  awslocal sqs create-queue --queue-name "${FILA}-dlq" >/dev/null
  DLQ_ARN="arn:aws:sqs:${REGIAO}:${CONTA}:${FILA}-dlq"
  awslocal sqs create-queue --queue-name "${FILA}" --attributes "{
    \"VisibilityTimeout\": \"30\",
    \"RedrivePolicy\": \"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"3\\\"}\"
  }" >/dev/null
  awslocal sns subscribe --topic-arn "${TOPICO_ARN}" --protocol sqs \
    --notification-endpoint "arn:aws:sqs:${REGIAO}:${CONTA}:${FILA}" \
    --attributes RawMessageDelivery=true >/dev/null
  echo "Fila ${FILA} (DLQ ${FILA}-dlq) assinada em ${TOPICO_ARN}"
done
