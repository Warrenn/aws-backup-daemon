#!/bin/bash

set -euo pipefail

mkdir -p "./ca" "./client"

# Generate CA private key and self-signed certificate
openssl req -x509 -newkey rsa:2048 -days 3650 -nodes \
  -keyout "./ca/ca.key" -out "./ca/ca.pem" \
  -config "./ca.cnf"

# Generate client key and CSR
openssl req -newkey rsa:2048 -nodes -keyout "./client/client.key" \
  -out "./client/client.csr" -subj "/CN=aws-backup-client" \
  -config "./client.cnf"

# Sign the client certificate with CA
openssl x509 -req -in "./client/client.csr" \
  -CA "./ca/ca.pem" -CAkey "./ca/ca.key" -CAcreateserial \
  -out "./client/client.pem" -days 365 -sha256 \
  -extfile "./client.cnf" -extensions v3_req