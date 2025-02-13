#!/bin/sh

# Wait for Dremio to start
echo "Waiting for Dremio to become available..."
until curl -s http://localhost:9047/ | grep -q "Dremio"; do
  sleep 10
done
echo "Dremio is ready!"

# Admin login - Adjust with your actual credentials
ADMIN_USER="dremio"
ADMIN_PASS="dremio123"  # Replace with the actual admin password

# Get authentication token
AUTH_RESPONSE=$(curl -s -X POST "http://localhost:9047/apiv2/login" \
  -H "Content-Type: application/json" \
  -d "{\"userName\": \"$ADMIN_USER\", \"password\": \"$ADMIN_PASS\"}")

TOKEN=$(echo "$AUTH_RESPONSE" | jq -r '.token')

# Verify token retrieval
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "Failed to authenticate with Dremio. Check credentials."
  exit 1
fi

echo "Authenticated with Dremio, token retrieved."

# Create Nessie Source
curl -X POST "http://localhost:9047/api/v3/source" \
  -H "Authorization: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "nessie",
    "type": "NESSIE",
    "config": {
      "nessieEndpoint": "http://nessie:19120/api/v2",
      "nessieAuthType": "NONE",
      "credentialType": "ACCESS_KEY",
      "awsRootPath": "commerce/warehouse",
      "awsAccessKey": "minio",
      "awsAccessSecret": "minio123",
      "secure": false,
      "propertyList": [
        {
        "name": "fs.s3a.path.style.access",
        "value": "true"
        },
        {
        "name": "fs.s3a.endpoint",
        "value": "http://minio:9000"
        },
        {
        "name": "dremio.s3.compat",
        "value": "true"
        }
      ]
    }
  }'

echo "Nessie source added successfully!"
