#!/bin/sh
set -e

POLARIS_HOST="${POLARIS_HOST:-polaris}"
POLARIS_URL="http://${POLARIS_HOST}:8181"
CLIENT_ID="${CLIENT_ID:-root}"
CLIENT_SECRET="${CLIENT_SECRET:-s3cr3t}"
CATALOG_NAME="${CATALOG_NAME:-iceberg}"
REALM="${REALM:-POLARIS}"

echo "Waiting for Polaris at $POLARIS_URL..."
until curl -f "http://${POLARIS_HOST}:8182/q/health" > /dev/null 2>&1; do
  echo "...waiting for Polaris..."
  error=$(curl -fsS "http://${POLARIS_HOST}:8182/q/health" -o /dev/null 2>&1)
  if [ $? -ne 0 ]; then
      echo "Failed at $(date): $error";
  fi
  sleep 5
done
echo "Polaris is up"

echo "Obtaining root access token from $POLARIS_URL..."
TOKEN_RESPONSE=$(curl --fail-with-body -s -S -X POST "$POLARIS_URL/api/catalog/v1/oauth/tokens" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=client_credentials&client_id=${CLIENT_ID}&client_secret=${CLIENT_SECRET}&scope=PRINCIPAL_ROLE:ALL" 2>&1) || {
  echo "Failed to obtain access token"
  echo "$TOKEN_RESPONSE" >&2
  exit 1
}

TOKEN=$(echo "$TOKEN_RESPONSE" | jq -r '.access_token')
if [ -z "$TOKEN" ] || [ "$TOKEN" = "null" ]; then
  echo "Failed to parse access token from response"
  echo "$TOKEN_RESPONSE"
  exit 1
fi
echo "Obtained access token"

STATUS=$(curl \
  -s \
  -o /dev/null \
  -w "%{http_code}" \
  "$POLARIS_URL/api/catalog/v1/config?warehouse=$CATALOG_NAME" \
  -H "Authorization: Bearer $TOKEN") || true

echo "Catalog check status: $STATUS"

if [ "$STATUS" != "200" ]; then
  echo "Creating catalog '$CATALOG_NAME' in realm $REALM..."
  PAYLOAD='{
    "catalog": {
      "name": "'"$CATALOG_NAME"'",
      "type": "INTERNAL",
      "readOnly": false,
      "properties": {
        "default-base-location": "s3://iceberg"
      },
      "storageConfigInfo": {
        "storageType": "S3",
        "allowedLocations": ["s3://iceberg"],
        "endpoint": "http://localhost:9000",
        "endpointInternal": "http://minio:9000",
        "pathStyleAccess": true
      }
    }
  }'

  RESPONSE=$(curl --fail-with-body -s -S -X POST "$POLARIS_URL/api/management/v1/catalogs" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -H "Polaris-Realm: $REALM" \
    -d "$PAYLOAD" 2>&1) && echo "" || {
    echo "Failed to create catalog"
    echo "$RESPONSE" >&2
    exit 1
  }
  echo "Catalog created"

  echo "Creating catalog role 'admin'..."
  curl --fail-with-body -s -S -X POST "$POLARIS_URL/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "admin"}}' 2>&1 || {
    echo "Failed to create catalog role"
    exit 1
  }
  echo "Catalog role 'admin' created"

  echo "Granting CATALOG_MANAGE_CONTENT to 'admin' role..."
  curl --fail-with-body -s -S -X PUT "$POLARIS_URL/api/management/v1/catalogs/${CATALOG_NAME}/catalog-roles/admin/grants" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -d '{"grant": {"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}}' 2>&1 || {
    echo "Failed to grant CATALOG_MANAGE_CONTENT"
    exit 1
  }
  echo "CATALOG_MANAGE_CONTENT granted"

  echo "Assigning 'admin' catalog role to principal role 'service_admin'..."
  curl --fail-with-body -s -S -X PUT "$POLARIS_URL/api/management/v1/principal-roles/service_admin/catalog-roles/${CATALOG_NAME}" \
    -H "Authorization: Bearer $TOKEN" \
    -H "Accept: application/json" \
    -H "Content-Type: application/json" \
    -d '{"catalogRole": {"name": "admin"}}' 2>&1 || {
    echo "Failed to assign catalog role to principal role service_admin"
    exit 1
  }
  echo "Catalog role assigned to principal role 'service_admin'"
else
  echo "Catalog '$CATALOG_NAME' already exists, skipping setup"
fi
