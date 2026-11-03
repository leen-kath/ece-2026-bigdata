#!/bin/bash
set -euo pipefail

POLARIS_URI="http://polaris:8181"
API="$POLARIS_URI/api/management/v1"
CATALOG="$KUBERNETES_NAMESPACE"
CRED_FILE="/home/onyxia/.polaris-credentials.env"

echo "== Token root"
TOKEN=$(curl -fsS "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials -d client_id=root -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL | jq -r .access_token)
AUTH=(-H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json")

# Prints the HTTP status: 201 = created, 409 = already exists
call() {
  curl -sS -o /dev/null -w "%{http_code}\n" "${AUTH[@]}" "$@"
}

echo "== 1. Catalog role silver_reader"
call -X POST "$API/catalogs/$CATALOG/catalog-roles" \
  -d '{"catalogRole":{"name":"silver_reader"}}'

echo "== 2. Grants on namespace silver only"
for p in NAMESPACE_READ_PROPERTIES TABLE_LIST TABLE_READ_PROPERTIES TABLE_READ_DATA; do
  printf "%-26s -> " "$p"
  call -X PUT "$API/catalogs/$CATALOG/catalog-roles/silver_reader/grants" \
    -d "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"silver\"],\"privilege\":\"$p\"}}"
done

echo "== 3. Principal role silver_analyst"
call -X POST "$API/principal-roles" \
  -d '{"principalRole":{"name":"silver_analyst"}}'

echo "== 4. silver_reader -> silver_analyst"
call -X PUT "$API/principal-roles/silver_analyst/catalog-roles/$CATALOG" \
  -d '{"catalogRole":{"name":"silver_reader"}}'

echo "== 5. Principal silver_user"
status=$(curl -sS -o /dev/null -w "%{http_code}" "${AUTH[@]}" "$API/principals/silver_user")
if [ "$status" = "404" ]; then
  response=$(curl -fsS -X POST "${AUTH[@]}" "$API/principals" \
    -d '{"principal":{"name":"silver_user","properties":{}},"credentialRotationRequired":false}')
  cred=$(echo "$response" | jq -r '"\(.credentials.clientId):\(.credentials.clientSecret)"')
  echo "POLARIS_SILVER_USER_CREDENTIAL=\"$cred\"" >> "$CRED_FILE"
  echo "created, credential saved in $CRED_FILE (${cred:0:6}...)"
else
  echo "already exists (HTTP $status), credential not changed"
fi

echo "== 6. silver_analyst -> silver_user"
call -X PUT "$API/principals/silver_user/principal-roles" \
  -d '{"principalRole":{"name":"silver_analyst"}}'

echo "== 7. Check: grants of silver_reader"
curl -fsS "${AUTH[@]}" "$API/catalogs/$CATALOG/catalog-roles/silver_reader/grants" \
  | jq -r '.grants[] | "\(.type) \(.namespace // "") \(.privilege)"'