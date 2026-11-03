# Lab: Update the S3 credentials in the Polaris pod (Kubernetes)

Polaris reads its S3 credentials from environment variables, and these variables are fixed when the pod starts. Our credentials are temporary STS credentials from Onyxia, so they expire. This lab shows how to give new credentials to Polaris without losing the catalog.

## Why a restart is needed

This section explains how Polaris gets its S3 credentials.

In the deployment lab, the file `values.yaml` loads the keys `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN` from the Kubernetes Secret `polaris-s3` into environment variables of the Polaris container. Polaris uses them to read the Iceberg metadata files in the bucket (for example `v4.metadata.json` when Spark loads `silver.orders`). Kubernetes copies the values of the Secret into the environment only when the container starts. So if we only update the Secret, the running pod keeps the old values: we must restart the pod.

The catalog, the principals, the roles and the grants are not stored in the Polaris pod, but in PostgreSQL (`polaris-postgresql-0`). So they survive a restart of Polaris, and the credentials of `dev`, `analyst` and `silver_user` stay valid.

## State before the update

This section checks that Polaris works before the change.

- Polaris pod: `polaris-6cf856d5b7-9sw4d`
- `kubectl exec deployment/polaris -- sh -c 'tr "\0" "\n" < /proc/1/environ | grep AWS_ACCESS_KEY_ID'` showed the same access key ID as our terminal.
- Test with the REST API as `dev`: `GET /api/catalog/v1/user-k-sison-ece/namespaces/silver/tables/orders` returned HTTP `200`. To answer this request, Polaris must read the metadata file of the table in S3, so this test shows that its S3 credentials work.

We only printed the access key ID (and later only its last 4 characters), never the secret key or the session token.

## Renew the S3 credentials locally

This section prepares the credentials that we give to Polaris.

The lab asks to get new credentials from Onyxia, to update the `default` AWS profile, and to export them with `eval "$(aws configure export-credentials --profile default --format env)"`. On our Onyxia platform, the "My account" page only has the tabs Profile, Git, Vault and Interface modes, so we did not find a page to generate new S3 credentials. We could not restart our VS Code service to get new credentials either, because it would start again with 4000Mi of memory, and there would be no space for Polaris in the quota of the namespace.

So we used the current credentials of the VS Code pod. They were still valid: `aws s3 --profile default ls "s3://user-k-sison-ece/bronze/"` showed `orders.csv` and `users.csv`. The process is exactly the same with new credentials. Only the result of the comparison is different (same key instead of a new key).

## Update the credentials in the Polaris deployment

This section updates the Secret and restarts Polaris.

We updated the Secret with `kubectl create secret generic polaris-s3 ... --dry-run=client -o yaml | kubectl apply -f -`. The result was `secret/polaris-s3 unchanged`, because the values were the same as before. With new credentials, the result would be `configured`.

Then we restarted the deployment:

```bash
kubectl rollout restart deployment/polaris
kubectl rollout status deployment/polaris --timeout=5m
```

The output showed `1 old replicas are pending termination...` two times, and then `deployment "polaris" successfully rolled out`. This is a rolling update: Kubernetes first creates a new pod, waits until it is ready (init container and start of Polaris), and only then stops the old pod. So the Polaris API stays available during the restart.

We did not need the "Resource constraint" part of the lab. During the rolling update, there are two Polaris pods for a short time, but the quota is enough:

| Resource | Used before | + new Polaris pod | Hard limit |
|---|---|---|---|
| `limits.cpu` | 2.75 (VS Code 2 + PostgreSQL 0.25 + Polaris 0.5) | 3.25 | 4 |
| `limits.memory` | 3268Mi (2500 + 256 + 512) | 3780Mi | 4096Mi |

If the quota was too small, the new pod would stay `Pending`. In this case, the lab says to scale the deployment to 0 replicas and then back to 1: the old pod is stopped first, so there is space for the new one, but Polaris is not available for a short time.

## Verify

This section checks that the new pod works and uses the credentials of the Secret.

| Check | Result |
|---|---|
| `kubectl get pods` | new pod `polaris-6fdcdf4856-sxmxg` (2 minutes old), `polaris-postgresql-0` not restarted (65 minutes old) |
| access key ID in the new pod vs in the terminal (last 4 characters) | the same |
| token for `dev` from `/api/catalog/v1/oauth/tokens` | OK (`eyJ...`) |
| `GET .../namespaces/silver/tables/orders` as `dev` | HTTP `200` |

The name of the pod changed (`6cf856d5b7` -> `6fdcdf4856`) because `rollout restart` adds an annotation with the date to the pod template, so Kubernetes creates a new ReplicaSet. PostgreSQL did not restart, and the `dev` credential still works, so the principals and the catalog were kept. The HTTP `200` shows that the new Polaris pod can read the metadata of `orders` in S3 with the credentials of the Secret.

## Problems met during the lab

This section lists the problems we had and how we solved them.

- **Placeholders pasted as values**: we pasted `export AWS_ACCESS_KEY_ID="…"` with the character `…` instead of real values. We did not update the Secret with these values (Polaris would lose access to S3). We closed the terminal and opened a new one, which gets the real `AWS_*` variables of the pod again, and we checked the AWS profile with `aws s3 ls`.
- **Long command cut in the terminal**: the one-line `kubectl create secret` command was cut in the middle, and the terminal waited for the end of a quote (`>`). We pressed `Ctrl+C` and used the same command on several short lines with `\`.
- **No page for new S3 credentials in Onyxia**: see "Renew the S3 credentials locally". We kept the current valid credentials.