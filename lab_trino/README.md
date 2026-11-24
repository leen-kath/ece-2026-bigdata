# Lab Trino – Part 1: Deploy Trino with Helm

In this lab, I deployed Trino in my Kubernetes namespace on Onyxia (`user-k-sison-ece`) with Helm. Trino uses Polaris to find the Iceberg tables, and then it reads the data files directly on S3. We need this deployment for the next Trino labs.

## 1. Checks before the deployment

Before installing Trino, I checked that my environment was still working, because a lot of things can expire or disappear on Onyxia.

- **VS Code pod**: my pod was evicted (I explain it in the "Problems" part). So I deleted the service and I created a new one with `cpu: 2000m` and `memory: 2000Mi`, like the lab asks. My code was on GitHub, so I just cloned the repo again.
- **S3**: the command `aws s3 --profile default ls s3://user-k-sison-ece/bronze/` shows `orders.csv` and `users.csv`. So my S3 keys work.
- **Polaris**: the health check returns `"UP"`. I also asked Polaris for the table `silver.orders` with the REST API, and it returned the snapshot `1220184473976417285`. This is the current snapshot of my table, so Polaris can still read the metadata file on S3.
- **Quota**: my namespace has a limit of 1Gi for `requests.memory`. Before Trino, 784Mi was already used. Trino asks for 224Mi, so the total is 1008Mi. It is less than 1024Mi, so it fits, but only 16Mi are left. The other resources (CPU, limits) were also OK.

## 2. Configuration

I put the Helm files in `trino/deployment/`, like we did for Polaris.

- `Chart.yaml` says that we use the official Trino chart, version 1.45.0.
- `values.yaml` contains our settings. We use Trino 480 with only one pod (the coordinator also does the work of a worker), because we don't have a lot of memory in the quota. It also creates a catalog called `lakehouse`. This catalog asks Polaris where the tables are, and it logs in as the `dev` user.

Trino reads its settings from 3 Kubernetes objects:

- the secret `trino-polaris-oauth`: the login of the `dev` user for Polaris;
- the secret `polaris-s3`: the S3 keys, to read the files;
- the configmap `polaris-config` (created in the Polaris lab): the catalog name, the Polaris URL and the S3 URL.

## 3. Deployment and checks

I installed Trino with `helm upgrade --install trino ./trino/deployment --wait`. It took around 2 minutes.

After that, `kubectl get pods` shows the pod `trino-coordinator-...` in `Running`, and `curl -i http://trino:8080/v1/info` returns `200 OK` with `"state":"ACTIVE"` and `"version":"480"`. So Trino is running.

But this test only shows that Trino is started. It doesn't show that Trino can talk to Polaris and S3. So I also ran 2 queries with the Trino CLI inside the pod:

```bash
kubectl exec deploy/trino-coordinator -- trino --execute "SHOW SCHEMAS FROM lakehouse"
# "information_schema", "silver", "system"
kubectl exec deploy/trino-coordinator -- trino --execute "SELECT count(*) FROM lakehouse.silver.orders"
# "2833"
```

The first query shows the `silver` namespace, so Trino can connect to Polaris. The second query returns 2833 rows. It is the same number as in Spark for my `orders` table (version 4), so Trino can read the files on S3 and it sees the same table as Spark.