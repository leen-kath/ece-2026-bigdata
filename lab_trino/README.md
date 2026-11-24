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


# Lab Trino – Part 2: Silver to gold

In this part, I used Trino from a Python notebook to read the silver tables, check them, and build a gold layer with SQL.

## Environment

I connected to Trino from a notebook with the Python client (`trino` and `pandas` added with `uv add`). `SELECT version()` returns `480`.

**What does `catalog=` change?**
It sets the default catalog for the session. So when I don't write the catalog in my query, Trino uses `lakehouse`. For example, `SHOW SCHEMAS` (without `FROM lakehouse`) gives `information_schema`, `silver` and `system`, the same as `SHOW SCHEMAS FROM lakehouse`. I can still write the full name `lakehouse.silver.orders`, and I must write the catalog name if I want to use another catalog like `system`.

## Catalogs, schemas and tables

Trino uses names like `catalog.schema.table`. I checked what my cluster shows.

`SHOW CATALOGS` gives `lakehouse`, `system`, `tpcds` and `tpch`. `tpcds` and `tpch` are test catalogs added by the Trino chart. They generate benchmark data, they don't read my bucket.

At first, `SHOW TABLES FROM lakehouse.silver` showed only `orders`, because I deleted `users` from Polaris at the end of the Polaris lab. I registered it again with the Polaris REST API on `v4.metadata.json` (the current version of the Spark table). After that, Trino shows `orders` and `users`, and `users` has 57 rows like in Spark.

**Which Trino type holds `ordered_at`? Does it keep a time zone?**
The type is `timestamp(6) with time zone`. The `(6)` means microseconds. In Iceberg it is a `timestamptz`, so the value is an exact moment saved in UTC. Trino shows the values with `+00:00` (UTC), for example `2020-01-28 12:31:23.938466+00:00`. So it is not a local time, but the original time zone of the order is not kept, only the moment.

**`order_id` was a string in Spark. What does Trino report?**
Trino reports `varchar`. Trino has a `uuid` type, but it reads the type written in the Iceberg metadata, and Spark wrote `string`. So Trino does not change it. `user_id` is also `varchar` for the same reason.

My tables also have the columns I added in the Iceberg lab: `source` in `orders` and `country` in `users`.

## Query the silver layer

In this part I counted the rows and I checked the quality of the silver tables before building the gold layer.

**Explain the output of the queries.**
- `orders` has 2833 rows and `users` has 57 rows. These are not the numbers of the lab (5000 and 50) because my data is different. 2833 = the 2829 orders from bronze + 4 rows that I appended in the Iceberg lab. For users, I also added rows in the Iceberg lab (schema evolution with `country`), so I have 57 and not 50.
- In the state query, `AP` (4 users) and `AE` (3 users) have no city at all (`missing_city` = number of users). These are US military addresses (like `APO AE 12345`), and they have no city, so the address parsing of the Spark lab gives NULL. It is normal. The other states have no missing city.
- The orphan query returns 0. So every order has a user, and an inner join will not lose any order.

**Which dbt test does the orphan query replace?**
It replaces the `relationships` test of dbt. This test checks that every `user_id` of `orders` exists in `users`, like a foreign key. To run it on every refresh, I would write it in the `schema.yml` and run `dbt test` (or `dbt build`) after each refresh:

```yaml
- name: user_id
  tests:
    - relationships:
        to: ref('users')
        field: user_id
```

**Trino gave the results without a job or a session. What does it tell us?**
Trino is a service that is always running. I just send SQL and I get the answer in a few seconds. So Trino is a good choice for interactive queries: exploration, dashboards, BI. Spark needs to start a session (driver, jars, memory) before running anything, so it is better for big batch transformations or Python/ML code.

## Iceberg metadata

Trino shows the metadata of an Iceberg table with hidden tables like `"orders$snapshots"`, `"orders$files"` and `"orders$history"`.

**How many snapshots does `silver.orders` have? Which engine wrote them?**
The table has 3 snapshots, and the column `summary['engine-name']` says `spark` for all of them:

| Date | Operation | Added records |
|---|---|---|
| 2026-10-28 | overwrite | 2829 |
| 2026-11-03 | append | 3 |
| 2026-11-03 | append | 1 |

The first one is the silver table written in the Spark lab. The two appends come from the Iceberg lab. 2829 + 3 + 1 = 2833, so it explains my count. Trino did not write any snapshot, it only reads. In `$history`, each snapshot has the previous one as parent, so the history is a simple line without rollback. (In pandas, `parent_id` is shown like `4.219974e+18` because the column has a NULL, so pandas uses float numbers.)

**Compare `aws s3 ls` with `$files`. Which one does not need access to the bucket, and why?**
Here both give the same 3 Parquet files (84055, 1956 and 2201 bytes). But they are not the same thing. `aws s3 ls` lists all the objects in the folder, also old files that are not in the table anymore, and I need my own S3 keys for it. `$files` only lists the files of the current snapshot. Trino reads this list in the Iceberg manifests, with its own keys. So the `$files` query does not need bucket access for me: I only send SQL to Trino. For example, an analyst could see the files of a table without any S3 rights.