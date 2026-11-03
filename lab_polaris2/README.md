# Lab: Govern Iceberg tables with Polaris

In this lab, Spark uses Apache Polaris as an Iceberg REST catalog. We register an existing Iceberg table in Polaris, we create new tables through Polaris, and we compare what the principals `dev` and `analyst` are allowed to do. At the end, we see what Polaris can control (the commits of the metadata) and what it cannot control (the files in S3).

All the code is in the notebook `lab_polaris2/lab_polaris2.ipynb`. The script of exercise 2 is `lab_polaris2/exercise2_rbac.sh`.

## Environment

This section describes the tools and the starting state of the lab.

- Onyxia service `vscode-pyspark` with limits `cpu: 2000m`, `memory: 2500Mi` (patched in the Polaris deployment lab)
- Spark 4.1.1, Scala 2.13, Python 3.13, Iceberg `iceberg-spark-runtime-4.1_2.13:1.11.0`
- Polaris 1.8.0 deployed in our namespace (`http://polaris:8181`), with PostgreSQL
- Polaris catalog `user-k-sison-ece`, base location `s3://user-k-sison-ece/warehouse/`
- S3 storage (SeaweedFS), bucket `user-k-sison-ece`

Before the lab, the table `lab.silver.orders` (Hadoop catalog) was at version 4 (`version-hint.text` = 4), with 2,833 rows and a column `source`, because we changed it in the Iceberg lab. There were 14 objects under `warehouse/silver/orders/`.

## Changes compared to the lab text

This section lists what we did differently, and why.

- **Variables in the notebook**: the `export` and `set -a; . .polaris-credentials.env` commands only work in the terminal. The Jupyter kernel does not see them, so the first cell of the notebook sets `LAB_BUCKET_NAME`, `POLARIS_URI`, `POLARIS_CATALOG`, `AWS_REGION` and reads `/home/onyxia/.polaris-credentials.env` with Python.
- **Notebook instead of the `pyspark` shell**: we created the SparkSession in a notebook cell, with the same configuration as the lab.
- **Version 4 instead of version 1**: we registered `v4.metadata.json`, so the count is 2,833 orders, not 5,000.
- **`polaris.silver.users` created from Parquet**: `lab.silver.users` already had a `country` column (from the Iceberg lab), so the `ADD COLUMN country` of the lab would fail. We created `polaris.silver.users` from the clean file `silver/spark/users.parquet` (50 users, 10 columns).

## RBAC chain

This section shows how the privileges are given to the principals.

| Principal | Principal role | Catalog role | Privileges (on the catalog) |
|---|---|---|---|
| `dev` | `data_engineer` | `catalog_contributor` | `CATALOG_MANAGE_ACCESS`, `CATALOG_MANAGE_METADATA`, `CATALOG_MANAGE_CONTENT` |
| `analyst` | `data_analyst` | `catalog_reader` | `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES`, `CATALOG_READ_PROPERTIES` |

This chain was created by the bootstrap Job of the deployment lab. The Spark session chooses the principal with the variable `POLARIS_PRINCIPAL`.

## SparkSession with the Polaris catalog

This section connects Spark to two catalogs at the same time: `lab` (Hadoop) and `polaris` (REST).

The new options for Polaris are: `type = rest`, `uri = http://polaris:8181/api/catalog`, `warehouse = user-k-sison-ece` (the name of the Polaris catalog, not an S3 path), `credential = clientId:clientSecret`, `scope = PRINCIPAL_ROLE:ALL`, `io-impl = HadoopFileIO` (Spark reads the files with our S3A keys) and `fs.s3.impl = S3AFileSystem` (Polaris writes locations with `s3://`).

Results: `spark.hadoop.fs.s3.impl` = `org.apache.hadoop.fs.s3a.S3AFileSystem`, `SHOW NAMESPACES IN lab` = `silver`, `SHOW NAMESPACES IN polaris` = empty.

**1. Which component answered each of the two `SHOW NAMESPACES` commands? How can you tell?**

The first command (`SHOW NAMESPACES IN lab`) was answered by Spark itself, with the Iceberg Hadoop catalog. There is no server: Spark lists the folders under `s3a://user-k-sison-ece/warehouse/` in S3, and it finds the folder `silver`. The second command (`SHOW NAMESPACES IN polaris`) was answered by the Polaris server, through the Iceberg REST API (`http://polaris:8181/api/catalog`). We can tell it because the result is different: `lab` shows `silver`, but `polaris` shows an empty list, even if the two catalogs use the same `warehouse/` folder in the same bucket. Polaris does not look at the folders in S3: it answers with the namespaces stored in its own database (PostgreSQL), and at this moment this list is empty.

**2. Where is the credential used: in the Spark session, in the bucket, or in a request to Polaris?**

The credential (`clientId:clientSecret` of `dev`) is written in the Spark session configuration (`spark.sql.catalog.polaris.credential`), but it is really used in a request to Polaris. Spark sends it to the endpoint `/api/catalog/v1/oauth/tokens` to get an access token (OAuth2 client credentials). Then Spark sends this token in every REST request to Polaris, and Polaris uses it to know who we are and which privileges we have. The credential is not used in the bucket: Spark reads and writes the files in S3 with our own S3A keys (`HadoopFileIO`), because Polaris does not give S3 credentials in this setup (`stsUnavailable: true`).

## Register the Silver table in Polaris

In this section, we add the existing table `orders` to Polaris without copying its data.

We created the namespace with `CREATE NAMESPACE IF NOT EXISTS polaris.silver`, then we called `polaris.system.register_table` with `metadata_file => 's3a://user-k-sison-ece/warehouse/silver/orders/metadata/v4.metadata.json'`.

| Result | Value |
|---|---|
| `current_snapshot_id` | 1220184473976417285 |
| `total_records_count` | 2,833 |
| `total_data_files_count` | 3 |
| `SELECT count(*) FROM polaris.silver.orders` | 2,833 |
| objects under `warehouse/silver/orders/` before / after | 14 / 14 |

**1. What did `register_table` write, in Polaris and in the bucket? Compare the number of objects under `warehouse/silver/orders/` before and after.**

In the bucket, `register_table` wrote nothing. There were 14 objects under `warehouse/silver/orders/` before the registration (3 Parquet files, 3 manifests, 3 manifest lists, 4 `metadata.json` files and `version-hint.text`), and there are still 14 objects after. In Polaris, it wrote only catalog information in its PostgreSQL database: the namespace `silver` (created just before with `CREATE NAMESPACE`) and a new table entry `silver.orders`, with a pointer to the current metadata file `s3a://user-k-sison-ece/warehouse/silver/orders/metadata/v4.metadata.json`. Polaris read this file to check it, and it found the current snapshot `1220184473976417285`, 2,833 rows and 3 data files, exactly like the table in the `lab` catalog. So the registration is only a new reference to existing metadata, without any copy of data.

**2. Why is it dangerous to keep writing to this table through the `lab` catalog once it is registered in `polaris`?**

Now two catalogs point to the same table, but each one has its own pointer: `lab` uses the file `version-hint.text` in the bucket, and Polaris uses its own database. They don't know each other. If we write through `lab`, Iceberg creates `v5.metadata.json` and `version-hint.text` goes to 5, but Polaris still points to `v4`. So the users of Polaris don't see the new data. If someone then writes through Polaris, Polaris creates a new metadata file from `v4`, and the history of the table splits into two different branches: each catalog loses the changes of the other one. There is also no common lock, so the two catalogs can't detect conflicts. And if one catalog cleans old snapshots, it could delete files that the other catalog still uses. This is why we must write only through one catalog, Polaris.

## Create tables as dev

In this section, we check the registered table and we create a new table through Polaris.

`SHOW TABLES IN polaris.silver` showed `orders`, and its 3 snapshots are the same as in `lab` (`4219974134046780544` overwrite, `7719747662070971571` append, `1220184473976417285` append). We created `polaris.silver.users` with `writeTo("polaris.silver.users").createOrReplace()` from `silver/spark/users.parquet`: 50 users.

New files in `warehouse/silver/users/` for this table: `data/00000-5-b908e118-...parquet`, `metadata/00000-7b0e5e51-...metadata.json`, the manifest `c5db6e6e-...-m0.avro` and the manifest list `snap-3270219024235150290-...avro`. Nothing changed for `orders`.

**1. Where did Polaris store the files for `silver.users`, and why?**

Polaris stored the files in `s3://user-k-sison-ece/warehouse/silver/users/`: the Parquet file in `data/` (`00000-5-b908e118-...parquet`) and the metadata in `metadata/` (`00000-7b0e5e51-...metadata.json`, a new manifest and a new manifest list). This is because we did not give a `LOCATION` when we created the table, so Polaris used the default location: the `default-base-location` of the catalog (`s3://user-k-sison-ece/warehouse/`), plus the namespace (`silver`), plus the table name (`users`). This location is also inside the `allowedLocations` of the catalog. The metadata file has a UUID in its name, not `v5`, and `version-hint.text` was not changed, because the pointer to the current metadata is kept in the Polaris database, not in the bucket. We can also see that this is the same folder as the old table `lab.silver.users` of the Iceberg lab: the folder now contains the files of two different tables. It works because each catalog points to its own metadata file, but it can be confusing.

**2. Both tables are in your bucket. What is the difference between "registered" and "created through Polaris" for the data files?**

For the registered table (`orders`), the data files already existed: they were written before by Spark through the Hadoop catalog, in a place chosen by this catalog. Polaris did not write or move any file (still 14 objects), it only saved a pointer to `v4.metadata.json`. These data files are also still used by the `lab` catalog, so two catalogs share them. For the table created through Polaris (`users`), the data files were written during the creation, in the location chosen by Polaris, and the first metadata file was committed through Polaris. Only Polaris knows this table. In both cases, the files are in our bucket and Spark writes them with our own S3 keys: Polaris never writes data files itself, it only manages the metadata pointer and the access rights.

## Schema change as dev

In this section, we add a column to the table created through Polaris.

After `ALTER TABLE polaris.silver.users ADD COLUMN country STRING`, `DESCRIBE` shows 11 columns with `country` (string) at the end. New file: `metadata/00001-093219a9-...metadata.json` (20:56:53, 3,077 bytes), next to `00000-7b0e5e51-...metadata.json` (20:54:38, 2,237 bytes). No new data file, and the `v1` to `v4.metadata.json` files of `lab.silver.users` did not change.

**1. Which metadata file records the change? Is it the same file that `register_table` pointed to?**

The change is recorded in a new metadata file: `warehouse/silver/users/metadata/00001-093219a9-...metadata.json` (written at 20:56:53, 3,077 bytes). Iceberg never changes an existing metadata file: it wrote a new file with the new schema (and the old one in its history), then Polaris moved its pointer for `silver.users` from `00000-7b0e5e51-...metadata.json` to this new file. It is not the same file that `register_table` pointed to. `register_table` pointed to `warehouse/silver/orders/metadata/v4.metadata.json`, which is the metadata of another table (`orders`), and this file was not changed. Also, no new Parquet file was written: adding a column is only a metadata change.

**2. Which privilege does Polaris check for this statement?**

`ALTER TABLE ... ADD COLUMN` is a commit that changes the table metadata (the schema), not the data. In the Iceberg REST commit, this change is an `add-schema` update, and Polaris checks it as the operation `ADD_TABLE_SCHEMA` (we saw this name later in the error message of `analyst`). For this operation, Polaris needs a write privilege on the table metadata (`TABLE_WRITE_PROPERTIES`) on the table `silver.users`. The principal `dev` has this privilege because it has the principal role `data_engineer`, which has the catalog role `catalog_contributor`, and this catalog role has `CATALOG_MANAGE_CONTENT`. `CATALOG_MANAGE_CONTENT` includes all the privileges to create, read and change namespaces and tables in the catalog. So Polaris accepted the commit.

## Read as the analyst

In this section, we restart the Spark session with `POLARIS_PRINCIPAL = "analyst"` and we test reads and writes.

`SHOW TABLES IN polaris.silver` showed `orders` and `users`, and `SELECT product, count(*) FROM polaris.silver.orders GROUP BY product` worked (donut 462, cookie 469, croissant 491, bread 459, brioche 468, drink 484).

The two writes failed:
- `INSERT`: `org.apache.iceberg.exceptions.ForbiddenException: Forbidden: Principal 'analyst' with activated PrincipalRoles '[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT`
- `ALTER TABLE ... ADD COLUMN phone`: same message, `... is not authorized for op ADD_TABLE_SCHEMA`

The Spark log also showed `IcebergBatchWrite(table=polaris.silver.orders, format=PARQUET) is aborting` and then `aborted`. After the test, `orders/data/` still had 3 Parquet files.

**1. Which privilege is missing for the `INSERT`? Which privileges does `catalog_reader` provide?**

The `INSERT` failed with `ForbiddenException: Forbidden: Principal 'analyst' ... is not authorized for op ADD_TABLE_SNAPSHOT`. An `INSERT` adds a new snapshot to the table, and for this operation Polaris needs the privilege `TABLE_WRITE_DATA`. The principal `analyst` does not have it. `analyst` has the principal role `data_analyst`, and this role has only the catalog role `catalog_reader`, which gives three read privileges: `TABLE_READ_DATA` (read the data of tables), `TABLE_READ_PROPERTIES` (read the metadata and properties of tables) and `CATALOG_READ_PROPERTIES` (read the properties of the catalog). This is why `SHOW TABLES` and the `SELECT ... GROUP BY product` worked, but the `INSERT` did not. In the same way, the `ALTER TABLE ... ADD COLUMN phone` failed with `not authorized for op ADD_TABLE_SCHEMA`, because changing the schema also needs a write privilege on the table.

We also saw an important detail in the Spark logs: `IcebergBatchWrite(table=polaris.silver.orders) is aborting` and then `aborted`. Spark first wrote a new Parquet file in S3 with our own S3 keys, because Polaris does not control the bucket. Then Polaris refused the commit, and Spark aborted the write and deleted this file. After the test, the folder `orders/data/` still had only 3 Parquet files. So Polaris controls the metadata (the commit), but not the files in S3: if Spark had crashed before the abort, an orphan file could stay in the bucket.

**2. Check the snapshots of `silver.orders` as `dev`, after restarting the kernel with `POLARIS_PRINCIPAL = "dev"`. Did the failed `INSERT` create one?**

No. After restarting the kernel with `POLARIS_PRINCIPAL = "dev"`, the table `polaris.silver.orders` still has exactly 3 snapshots: `4219974134046780544` (`overwrite`, 2026-10-28 15:48:20), `7719747662070971571` (`append`, 2026-11-03 19:54:57) and `1220184473976417285` (`append`, 2026-11-03 19:59:47). The current snapshot is still `1220184473976417285`, the same as just after the registration, and there is no snapshot at 20:59, the time of the failed `INSERT`. This shows that the commit is the step that Polaris controls: a snapshot only exists when Polaris accepts the commit and moves its pointer to a new metadata file. Because Polaris answered `Forbidden`, no new metadata file was committed, and Spark also deleted the data file that it had written (we still had 3 Parquet files in `orders/data/`). For all readers, the table did not change.

## Commit the Iceberg table metadata

In this section, we download the warehouse from S3 and we commit only the metadata in Git.

We ran `aws s3 sync "s3://$LAB_BUCKET_NAME/warehouse/" ./warehouse/`, then we committed the `metadata/` folders of the two tables (commit `624cc78`, 20 files: 18 added, 2 modified). The `data/` folders were not added.

**Compare the difference in the Git repository, which files are added?**

The repository already had version 1 of the two tables (from the Spark lab). After `aws s3 sync`, `git status` showed as new all the metadata files created after this first version:
- `orders`: `v2.metadata.json`, `v3.metadata.json`, `v4.metadata.json`, 2 manifests and 2 manifest lists (`snap-7719747662070971571-...`, `snap-1220184473976417285-...`), created during the Iceberg lab (append, `ALTER TABLE`, `INSERT`).
- `users`: `v2` to `v4.metadata.json`, 2 manifests and 2 manifest lists (`snap-7345446893973528675-...`, `snap-4877665324485103323-...`), created during the exercises of the Iceberg lab.
- `users`, created through Polaris: `00000-7b0e5e51-...metadata.json` (creation of the table), `00001-093219a9-...metadata.json` (`ADD COLUMN country`), the manifest `c5db6e6e-...-m0.avro` and the manifest list `snap-3270219024235150290-...`.
- the two folders `data/` with the Parquet files, which we did not add, because the data stays in S3 and Git only keeps the metadata.

There was no new file for the registration of `orders` in Polaris, because `register_table` does not write in the bucket.

We found one problem: `version-hint.text` was not updated by `aws s3 sync` (it was still `1` in the repository, but `4` in S3). The reason is that `sync` only downloads a file when its size is different or when the S3 object is newer than the local file. Here both versions are 1 byte, and the local file was created by `git clone` after the last change in S3. We copied the two `version-hint.text` files with `aws s3 cp` to fix it, so they appear as modified (`M`) in the commit.

## Exercises

In this section, we change the schema again and we build a more restrictive role.

### 1. Remove the `country` column

This exercise shows that dropping a column is only a metadata operation.

We ran `ALTER TABLE polaris.silver.users DROP COLUMN country` as `dev`. After this, `DESCRIBE` shows 10 columns again, without `country`.

The snapshots did not change: before and after the `DROP COLUMN`, the table has only one snapshot, `3270219024235150290` (`overwrite`, 2026-11-03 20:54:36), created when we created the table. The metadata table `metadata_log_entries` shows that three metadata files were written:

| timestamp | metadata file | latest_snapshot_id | latest_schema_id |
|---|---|---|---|
| 20:54:37 | `00000-7b0e5e51-...metadata.json` (creation) | 3270219024235150290 | 0 |
| 20:56:53 | `00001-093219a9-...metadata.json` (`ADD COLUMN country`) | 3270219024235150290 | 0 |
| 21:10:22 | `00002-32acfd41-...metadata.json` (`DROP COLUMN country`) | 3270219024235150290 | 0 |

In S3, the only new file is `00002-32acfd41-...metadata.json` (3,237 bytes). The Parquet file of the table (`00000-5-b908e118-...parquet`, 7,435 bytes, written at 20:54:36) was not rewritten.

This is a metadata operation because Iceberg only changes the schema in a new metadata file. The data files are not touched: Iceberg knows the columns by their ID, so the old Parquet file still contains the `country` column (empty), but the new schema says that this column is not part of the table anymore, so readers simply ignore it. There is no new snapshot because the data did not change: a snapshot is created only when data files are added or removed. This is also why `latest_schema_id` stays `0`: this column shows the schema of the latest snapshot, and the snapshot was written with the first schema. The table changed from `00001` to `00002` in Polaris, but the data stayed exactly the same.

### 2. A `silver_reader` role limited to the `silver` namespace

This exercise creates a role with read privileges on one namespace only, and tests it.

As `dev`, we created the namespace `polaris.gold` and a table `polaris.gold.orders_by_product` (orders and quantity by product, from `polaris.silver.orders`): 6 rows, 2,833 orders in total.

As `root`, we used the Polaris management API (`/api/management/v1`) with the script `lab_polaris2/exercise2_rbac.sh` to build a new RBAC chain. Every call returned HTTP `201`:

| Step | Object |
|---|---|
| 1 | catalog role `silver_reader` |
| 2 | grants on `namespace ["silver"]` only: `NAMESPACE_READ_PROPERTIES`, `TABLE_LIST`, `TABLE_READ_PROPERTIES`, `TABLE_READ_DATA` |
| 3 | principal role `silver_analyst` |
| 4 | `silver_reader` granted to `silver_analyst` |
| 5 | principal `silver_user` (its credential was saved in `/home/onyxia/.polaris-credentials.env`, not in Git) |
| 6 | `silver_analyst` assigned to `silver_user` |

So the chain is: `silver_user` -> `silver_analyst` -> `silver_reader` -> read privileges on `silver` only. The difference with `catalog_reader` is the type of the grants: `catalog_reader` has grants of type `catalog`, which apply to all namespaces and tables of the catalog, but `silver_reader` has grants of type `namespace`, which apply only to the namespace `silver` and its tables.

Then we restarted the Spark session with `POLARIS_PRINCIPAL = "silver_user"` and tested:

| Test | Result |
|---|---|
| `SHOW TABLES IN polaris.silver` | OK (`users`, `orders`) |
| `SELECT count(*) FROM polaris.silver.orders` | OK (2,833) |
| `SELECT count(*) FROM polaris.silver.users` | OK (50) |
| `SHOW TABLES IN polaris.gold` | Forbidden, op `LIST_TABLES` |
| `SELECT * FROM polaris.gold.orders_by_product` | Forbidden, op `LOAD_TABLE` |
| `SHOW NAMESPACES IN polaris` | Forbidden, op `LIST_NAMESPACES` |
| `INSERT INTO polaris.silver.orders ...` | Forbidden, op `ADD_TABLE_SNAPSHOT` |

The result is what we wanted: `silver_user` can list and read the tables in `silver`, but it cannot list or read anything in `gold`, and it cannot write in `silver`. It also cannot list the namespaces of the catalog, because `NAMESPACE_LIST` was not granted at the catalog level. This is the principle of least privilege: the principal only has the privileges that it needs. If we want this user to see the list of namespaces, we could add `NAMESPACE_LIST` at the catalog level, but then it would also see that `gold` exists (without being able to read it).

## Cleanup

This section removes the tables that we created for the lab, and keeps the table needed for the Trino lab.

As `dev`, we ran `DROP TABLE IF EXISTS polaris.gold.orders_by_product`, `DROP TABLE IF EXISTS polaris.silver.users` and `DROP NAMESPACE IF EXISTS polaris.gold`. We had to drop the table of `gold` first, because a namespace that still contains a table cannot be dropped. After the cleanup, Polaris has only the namespace `silver` with the table `orders` (2,833 rows). We did not unregister `silver.orders`, because the Trino lab reads it. A `DROP TABLE` without `PURGE` only removes the table from the catalog: the files stay in the bucket. We kept the role `silver_reader`, the principal role `silver_analyst` and the principal `silver_user` of exercise 2. Then we stopped Spark with `spark.stop()`.

## Problems met during the lab

This section lists the problems we had and how we solved them.

- **Variables not visible in the notebook**: the variables exported in the terminal are not visible in the Jupyter kernel (`KeyError`). We set them in the first cell with `os.environ` and we read the credentials file with Python.
- **Order of the cells**: we first ran the session cell before the cell with `import os`, so we got `NameError: name 'os' is not defined`. We put everything in one cell.
- **`lab.silver.users` already had a `country` column**: we created `polaris.silver.users` from `silver/spark/users.parquet` instead.
- **Long error messages in the notebook**: the Spark errors were truncated. We caught the exception in Python and printed only the lines with `Forbidden`, to see the operation refused by Polaris.
- **`aws s3 sync` did not update `version-hint.text`**: same size (1 byte) and older S3 timestamp, so we copied it with `aws s3 cp`.
- **The jar was downloaded again**: the pod restart of the deployment lab deleted the `~/.ivy2` cache, which is outside `~/work`.