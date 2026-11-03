# Lab: Explore Iceberg tables with Spark

This lab shows how an Apache Iceberg table is stored in S3 and how it works with Spark. We look at the files of a table, its metadata and snapshots, then we test updates, time travel and schema evolution. At the end, we compare an Iceberg table with a simple Parquet folder.

All the code is in the notebook `lab_iceberg/lab_iceberg.ipynb`.

## Environment

This section describes the tools and the data used in the lab.

- Onyxia service `vscode-pyspark` (CPU max 4000m, memory max 4000Mi)
- Spark 4.1.1, Scala 2.13, Python 3.13, hadoop-aws 3.4.2
- Iceberg package: `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0`
- S3 storage (SeaweedFS), bucket `user-k-sison-ece`
- Iceberg catalog `lab`, type `hadoop`, warehouse `s3a://user-k-sison-ece/warehouse`
- Input tables from the previous lab (Spark DataFrames, bronze to silver): `lab.silver.orders` (2,829 rows) and `lab.silver.users` (50 rows)

## SparkSession with S3 access

This section explains how we started Spark with S3 access and the Iceberg catalog.

We created the SparkSession directly in a Jupyter notebook (kernel Python 3.13), not in the `pyspark` shell. The session configures the S3A connector with the temporary STS credentials of Onyxia (`TemporaryAWSCredentialsProvider`, because the default provider ignores the session token) and `path.style.access=true`. It also loads the Iceberg jar with `spark.jars.packages` and declares the catalog `lab`. This must be done when the session starts, because the libraries are loaded at the start.

To check the catalog, we ran `SHOW NAMESPACES IN lab` and `SHOW TABLES IN lab.silver`. Spark found the namespace `silver` with the two tables `orders` and `users`. The first query on the table gave 2,829 orders with a total quantity of 8,615.

Schema of `lab.silver.orders`: `order_id` (string), `user_id` (string), `ordered_at` (timestamp), `quantity` (int), `product` (string).

## Create an Iceberg table

This section is about the creation of the Iceberg table.

The table `lab.silver.orders` was already created in the previous lab with `stg_orders.writeTo("lab.silver.orders").createOrReplace()`, so we did not create it again. We only checked that it is known by the catalog. Unlike a Parquet folder, we can read it by its name (`lab.silver.orders`) and not with an S3 path.

## Inspect the files

In this section, we list the files of the orders table in S3 to understand how an Iceberg table is stored.

At the start of the lab, the table had only 5 files:

| File | Size | Role |
|---|---|---|
| `data/00000-64-d5f6b0a8-...parquet` | 84,055 B | data (the 2,829 rows) |
| `metadata/3bea5980-...-m0.avro` | 7,347 B | manifest |
| `metadata/snap-4219974134046780544-...avro` | 4,453 B | manifest list of the snapshot |
| `metadata/v1.metadata.json` | 1,931 B | table metadata |
| `metadata/version-hint.text` | 1 B | current version number (Hadoop catalog) |

**1. Which files contain the actual rows?**

The actual rows are in the Parquet files in the `data/` folder. For the orders table, there is only one file: `data/00000-64-d5f6b0a8-...-00001.parquet` (84,055 bytes). This file contains the 2,829 orders. For the users table, it is also one Parquet file (7,435 bytes) with the 50 users.

**2. Which files describe the table?**

The files in the `metadata/` folder describe the table:
- `v1.metadata.json` contains the schema of the table (the column names and types), the list of snapshots and the current snapshot.
- `snap-4219974134046780544-....avro` is the manifest list. It gives the list of manifest files for this snapshot.
- `3bea5980-...-m0.avro` is the manifest file. It gives the list of the Parquet data files, with some statistics like the number of rows.
- `version-hint.text` is a very small file (1 byte). It tells the Hadoop catalog that the current version of the table is version 1.

**3. Why does an Iceberg table need more than the Parquet files?**

Parquet files only store data. They don't know which other files belong to the same table, and they don't keep any history. With only Parquet files, if a file is added or removed while someone is reading, the reader can see a mix of old and new data. Iceberg adds metadata files to solve this problem. The metadata says exactly which files are part of the table at a given moment (a snapshot). Because of this, Iceberg can offer a clear table schema, a history of versions, time travel and safe updates. The cost is small: in our case, the metadata is about 13.7 KB, while the data is about 84 KB.

## Explore Iceberg metadata

In this section, we read the Iceberg metadata tables (`snapshots` and `files`) with SQL.

The `snapshots` table gave one row: snapshot `4219974134046780544`, committed on 2026-10-28 15:48:20, `parent_id = NULL`, operation `overwrite`. Its summary contains `added-data-files = 1`, `added-records = 2829`, `added-files-size = 84055`, `total-data-files = 1`, `total-records = 2829`, `engine-name = spark`, `engine-version = 4.1.1`, `iceberg-version = 1.11.0` and `app-name = lab-silver-pyspark`.

Note: the query on `lab.silver.orders.files` was very slow in our environment (the kernel was blocked and we had to restart it), so we used the snapshot summary to get the number of data files.

**1. How many snapshots does the table currently have?**

The table has only one snapshot. Its ID is `4219974134046780544`, and it was committed on 2026-10-28 at 15:48:20. Its `parent_id` is `NULL`, because it is the first version of the table. The operation is `overwrite`, because the previous lab created the table with `createOrReplace()`.

**2. How many data files belong to the current table?**

Only one data file belongs to the table. The summary of the snapshot shows `total-data-files -> 1` and `total-records -> 2829`. This is the same Parquet file that we saw in the `data/` folder (84,055 bytes).

**3. What information does the `snapshots` table contain that the Parquet files do not?**

A Parquet file only knows its own rows and its own schema. The `snapshots` table gives information about the history of the whole table:
- when the change was made (`committed_at`),
- a unique ID for each version (`snapshot_id`) and the previous version (`parent_id`),
- the type of operation (`overwrite`, `append`, ...),
- a summary of the change: how many files and rows were added (`added-data-files = 1`, `added-records = 2829`) and the totals for the table,
- who wrote the data: the engine (Spark 4.1.1), the Iceberg version (1.11.0) and the application name (`lab-silver-pyspark`).

With only Parquet files, we can't know when the data changed, what changed, or what the table looked like before.

## Create a second snapshot

In this section, we append 3 new orders (with new `order_id` values made with `uuid()`) and we look at the new snapshot.

| snapshot_id | parent_id | operation | committed_at |
|---|---|---|---|
| 4219974134046780544 | NULL | overwrite | 2026-10-28 15:48:20 |
| 7719747662070971571 | 4219974134046780544 | append | 2026-11-03 19:54:57 |

After the append, the table has 2,832 rows.

**1. How many snapshots are there now?**

There are now two snapshots. The first one is `4219974134046780544` (operation `overwrite`, 2026-10-28). The second one is `7719747662070971571` (operation `append`, 2026-11-03). The `parent_id` of the second snapshot is the ID of the first one, so we can see that the new version comes from the old one. The table now has 2,832 rows (2,829 + 3).

**2. Did the original Parquet files disappear?**

No, they did not disappear. In S3, the old file `00000-64-d5f6b0a8-...parquet` (84,055 bytes, written on 2026-10-28) is still there and was not changed. Iceberg only added a new small file `00000-3-b3fc9c12-...parquet` (1,956 bytes) with the 3 new orders. It also added a new manifest, a new manifest list (`snap-7719747662070971571-...avro`) and a new metadata file `v2.metadata.json`. The old `v1.metadata.json` is also still there.

**3. Why does Iceberg create a new snapshot instead of simply replacing the table?**

Iceberg never modifies existing files. It writes the new files next to the old ones, and then it creates a new snapshot that lists all the valid files. There are several good reasons for this:
- It is safe: a reader who started before the append still reads the old snapshot, so it never sees a mix of old and new data.
- It is atomic: the switch to the new version happens at the end, when `v2.metadata.json` and `version-hint.text` are written. We can see this in the S3 timestamps: the data files and manifests were written at 19:54:56, and `v2.metadata.json` and `version-hint.text` at 19:54:57. If the write fails before this step, the table stays at version 1.
- It is cheap: we only wrote 1,956 bytes of new data, and we did not copy the 84,055 bytes of the old file.
- It keeps the history, so we can use time travel to read the old version.

## Time travel

In this section, we read the table as it was before the append, with `VERSION AS OF`.

| Query | Result |
|---|---|
| `SELECT count(*) FROM lab.silver.orders VERSION AS OF 4219974134046780544` | 2,829 |
| `SELECT count(*) FROM lab.silver.orders` (current) | 2,832 |

**1. What does time travel read?**

Time travel reads the table as it was at an older snapshot. With `VERSION AS OF 4219974134046780544`, Iceberg uses the manifest list of the first snapshot instead of the current one. This manifest list only knows the first Parquet file, so the query returns 2,829 rows. The current table (second snapshot) returns 2,832 rows, because it also includes the new file with the 3 new orders.

**2. Why can Iceberg access an older version without making a complete copy of the table?**

Because Iceberg never changes or deletes files when it writes a new version. The old snapshot is only a list of files, and these files are still in S3. The two snapshots even share the same big Parquet file (84,055 bytes): the first snapshot uses only this file, and the second snapshot uses this file plus the new small file (1,956 bytes). So reading an old version only means reading another list of files. There is no copy of the data, and both queries took about 0.3 seconds.

**3. What would happen if the old snapshot's metadata and data files were physically deleted?**

If the metadata files of the old snapshot (its manifest list and manifests) were deleted, time travel to this snapshot would not work anymore: Iceberg could not find the list of files, so the query would fail with an error. If data files were deleted, it is even more dangerous: in our case, the big Parquet file is used by both snapshots, so deleting it would also break the current table, not only the old version. This is why we should never delete files by hand. Iceberg has a special operation (`expire_snapshots`) that removes old snapshots and deletes only the files that no current snapshot uses anymore. After that, time travel to these old snapshots is not possible.

## Schema evolution

In this section, we add a column `source` to the orders table and we write one row with the new schema.

After `ALTER TABLE lab.silver.orders ADD COLUMN source STRING`, `DESCRIBE` shows 6 columns, with `source` (string) at the end. Then we inserted one row with `source = 'spark'`.

| source | orders |
|---|---|
| NULL | 2,832 |
| spark | 1 |

The table now has 3 snapshots: `overwrite` (2026-10-28), `append` (19:54:57) and `append` (19:59:47, ID `1220184473976417285`). In S3, we see `v3.metadata.json` (19:58:51, created by the `ALTER TABLE`) and `v4.metadata.json` (19:59:47, created by the `INSERT`), plus one new Parquet file of 2,201 bytes.

**1. Did the existing Parquet files have to be rewritten?**

No. After `ALTER TABLE ... ADD COLUMN source STRING`, the S3 listing shows that the two old Parquet files are exactly the same: same size (84,055 and 1,956 bytes) and same date. The only new file created by the `ALTER TABLE` is `v3.metadata.json` (19:58:51). When we read the table, the old rows simply show `NULL` in the `source` column: 2,832 rows have `source = NULL`. Only the new row written by the `INSERT` has `source = spark`, and it is stored in a new Parquet file (`00000-15-936e4070-...parquet`, 2,201 bytes, 19:59:47).

**2. Where is the new schema recorded?**

The new schema is recorded in the Iceberg metadata file `v3.metadata.json`, which was created by the `ALTER TABLE`. The next file, `v4.metadata.json` (created by the `INSERT`), also contains it. The schema is not stored in the old Parquet files. We can also see that the `ALTER TABLE` did not create a new snapshot: the table has 3 snapshots (`overwrite`, `append`, `append`), and the third one comes from the `INSERT`, not from the `ALTER TABLE`.

**3. Why is schema evolution a table-level operation rather than simply changing a Parquet file?**

A table is made of many Parquet files, written at different times. Each Parquet file has its own fixed schema, so we can't add a column to an existing file without rewriting it. If we only changed one file, the files of the same table would have different schemas, and readers would not know which schema is the right one. With Iceberg, the schema belongs to the table, in the metadata. Iceberg gives a unique ID to each column, so it knows that old files do not have the column `source` and it returns `NULL` for them. This way, one small metadata change is enough, and it works for all files, old and new, without copying any data.

## Compare Iceberg with Parquet

In this section, we compare the Parquet folder written by the Spark lab with the Iceberg table.

| `silver/spark/orders.parquet/` | `warehouse/silver/orders/` |
|---|---|
| 1 data file `part-00000-...snappy.parquet` (139,556 B) | 3 data files in `data/` |
| 1 `_SUCCESS` file (11,799 B) | 3 manifests, 3 manifest lists |
| no metadata, no snapshots | `v1` to `v4.metadata.json` + `version-hint.text` |

**1. Which one is simply a collection of Parquet files?**

`silver/spark/orders.parquet/` is simply a collection of Parquet files. It only contains one data file (`part-00000-...snappy.parquet`, 139,556 bytes) and a `_SUCCESS` file. The `_SUCCESS` file (11,799 bytes) is only a small report written by the Spark job when it finishes. It is not table metadata: no reader uses it to know which files to read, and it has no versions. The table is just "all the Parquet files that are in the folder".

Note: the same 2,829 orders take 139,556 bytes in this folder and 84,055 bytes in the Iceberg table. This is because Spark uses Snappy compression by default (we can see `.snappy` in the file name), and Iceberg uses Zstandard (zstd) by default, which compresses more.

**2. Which one has table metadata and snapshots?**

`warehouse/silver/orders/` has table metadata and snapshots. In the `metadata/` folder, there are 4 metadata files (`v1.metadata.json` to `v4.metadata.json`), 3 manifest lists (`snap-...avro`, one for each snapshot), 3 manifests (`...-m0.avro`) and `version-hint.text`. With these files, the table has a schema, a history of 3 snapshots, time travel and schema evolution.

**3. How would you safely replace a Parquet directory while another query is reading it?**

With a plain Parquet directory, it is difficult. If we delete the old files and write the new ones in the same folder, a reader can see an empty folder, or a mix of old and new files. On S3, there is no atomic "rename" of a folder and no lock. A safer solution is to write the new data in a new folder (for example `orders_v2/`), wait until the write is complete, then tell the readers to use the new folder, and delete the old folder later, when nobody reads it. But we have to manage this switch ourselves, outside the data. This is exactly what Iceberg does automatically.

**4. How does Iceberg make the table state visible atomically?**

Iceberg first writes all the new files: the Parquet data files, then the manifests, the manifest list and a new `metadata.json`. During this time, readers still use the old version. At the end, Iceberg changes only one pointer to the new `metadata.json`. In our Hadoop catalog, this pointer is the file `version-hint.text`, which went from 1 to 4 during the lab. We could see it in the S3 timestamps: for the first append, the data and manifests were written at 19:54:56, and `v2.metadata.json` and `version-hint.text` at 19:54:57. So a reader sees either the old version or the new version, never something in between. If the write fails before the pointer changes, the table stays at the old version.

**5. Why is a catalog needed once several engines need to share the same Iceberg table?**

When several engines (for example Spark, DuckDB, Trino or Flink) use the same table, they must all agree on which `metadata.json` is the current version. The catalog is the single place that keeps this information and gives a name to the table (like `lab.silver.orders`). It also protects commits: if two engines try to write at the same time, the catalog accepts only one commit and the other one must try again, so no write is lost. Without a shared catalog, each engine could read a different version, or two writers could overwrite each other's changes. Our Hadoop catalog is simple (only files in the bucket), but it is not safe for concurrent writes on S3, so in production we usually use a catalog service like a REST catalog, Hive Metastore, Nessie or AWS Glue.

## Exercises

In this section, we do the same operations on the users table. The code is at the end of the notebook, in the "Exercises" part.

### 1. Create `lab.silver.users` as an Iceberg table

This exercise checks that the users table exists as an Iceberg table.

The table `lab.silver.users` was already created in the previous lab from `silver/spark/users.parquet` with `writeTo("lab.silver.users").createOrReplace()`, so we did not create it again (this would create one more snapshot). We checked it: it has 50 users and 1 snapshot (`1533708320527085805`, operation `overwrite`, 2026-10-28). Its schema has 10 columns: `user_id`, `username`, `name`, `sex`, `email`, `birthdate` (date), `street`, `city`, `state`, `zip_code`.

### 2. Append a new batch of users and inspect the snapshots

This exercise adds new users and shows the new snapshot.

We took 5 existing users, gave them a new `user_id` with `uuid()`, and appended them with `writeTo("lab.silver.users").append()`. The table now has 55 users and 2 snapshots:

| snapshot_id | parent_id | operation |
|---|---|---|
| 1533708320527085805 | NULL | overwrite |
| 7345446893973528675 | 1533708320527085805 | append |

Like for the orders, the new snapshot has the first snapshot as parent, and the old Parquet file was not changed.

### 3. Query the table before and after the append using time travel

This exercise compares the old and the new version of the users table.

| Query | Result |
|---|---|
| `VERSION AS OF 1533708320527085805` (before the append) | 50 users |
| current table (after the append) | 55 users |

Time travel reads the first snapshot, which only knows the first Parquet file (50 users). The current snapshot also knows the new file with the 5 new users.

### 4. Add a `country` column and write new data using the evolved schema

This exercise tests schema evolution on the users table.

We ran `ALTER TABLE lab.silver.users ADD COLUMN country STRING`, then we appended 2 new users (new `user_id`) with `country = 'France'`. We used a DataFrame (`withColumn` + `writeTo(...).append()`) instead of `INSERT INTO ... SELECT`, so we did not need to write all the column names.

| country | users |
|---|---|
| France | 2 |
| NULL | 55 |

The 55 old users have `country = NULL`, because their Parquet files were written before the new column and were not rewritten. Only the 2 new users have a value. The table now has 57 users and 3 snapshots (the third one is `4877665324485103323`).

### 5. Compare the S3 layout of the users and orders tables

This exercise compares the files of the two tables in S3.

| | `warehouse/silver/users/` | `warehouse/silver/orders/` |
|---|---|---|
| Parquet data files | 3 (7,435 B + 3,518 B + 3,525 B) | 3 (84,055 B + 1,956 B + 2,201 B) |
| Manifests (`...-m0.avro`) | 3 | 3 |
| Manifest lists (`snap-...avro`) | 3 | 3 |
| Metadata files | `v1` to `v4.metadata.json` | `v1` to `v4.metadata.json` |
| `version-hint.text` | yes (version 4) | yes (version 4) |

The two tables have exactly the same layout, because we did the same operations on both: one creation, one append, one `ADD COLUMN` and one write with the new schema. Each table has its own folder with a `data/` part and a `metadata/` part. For each write, Iceberg added one Parquet file, one manifest, one manifest list and one `metadata.json`. For the `ADD COLUMN`, it only added one `metadata.json` (`v3`), with no data file and no snapshot. In both tables, the old files were never changed. The only big difference is the size of the data: the users table is much smaller (50 users in 7,435 bytes) than the orders table (2,829 orders in 84,055 bytes). We can also see that small writes create small files with a fixed cost: 2 users take 3,525 bytes, and 1 order takes 2,201 bytes.

### 6. Explain the difference between Parquet, Iceberg, and an Iceberg catalog

This exercise sums up the three ideas of the lab.

**Parquet** is a file format. It stores data in columns, with compression and a schema, but only for one file. A folder of Parquet files is not a real table: there is no history, no transactions and no list of valid files. For example, `silver/spark/orders.parquet/` is just one `part-00000-...parquet` file and a `_SUCCESS` file.

**Iceberg** is a table format. It adds metadata files on top of Parquet files: `metadata.json` files, manifest lists and manifests. This metadata says which Parquet files belong to the table at each snapshot. Because of this, we get snapshots (3 for `orders`), time travel (2,829 rows in the first snapshot and 2,832 in the second one), schema evolution without rewriting files (the `source` column) and atomic commits.

An **Iceberg catalog** is the service that knows the tables by name and keeps the pointer to the current `metadata.json` of each table. In our lab, it is the Hadoop catalog `lab`, and the pointer is the `version-hint.text` file. With the catalog, we can write `lab.silver.orders` instead of an S3 path. It is also the single source of truth when several engines read or write the same table, and it makes sure that only one commit wins at a time.

In short: Parquet stores the data, Iceberg describes the table, and the catalog says where the current version of the table is.

## Problems met during the lab

This section lists the problems we had and how we solved them.

- **Expired credentials** (`InvalidAccessKeyId` with the `aws` command): the STS credentials of the new service were already expired. We logged out and logged in again on Onyxia, created a new service, and configured the AWS profile again from the environment variables.
- **Python code in the terminal**: the SparkSession code must be run in a notebook cell, not in the bash terminal.
- **Lost Git repository**: when we deleted the old service, the `~/work` folder was deleted too, so we cloned the repository again.
- **Slow `files` metadata table**: the query on `lab.silver.orders.files` was blocked. We interrupted it and restarted the kernel, and we used the snapshot summary instead.

## Cleanup

This section explains how to remove the lab data at the end.

The lab asks to delete the Iceberg warehouse and to stop Spark:

```bash
aws s3 --profile default rm "s3://$LAB_BUCKET_NAME/warehouse/" --recursive
```

```python
spark.stop()
```