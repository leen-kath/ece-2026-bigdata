# Lab: Bronze to silver with Spark DataFrames

In this lab, I build the same silver layer as in the dbt lab, but with PySpark. I read the bronze CSV files from the S3 storage of Onyxia, I clean them with the DataFrame API and with Spark SQL, and I write the result as Parquet files and as Iceberg tables. At the end, I compare the two approaches.

## Environment

This section explains the setup and the problems I had before the lab could run.

- Service: `vscode-pyspark` on Onyxia, with cpu `4000m` and memory `4000Mi`.
- Versions: Spark `4.1.1`, Scala `2.13`, `hadoop-aws-3.4.2.jar`.
- Iceberg package: `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0` (it matches Spark 4.1 and Scala 2.13).
- Bronze data: `bronze/users.csv` (50 users) and `bronze/orders.csv` (2829 orders).
- The code is in the notebook `lab_spark_silver/lab.ipynb`.

Problems I had:

- **Expired credentials.** My first service had old STS credentials. The AWS CLI returned `InvalidAccessKeyId`. I launched a new service to get new credentials, and I created the AWS profile again from the environment variables.
- **Not enough memory.** One of my services was launched with only 2000Mi of memory (I checked with `cat /sys/fs/cgroup/memory.max`). When Spark started, VS Code was disconnected and the page reloaded many times. With 4000Mi, everything worked.
- **Long code in the terminal.** Pasting long blocks in the `pyspark` shell broke the code, so I used a Jupyter notebook in VS Code instead.

## SparkSession with S3 access

The SparkSession connects Spark to the S3 storage with the temporary credentials of Onyxia, and declares the Iceberg catalog `lab`.

### Question 1: The credentials are temporary. What happens to a running Spark job if the service is restarted mid-lab, and why?

The Spark job stops and fails. In this lab Spark runs in `local[*]` mode, so the driver and the executors are inside one JVM, in the container of the Onyxia service. When the service is restarted, the container is stopped, so the JVM is killed and the job is killed with it. If the job was writing files to S3, the output can stay half-written (for example some part-files are there and others are missing).

After the restart, Onyxia gives new credentials as environment variables. But the old SparkSession does not exist anymore, and the old keys are not valid anymore. So we have to start a new SparkSession, which reads the new values, and run the job again from the start.

Even without a restart, there is a similar problem: the keys are copied into the Spark configuration only when the session is created. Spark does not refresh them. So when the credentials expire, the next read or write to S3 fails with an access error.

I saw this problem at the start of the lab: my old service had expired credentials, and the AWS CLI returned `InvalidAccessKeyId`. After launching a new service, the new credentials worked.

### Question 2: Why does a plain `SimpleAWSCredentialsProvider` fail here, while it worked in setups without a session token?

Onyxia gives temporary credentials (STS). They are made of three values: the access key, the secret key and the session token. The three values only work together: the storage server checks the token to know that the temporary key is valid.

`SimpleAWSCredentialsProvider` only reads the access key and the secret key. It ignores the session token. So the request is sent without the token, and the server refuses it.

In a setup with permanent keys (for example a fixed user key on MinIO), there is no token. Two values are enough, so the simple provider works.

`TemporaryAWSCredentialsProvider` reads the three values, including `fs.s3a.session.token`. This is why the lab uses it.

## Bronze layer

The two CSV files are read directly from S3 with `header=True`, `inferSchema=True` and `multiLine=True`.

- `inferSchema=True` found the right types, for example `birthdate` as a `date`. It is fine here because the files are very small, but on real data the schema should be written by hand.
- `multiLine=True` is needed because the `address` column contains a line break (the street, then `City, ST 12345`). Without it, one user would be cut into two broken rows.

I checked that no rows were merged: reading `orders.csv` without `multiLine` also gives 2829 rows, and there is no NULL in the key columns.

## Silver layer

The silver layer uses the same rules as `stg_users` and `stg_orders` in the dbt lab. Only the engine changes.

**Users.** The columns are renamed and cleaned (trim, upper for `sex`, lower for `email`), and the address is split into `street`, `city`, `state` and `zip_code`. For a military address like `DPO AP 09617`, there is no city, so `city` is NULL. `zip_code` stays a string, so the first zero is kept (`01352`).

**Orders.** The orders are deduplicated with a window function: `row_number()` over `partitionBy("order_id")`, ordered by `ordered_at` descending, and we keep only row number 1. To test it, I added 3 duplicates on purpose:

| Step | Number of orders |
|---|---|
| Original | 2829 |
| With 3 duplicates | 2832 |
| After deduplication | 2829 |

After deduplication, no `order_id` appears more than once. The Spark SQL version gives the same number of rows and the same columns as the DataFrame version.

### Question 1: Explain the output of the code that was just run.

The query groups the users by state. For each state, `users` is the number of users, and `missing_city` is the number of users with no city (city is NULL). The table is sorted by the number of users, from the biggest to the smallest.

The numbers are small (2 or 3 users per state) because the dataset is small: `users.csv` is only about 7 KB, so there are not many users for each state.

Only AE and AP have missing cities, and all their users have no city (AE: 3 of 3, AP: 2 of 2). AE and AP are not real states, they are codes for military addresses, like `DPO AP 09617`. These addresses have no city and no comma, so the city regex does not match, and the city becomes NULL. This shows that the address parsing works well.

FM and VI are also not states (they are territories), but their addresses have the normal format `City, ST 12345`, so the city is found and `missing_city` is 0.

### Question 2: Compare the DataFrame and SQL cells. Which one for a one-off exploration, and which for a model reused by several colleagues? Why?

Both give the same result, because Spark turns both into the same execution plan (Catalyst). So the choice is about the people, not about speed.

For a one-off exploration, I would choose Spark SQL. It is fast to write, and most people can read SQL. It is good to test an idea quickly.

For a model reused by several colleagues, I would choose the DataFrame API. It is Python code, so we can put the steps in functions, reuse them, test them, and get errors earlier (for example a wrong column name). It is also easier to build step by step, like we did with `users_typed` then `stg_users`.

But if the colleagues only know SQL, SQL can also be a good choice for a shared model, like in the dbt lab.

### Question 3: `regexp_extract()` returned an empty string instead of NULL. Where else in this notebook could this silently produce a wrong result?

The same problem can happen with `state` and `zip_code`, because they also use `regexp_extract()`. In the notebook, only `city` is fixed with `when(... == "", None)`.

If an address does not match the pattern, `state` and `zip_code` become `""` and not NULL. Then:

- In the `GROUP BY state` query, the empty string would be a group with no name, counted like a real state.
- A check like `state IS NULL` or `count(state)` would think the value exists.

To be safe, we should apply the same fix to `state` and `zip_code`, or use `NULLIF(..., '')` in SQL.

## Write the silver layer as Parquet

Spark writes each DataFrame to S3 as a folder of Parquet files.

```
silver/spark/users.parquet/_SUCCESS
silver/spark/users.parquet/part-00000-...-c000.snappy.parquet     (9 581 bytes)
silver/spark/orders.parquet/_SUCCESS
silver/spark/orders.parquet/part-00000-...-c000.snappy.parquet    (139 556 bytes)
```

When I read the files back, I get 50 users and 2829 orders, so no rows were lost or duplicated.

### Question 1: List the objects written under `silver/spark/users.parquet/`. How many part-files are there, and why?

There is only one part-file for users (and one for orders). Spark writes one part-file for each partition of the DataFrame. Our datasets are very small (`users.csv` is about 7 KB, `orders.csv` about 330 KB), so Spark read each CSV in only one partition.

For orders, the window function makes a shuffle, which normally creates 200 partitions (`spark.sql.shuffle.partitions`). But Spark's Adaptive Query Execution (AQE) sees that the data is very small and merges these partitions into one. So there is still only one part-file.

On a big dataset, there would be many part-files, one for each partition.

### Question 2: `coalesce(1)` before `.write` would produce a single file. What do you lose by doing that on a larger dataset?

We lose the parallelism. With `coalesce(1)`, all the data goes into one partition, so only one task (one core) writes the whole file, and the other cores do nothing. On a big dataset this is very slow.

It can also use too much memory on one executor, and the job can fail. And for the readers, one very big file is harder to read in parallel than many medium files.

`coalesce(1)` is fine only for small data, for example to export a small result.

## Write the silver layer as Iceberg tables

The same DataFrames are written as Iceberg tables `lab.silver.users` and `lab.silver.orders`, in the `warehouse/` folder of the bucket.

```
warehouse/silver/orders/data/00000-64-...-00001.parquet          (84 055 bytes, 2829 rows)
warehouse/silver/orders/metadata/v1.metadata.json
warehouse/silver/orders/metadata/snap-4219974134046780544-1-...avro
warehouse/silver/orders/metadata/...-m0.avro
warehouse/silver/orders/metadata/version-hint.text
```

The `snapshots` table shows one snapshot (one commit, operation `overwrite`), and the `files` table shows one Parquet file with 2829 rows. When I read the tables back, I get 50 users and 2829 orders.

The Iceberg data file is smaller (84 055 bytes) than the Parquet file written by Spark (139 556 bytes) for the same data, because Iceberg uses zstd compression by default, and `.write.parquet()` uses snappy.

### Question 1: Compare `silver/orders.parquet/` and `warehouse/silver/orders/`. Which files hold the data, and which ones describe the table?

In both cases, the data is in Parquet files: `part-00000-...snappy.parquet` for the Parquet output, and `data/00000-...parquet` for the Iceberg table. Both have 2829 rows.

The Parquet folder has nothing that describes the table. The `_SUCCESS` file only says that the job finished. A reader must list the folder and read the schema from the Parquet files.

The Iceberg table has a `metadata/` folder that describes the table:

- `version-hint.text` says which version is the current one (here `1`).
- `v1.metadata.json` has the schema and the list of snapshots.
- `snap-...avro` (manifest list) and `...-m0.avro` (manifest) give the exact list of data files, with their number of rows and their size.

So with Iceberg, the table is not "all the files in the folder", it is "the files listed in the current metadata".

### Question 2: A second job overwrites the Parquet directory while a query is reading it. What does the reader see? What changes with an Iceberg table?

With a Parquet folder, there is no protection. The overwrite deletes the old files and writes new ones. The reader can see a mix of old and new files, or some data is missing, or the query fails because a file was deleted during the read (`FileNotFoundException`). The result can be wrong without any error.

With Iceberg, the new job writes new data files and new metadata files next to the old ones. At the end, it changes the pointer to the new version in one step (atomic commit). The reader that started before keeps reading the old snapshot, which is complete and still exists. The next query will see the new version. So a reader never sees a half-written table.

### Question 3: With this catalog, the pointer to the current version of a table is a file in the bucket. What can go wrong if two jobs commit at the same time, and what would a catalog service add?

With the `hadoop` catalog, both jobs can read the same current version (v1), and both try to write `v2.metadata.json` and update `version-hint.text`. S3 does not have a safe atomic rename, so the second job can overwrite the first one. The changes of the first job are lost, or the pointer can be in a bad state. This catalog is only safe with one writer at a time.

A catalog service (for example a REST catalog, Nessie, Polaris, Hive Metastore or AWS Glue) keeps the pointer in a central place and changes it with a "compare and swap": the commit only works if the version did not change in the meantime. If two jobs commit at the same time, the second one sees the conflict and must retry, so nothing is lost. A catalog service also gives one list of all tables, access control, and lets several engines (Spark, DuckDB, Trino...) use the same tables.

## Commit the Iceberg table

Only the metadata of the Iceberg tables is committed to Git, not the data files. I copied the `warehouse/` folder from S3 with `aws s3 sync`, then I added only the two `metadata/` folders (and the notebook) to the commit. The commit has 9 files: for each table, the `v1.metadata.json`, the `version-hint.text`, the manifest list and the manifest.

## Compare with the dbt lab

Both labs build the same silver layer. Here I compare the results and the two ways of working.

### Question 1: What differs between the two schemas? Spark has no UUID type: what did you do about it, and what is the consequence for the consumers of the silver layer?

I compared the two schemas with `DESCRIBE` in DuckDB:

| Column | dbt | Spark |
|---|---|---|
| `order_id` | `uuid` | `varchar` |
| `user_id` | `uuid` | `varchar` |
| `ordered_at` | `timestamp with time zone` | `timestamp` |
| `quantity` | `integer` | `integer` |
| `product` | `varchar` | `varchar` |

Spark has no UUID type, so I kept `order_id` and `user_id` as strings with `cast("string")`. In the dbt output, DuckDB stores them with the `uuid` type.

For `ordered_at`, the dbt file has a timestamp with a time zone, but the Spark file has a simple timestamp, without time zone information.

For the consumers, the same column does not have the same type in the two outputs. A join between the two outputs needs a cast (for example `uuid` to `varchar`). A string also takes more space than a UUID, and nothing checks that the value is a real UUID. For `ordered_at`, a consumer of the Spark file does not know in which time zone the time is, so it can be read with a few hours of difference if the tools use different time zones.

### Question 2: Compare the output layout: one object per model with dbt, one directory of part-files with Spark. Which one is easier for a consumer to read? Which one scales?

One file per model (dbt) is easier for a consumer: there is one path, and any tool can open it directly.

A folder of part-files (Spark) is a bit harder: the consumer must read the whole folder or use a pattern like `*.parquet`. But this layout scales: each task writes its own file in parallel, and readers can also read the files in parallel. With very big data, one single file would be too slow to write and to read.

### Question 3: The dbt lab validates the silver layer with the `unique`, `not_null`, `accepted_values` and `relationships` tests. What did this lab use instead, and what would you need to write to get the same guarantees?

This lab only used `assert` on row counts (bronze vs silver, DataFrame vs SQL) and a `groupBy().count()` to check that there are no duplicate `order_id` after deduplication.

To get the same guarantees as dbt, I would need to write checks like these:

- `unique`: `groupBy("order_id").count().filter("count > 1")` must be empty.
- `not_null`: `filter(col("order_id").isNull())` must return 0 rows.
- `accepted_values`: `filter(~col("sex").isin("M", "F"))` must return 0 rows (same idea for `product`).
- `relationships`: a `left_anti` join from `stg_orders` to `stg_users` on `user_id` must return 0 rows (every order has a real user).

We could put these checks in functions or use a tool like Great Expectations. With dbt, these tests are already built in and run with `dbt build`.

### Question 4: `ref()` and `source()` give dbt the dependency graph of the models. What plays this role in the notebook?

In the notebook, the dependencies are the Python variables and the order of the cells. For example, `stg_users` is built from `users_typed`, which is built from `users_bronze`. Spark keeps this lineage in the logical plan of each DataFrame.

But there is no real graph of the models: if I run the cells in the wrong order, it fails, and Spark does not know which saved table depends on which. With dbt, `ref()` and `source()` give this graph automatically, and dbt runs the models in the right order.

### Question 5: Compare the time to build the silver layer with `dbt build` and with the cells of this lab. Why are these numbers not directly comparable?

The numbers are not directly comparable because the two runs do not do the same work:

- Spark must start a JVM and load the Iceberg library, which takes many seconds before any work begins. DuckDB starts almost instantly.
- In the notebook, I run the cells one by one, with many actions (`show`, `count`, asserts). Each action can compute the data again, because there is no cache.
- The outputs are different: this lab writes Parquet and also Iceberg tables, while `dbt build` writes the models and runs the tests.
- The data is very small, so the time is mostly fixed overhead, not real processing. On big data, the result could be the opposite.

### Question 6: For each of these cases, would you choose dbt with DuckDB or Spark: a daily batch of 10 GB, a daily batch of 10 TB, a team of analysts who only know SQL?

- **Daily batch of 10 GB:** dbt with DuckDB. One machine can process 10 GB easily, and it is simpler and cheaper than a Spark cluster.
- **Daily batch of 10 TB:** Spark. This is too big for one machine, so we need a distributed engine that splits the work across many executors.
- **Team of analysts who only know SQL:** dbt (with DuckDB, or with another SQL engine if the data is big). The models are written in SQL, and dbt gives tests, documentation and the dependency graph for free.