# Lab: SQL analytics with DuckDB — Answers

## S3 configuration

**The persistent secret is stored in a file of the home directory. What are the risks, and why are they limited on Onyxia?**

The file contains the credentials in clear. If someone or some program can read it (copy in a Git repository, backup, shared archive), they can access the bucket as if they were me. On Onyxia the risk is limited: the credentials are temporary (session token, they expire), they only give access to my own bucket, and the service runs in my own container that other users cannot see.

**The secret is visible to any process running as your user. How would you restrict the access to S3 for a Kubernetes Job?**

The Job should have its own credentials with only the permissions it needs (least privilege), for example read only on `bronze/`. They are stored in a Kubernetes Secret injected only in this Job, and RBAC limits who can read this Secret. The best is short-lived credentials obtained automatically by the Job (workload identity) instead of fixed keys.

## Query the bronze layer

**Why does `approx_unique` return 40 and 3167 instead of 50 and 2829?**

`approx_unique` is an approximation computed with the HyperLogLog algorithm. It hashes each value and keeps only a small summary of fixed size, so it is fast and uses little memory, but the result has an error (here -20% and +12%). The exact value needs `count(DISTINCT ...)`.

**The types are inferred from a sample. Which issue may occur with a large file whose first rows are not representative?**

DuckDB may choose a wrong type. For example, if a column has only numbers in the first rows, it chooses `BIGINT`, and a later value like `N/A` makes the reading fail. Solutions: `sample_size = -1` to read the whole file, or give the types with the `columns` option.

## Exercises

**1. Average number of orders per user, and average quantity per order**

```sql
WITH per_user AS (
  SELECT user_uuid, count(*) AS nb_orders FROM orders GROUP BY user_uuid
)
SELECT avg(nb_orders) AS avg_orders_per_user,
  (SELECT round(avg(quantity), 2) FROM orders) AS avg_quantity_per_order
FROM per_user;
```

Result: 56.58 orders per user and 3.05 units per order.

**2. First order, last order and days between them**

```sql
SELECT u.username, min(o.date) AS first_order, max(o.date) AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.username
ORDER BY days_between DESC, u.username;
```

Result: the longest period is 5 days (hoganashlee) and 6 users have 0 days. The generator gives each user a block of consecutive orders, one per hour, so the orders of a user are not spread over the whole dataset.

**3. Hour of the day with the highest quantity**

```sql
SELECT hour(date) AS hour, count(*) AS orders, sum(quantity) AS quantity
FROM orders GROUP BY hour
ORDER BY quantity DESC, hour LIMIT 5;
```

Result: 6 am with 385 units. All hours have 118 orders and the top 5 is between 365 and 385 units, so this ranking is only chance (synthetic data).

**4. Month-over-month variation per product with `lag`**

```sql
WITH monthly AS (
  SELECT product, strftime(date, '%Y-%m') AS month, sum(quantity) AS quantity
  FROM orders GROUP BY product, month
)
SELECT product, month, quantity,
  lag(quantity) OVER (PARTITION BY product ORDER BY month) AS prev_quantity,
  round(100 * (quantity - lag(quantity) OVER (PARTITION BY product ORDER BY month))
        / lag(quantity) OVER (PARTITION BY product ORDER BY month), 1) AS variation_pct
FROM monthly ORDER BY product, month;
```

Result (cookie): NULL, -1.3%, -8.4%, -18.2%. January is NULL because there is no previous month. April is incomplete (it stops on the 27th), so its variation is biased downward.

**5. Users who ordered every product**

```sql
SELECT u.username, count(DISTINCT o.product) AS nb_products
FROM orders o JOIN users u ON o.user_uuid = u.uuid
GROUP BY u.username
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
```

Result: 45 users of 50. The 5 others have very few orders (2 to 12).

## Parquet export

**Why is the `uuid` column barely compressed?**

All the 2829 values are different and random. The dictionary encoding is useless and Snappy finds no repeated pattern. In comparison, `user_uuid` has only 50 distinct values and takes 2 KB.

**Why is the compressed size of some columns larger than their uncompressed size?**

Snappy adds a small header to each block. When the data is already compact (dictionary codes for `product`, binary integers for `date`), it cannot reduce it and we only pay the header.

## Hive partitioning

**Why is partitioning by `uuid` a bad idea?**

It would create 2829 files of one row (small files problem). On object storage each file needs at least one HTTP request with a fixed latency, the listing is slow and paginated, and each file has its own footer bigger than the data. Also, no analytical query filters on one order `uuid`.

**Which partition column would you choose for a dataset of orders growing every day?**

A column derived from the date (day or month, depending on the volume). Queries usually filter on a period, so the pruning works, and every day the pipeline only adds a new partition without rewriting the old ones.

## CSV vs. Parquet at scale

My large dataset has 252,416 orders (dates from 2020 to 2048).

| Query | Data received | GET | Time |
| --- | --- | --- | --- |
| CSV, sum per product | 28.2 MiB | 1 | 0.910 s |
| Parquet, sum per product | 203.5 KiB | 4 | 0.419 s |
| Parquet, date >= 2100 | 16.0 KiB | 1 | 0.228 s |
| Parquet, date >= 2040 | 1022.4 KiB | 3 | 0.334 s |

**How many row groups does the file contain, and how many are skipped by the filter?**

3 row groups (123,573 / 124,276 / 4,567 rows). With `date >= '2100-01-01'`, all 3 are skipped because no maximum reaches 2100: only the footer is read. With `date >= '2040-01-01'`, row group 0 (max 2034) is skipped and the 2 others are read.

**The orders are sorted by date. What would happen if they were shuffled?**

Each row group would have a min close to 2020 and a max close to 2048, so the statistics could not skip any row group and the whole `date` column would be read.

**Which part of the difference is due to the network, which part to the parsing?**

The same CSV query on the local file took 0.233 s, which is the parsing part. So about 0.68 s of the 0.910 s (75%) is due to the network. Parquet avoids both: less data to download and no text to parse.
