# Lab: Introduction to Spark's RDD API

In this lab we use the RDD API of Spark, which is the low-level API, on the bronze datasets (`users.csv` and `orders.csv`). The idea is to do everything by hand first, to understand what the DataFrame API does for us.

## Environment

In this part we prepare the local files and the Spark session.

RDDs can't read directly from S3A, so I downloaded the two bronze files from my bucket (`user-k-sison-ece`) into the project folder:

| File | Size |
|---|---|
| `users.csv` | 7 351 bytes (~7 KB) |
| `orders.csv` | 331 568 bytes (~324 KB) |

I had a problem at the beginning: my S3 credentials were expired (`InvalidAccessKeyId` error). So I restarted the vscode-pyspark service to get new credentials. In the new service there was no `default` profile, so I used `aws s3` without `--profile` and it worked.

After that I launched the shell with `pyspark --master local[*]` (Spark 4.1.1, Python 3.13) and I got the `SparkContext` from the `SparkSession`.

## 1. RDD creation and partitions

In this part we create RDDs from the CSV files and we look at how Spark splits them into partitions.

Results:

| Command | Result |
|---|---|
| `users_lines.getNumPartitions()` | 2 |
| `orders_lines.getNumPartitions()` | 2 |
| `sc.defaultMinPartitions` | 2 |
| `sc.defaultParallelism` | 2 |
| `orders_lines.repartition(8).getNumPartitions()` | 8 |

**Q1. What decides the number of partitions `textFile()` picks by default, for a file this size?**

Normally `textFile()` splits the file depending on the block size, which is around 32 MB or 128 MB. Our files are very small (7 KB and 324 KB), so only one split should be enough. But there is also a minimum number of partitions, called `minPartitions`. By default it is equal to `min(defaultParallelism, 2)`. In our case we have 2 cores with `local[*]`, so `defaultParallelism` is 2 and the minimum is 2. That's why we get 2 partitions for both files, even if they are small.

**Q2. `repartition()` and `coalesce()` both change the partition count. Which one shuffles data, and which one doesn't?**

`repartition()` always makes a shuffle. All the rows can move to any partition, so the data is well balanced, and we can increase or decrease the number of partitions. Actually `repartition(n)` is the same as `coalesce(n, shuffle=True)`.

`coalesce()` doesn't make a shuffle by default. It just merges some partitions together without moving the data on the network. So it is cheaper, but we can only use it to reduce the number of partitions, and the partitions can be unbalanced.

## 2. Lazy evaluation and lineage

In this part we want to see that transformations are lazy. They only build a lineage, and Spark does nothing until we call an action.

First I removed the header of `orders_lines` with `filter()`, and I printed the lineage with `toDebugString()`:

(2) PythonRDD[10] at RDD at PythonRDD.scala:58 []
 |  orders.csv MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0 []
 |  orders.csv HadoopRDD[2] at textFile at NativeMethodAccessorImpl.java:0 []

We can see that no data is read at this moment, it is just the list of steps. We read it from the bottom: `HadoopRDD` reads the file, then `MapPartitionsRDD` makes one element for each line, and `PythonRDD` is my filter with the lambda. The `(2)` is the number of partitions. All the lines are at the same level, so there is no shuffle and everything is in only one stage.

After that I called `count()` two times, and I got **2829** orders both times (the file has 2830 lines with the header).

**Q1. Call `orders_data.count()` a second time. Does the Spark UI show a new stage? Why?**

Yes, there is a new job with a new stage. Spark doesn't keep the result of the RDD after the action. So every time we call an action, Spark runs all the lineage again from the start: it reads `orders.csv` again, it filters again and it counts again.

**Q2. At which point in this notebook would `.cache()` change that answer?**

We need to call `.cache()` on `orders_data` before the first action, for example `orders_data = orders_lines.filter(...).cache()`. Like this, the first `count()` computes the partitions and keeps them in memory. For the second `count()` there is still a job, but Spark reads the data from the cache and not from the file. We can see it in the Storage tab of the Spark UI. Also `.cache()` is lazy too, the data is only saved when the first action runs.

## 3. The cost of no schema: the multiline address

In this part we read `users.csv` with `textFile()`, without a schema, and we see the problem.

I printed the first 6 elements of `users_lines`:

uuid,username,name,sex,address,mail,birthdate
bdd640fb-0667-4ad1-9c80-317fa3b1799d,garzaanthony,Charles Garcia,M,"908 Jennifer Squares
Robinsonshire, KY 01352",helenpeterson@gmail.com,1935-09-19
17be3111-1a2a-43ed-962b-0f79c37459ee,blairamanda,Ryan Munoz,M,"Unit 6184 Box 9593
DPO AP 09617",stanleykendra@gmail.com,2009-12-15
47294739-614f-43d7-99db-3ad0ddd1dfb2,elizabethmiles,Jacob Wood,M,"283 Steven Groves

The address is on two lines. `textFile()` cuts at each new line, so one user is split in two elements. The second element has no uuid.

`users_lines.count()` gives **101**: 1 header + 50 users x 2 lines.

Then I kept only the lines that start with a UUID. `users_records.count()` gives **50**, the real number of users. But the second lines are just deleted. So we lose the end of the address, the `mail` and the `birthdate`. We can only use `uuid`, `username`, `name` and `sex`.

**Q1. Why does `spark.read.csv(multiLine=True)` not have this problem, while `sc.textFile()` does?**

`spark.read.csv` knows the CSV format. With `multiLine=True`, it understands that a new line inside quotes is part of the value. `textFile()` doesn't know CSV, it just cuts the text at each new line.

**Q2. In general terms, what has to be true of a file format for a line-oriented reader like `textFile()` to be safe to use on it?**

Each record must be on one line only, with no new line inside the values. Then one line = one record. For example `orders.csv`, log files or JSON Lines.

## 4. Manual parsing of orders

In this part we parse the orders by hand, with `split(",")`. It is OK here because `orders.csv` has no new line inside the values.

Each line becomes a dictionary. `quantity` is converted to `int` and `product` is put in lowercase. I added `.cache()` because the next parts use `orders_rdd` again.

Result of `orders_rdd.take(3)` (first element):

{'order_id': '2339ba19-2563-4cc3-8a97-ebf555d596af', 'user_id': 'bdd640fb-0667-4ad1-9c80-317fa3b1799d', 'date': '2020-01-01 00:02:17.891488+00:00', 'quantity': 4, 'product': 'drink'}

The `date` is still a string, not a real date. And there is no check: if a line is wrong, we only see the error at the action that reads this line.

**Q1. What would happen to this cell if one line in `orders.csv` had an extra comma inside `product`?**

`split(",")` would give 6 values instead of 5. Python would give an error `ValueError: too many values to unpack`. Because of lazy evaluation, the error comes only at the action that reads this line, and the whole job fails.

**Q2. Where does the DataFrame API perform the equivalent check, and when?**

In the CSV reader, `spark.read.csv`, with a schema. It knows the CSV rules, so a comma inside quotes is not a problem. When a line doesn't match the schema, by default (`mode="PERMISSIVE"`) the bad values become `null` and the job doesn't crash. We can also use `FAILFAST` to stop on the first error, or `DROPMALFORMED` to remove the bad lines. The check happens when the data is read, at the action.

## 5. Key-value RDDs: total quantity per user

In this part we compute the total quantity per user in two ways: with `reduceByKey` and with `groupByKey`.

First we make pairs `(user_id, quantity)`, then we group by key. Both versions give the same result, for example:

('f143262f-dc5c-4eed-8da0-365bf89897b9', 12)
('969b6662-0562-4059-968c-c69b1064005c', 40)
('ce9e1a11-fcbb-4e59-bbdd-cf7c9c96e9ec', 253)

The result is the same, but the shuffle is not the same.

**Q1. `reduceByKey` combines values on each partition before shuffling. What does `groupByKey` shuffle instead, and why is that more expensive?**

`groupByKey` sends all the pairs `(user_id, quantity)` in the shuffle, one by one, and the sum is done after. `reduceByKey` first makes the sum inside each partition, so it sends only one pair per user per partition.

With our data: 2829 orders, 50 users, 2 partitions. `reduceByKey` sends about 100 pairs (50 users x 2 partitions). `groupByKey` sends all the 2829 pairs. So more data goes on the network and in memory, and it is slower. With a big dataset, one key with a lot of values can also make an executor run out of memory.

**Q2. For which aggregations would `groupByKey` be unavoidable, `reduceByKey` not being able to express them?**

When we need all the values of a key at the same time, not only a partial result. For example the median, sorting the values of each user, or getting the full list of orders of each user. `reduceByKey` only works when we can combine two values into one value of the same type, like a sum, a max or a min.

## 6. Manual deduplication

In this part we remove the duplicate orders by hand, with `reduceByKey`. For each `order_id` we keep the most recent order.

We make pairs `(order_id, order)`, then `reduceByKey` compares two orders with the same `order_id` and keeps the one with the biggest date.

Result: `orders_rdd.count()` = **2829** and `orders_dedup.count()` = **2829**. So there is no duplicate `order_id` in this file, but the code is ready if there are some.

Note: the dates are compared as strings. It works only because all the dates have the same format, so the text order is the same as the time order.

**Q1. `partitionBy()` in the window function is described as a wide transformation, shuffling rows so every row of a given `order_id` lands on the same partition. What plays that role here?**

`reduceByKey`. It makes a shuffle by key, so all the orders with the same `order_id` go to the same partition. Then it can compare them and keep the most recent one.

**Q2. Which version states the intent more directly: the window function, or the `reduceByKey` with a custom combiner?**

The window function. With `row_number()`, `PARTITION BY order_id` and `ORDER BY ordered_at DESC`, we can read directly "keep the last order for each id". With `reduceByKey` we must read the `most_recent` function to understand what the code does.

## 7. Joining orders with users

In this part we join the orders with the users, using key-value RDDs with `user_id` as the key.

For the users I kept only `uuid` and `username`, because the other columns are not usable after the filter of part 3. Each element of the result is `(user_id, (order, username))`, for example:

('f143262f-dc5c-4eed-8da0-365bf89897b9', ({'order_id': 'f1f7d904-1307-4157-98c1-28e7d6cd3155', ..., 'quantity': 1, 'product': 'bread'}, 'kayla51'))

Results:

| Command | Result |
|---|---|
| `enriched.count()` (join) | 2829 |
| `enriched_left.count()` (leftOuterJoin) | 2829 |
| orders with no user (`None`) | 0 |

All the orders found their user, so the two joins give the same result here.

**Q1. `join()` is a wide transformation on both RDDs. What would need to be true of how `orders_kv` and `users_kv` are partitioned for Spark to skip the shuffle?**

The two RDDs must be partitioned in the same way: same number of partitions and same hash function on `user_id` (for example `partitionBy(4)` on both, and cached). Then the orders and the user with the same `user_id` are already in the same partition, so Spark doesn't need to move the data.

**Q2. Replace `join()` with `leftOuterJoin()`. What changes in the result if a `user_id` in orders has no matching user?**

With `join()`, the order is removed from the result. With `leftOuterJoin()`, the order is kept, and the username is `None`: `(user_id, (order, None))`. In our data there is no missing user, so the result is the same (2829 and 0 `None`).

**Q3. `enriched` is not yet filtered or aggregated. What is the risk of calling `.collect()` on it directly, on a dataset much larger than this one?**

`collect()` sends all the data to the driver. With a big dataset, the driver doesn't have enough memory and it crashes (out of memory). A join can also make more rows than at the start. It is better to filter or aggregate first, or to use `take()`, or to write the result in a file.

## 8. Exercises

### Exercise 1: aggregateByKey

The goal is to get `(order_count, total_quantity)` for each user in only one pass, and then the average quantity per order.

`aggregateByKey` takes 3 things:
- a start value `(0, 0)`,
- a function to add one quantity inside a partition: `(count + 1, total + q)`,
- a function to merge the results of two partitions: we add the counts and the totals.

We can't do this with `reduceByKey`, because the input is a number but the result is a pair.

Results (first 3 users):

| user_id | (order_count, total_quantity) | average |
|---|---|---|
| f143262f-... | (4, 12) | 3.0 |
| 969b6662-... | (12, 40) | 3.33 |
| ce9e1a11-... | (85, 253) | 2.98 |

The totals are the same as in part 5, so the result is correct.

### Exercise 2: leftOuterJoin with a fake user

The goal is to see what `leftOuterJoin` does when an order has a `user_id` that doesn't exist.

I copied the first order and changed its `order_id` and its `user_id` to a fake value (`00000000-0000-0000-0000-000000000000`). Then I added it to the orders with `union`, and I made a `leftOuterJoin` with the users.

Results:

| Command | Result |
|---|---|
| `orders_with_fake.count()` | 2830 |
| orders with no user (`None`) | 1 |

The fake order is kept, with `None` for the username:

('00000000-0000-0000-0000-000000000000', ({'order_id': 'fake-order-0001', ..., 'quantity': 4, 'product': 'drink'}, None))

With a normal `join()`, this order would be removed and we would not see it. With `leftOuterJoin()` we can find the orders with no user, so it is useful to check the quality of the data.

### Exercise 3: the same aggregation with the DataFrame API

The goal is to compare the RDD version of `user_quantity` with the DataFrame version.

First I wrote `groupBy("user_id")`, but I got an `AnalysisException`: the column is called `user_uuid` in the file (the name `user_id` came only from my `parse_order` function). The error came directly, before reading the data, because the DataFrame knows the schema. With RDDs, an error only comes at the action.

The working code:

user_quantity_df = orders_df.groupBy("user_uuid").sum("quantity")

Result (5 rows):

| user_uuid | sum(quantity) |
|---|---|
| d47d577b-... | 80 |
| 0b2f6d5c-... | 190 |
| d9e604b3-... | 184 |
| 97fa7f04-... | 285 |
| 2051acef-... | 301 |

**Number of lines:** the DataFrame version needs 2 lines (read the CSV + `groupBy().sum()`). The RDD version needs to remove the header, write the `parse_order` function (about 10 lines), make the pairs and call `reduceByKey`.

**Plan of the DataFrame (`.explain()`):**

HashAggregate(keys=[user_uuid], functions=[sum(quantity)])
+- Exchange hashpartitioning(user_uuid, 200)
   +- HashAggregate(keys=[user_uuid], functions=[partial_sum(quantity)])
      +- FileScan csv [user_uuid, quantity]

From the bottom: Spark reads only the 2 columns it needs, makes a partial sum in each partition, does a shuffle by `user_uuid`, and makes the final sum. So it does the same thing as `reduceByKey` automatically.

**Lineage of the RDD (`.toDebugString()`):**

(2) PythonRDD[100] at RDD at PythonRDD.scala:58 []
 |  MapPartitionsRDD[21] at mapPartitions at PythonRDD.scala:170 []
 |  ShuffledRDD[20] at partitionBy at NativeMethodAccessorImpl.java:0 []
 +-(2) PairwiseRDD[19] at reduceByKey at <python-input-5>:3 []
    |  PythonRDD[18] at reduceByKey at <python-input-5>:3 []
    |  PythonRDD[16] at RDD at PythonRDD.scala:58 []
    |      CachedPartitions: 2; MemorySize: 168.0 KiB; DiskSize: 0.0 B
    |  orders.csv MapPartitionsRDD[3] at textFile at NativeMethodAccessorImpl.java:0 []
    |  orders.csv HadoopRDD[2] at textFile at NativeMethodAccessorImpl.java:0 []

The `+-` shows the shuffle of `reduceByKey`, so there are 2 stages. We can also see that `orders_rdd` is in the cache (2 partitions, 168 KiB).

The difference: the RDD lineage only shows the steps that I wrote by hand, and Spark can't change them. The DataFrame plan is made by Spark itself (Catalyst optimizer), so it can choose the best way, for example read only the needed columns.

### Exercise 4: time with and without cache

The goal is to see if `.cache()` makes the actions faster. I made a version of `orders_rdd` and `user_quantity` without cache, and I measured each `count()` two times with `time.perf_counter()`.

| Action | Run 1 | Run 2 |
|---|---|---|
| `orders.count` with cache | 0.408 s | 0.193 s |
| `orders.count` without cache | 0.210 s | 0.204 s |
| `user_quantity.count` with cache | 0.191 s | 0.297 s |
| `user_quantity.count` without cache | 0.315 s | 0.200 s |

All the times are around 0.2 s, so we can't see a real difference. The data is too small (324 KB): reading and parsing the file is very fast, and most of the time is the cost to start a Spark job. The first time (0.408 s) is slower only because it is the first job (warm-up).

Also, the second `user_quantity.count` without cache is fast too. This is because Spark keeps the shuffle files, so it can skip the stage before the shuffle, even without cache.

In the Storage tab of the Spark UI, `orders_rdd` is in the cache (2 partitions, about 168 KiB), and the version without cache is not there.

Conclusion: `.cache()` is useful when the data is big or the computation is long, and when we use the same RDD many times. With a small file like this one, it doesn't change anything.