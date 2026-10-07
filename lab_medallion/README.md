# Lab: Bronze to Silver with dbt and DuckDB

In this lab, we use dbt with DuckDB to transform the raw CSV files from the bronze layer (stored on S3) into clean data in the silver layer.

## Bronze layer

Here are my answers to the questions about the bronze sources.

**1. The bronze CSV files are left untouched. What is the benefit when a bug is found in a transformation, six months later?**

Since we never modify the bronze files, we always keep the original data. So if we find a bug in a transformation, we just need to correct the SQL model and run `dbt build` again. dbt will rebuild the silver data from the raw files, and we don't lose anything. We also don't have to ask the source to send the data again, which is useful because maybe they don't have it anymore. It is also good for checking the results, because we can always go back to the original data.

**2. The source reads the whole file on every query. Which other format and layout would you choose if the orders were ingested every hour for years?**

I think I would use Parquet instead of CSV. Parquet stores the data by column, it is compressed and it keeps the types, so DuckDB can read only the columns it needs and it is much faster. I would also split the files by date (partitioning), with a new file for each ingestion, for example:

`bronze/orders/year=2026/month=10/day=07/hour=14/orders.parquet`

Like this, if we want the orders of yesterday, DuckDB only reads the files of yesterday and not all the data since the beginning. And when new data arrives, we just add a new file without changing the old ones.

## Silver layer

Here are my answers to the questions about the silver models.

**1. The silver models are views. What happens when a view is queried, and when is it preferable to a table?**

A view does not store any data, it only stores the SQL query. So every time we query the view, DuckDB runs the query again: it reads the CSV files on S3, does the casts and the cleaning, and then gives the result. The good point is that the data is always up to date with the bronze files and it takes no storage space. A view is better when the data is small, when it changes often, or when the view is not queried a lot. If the transformation is heavy or if many dashboards query it often, a table is better, because the work is done only one time during `dbt run`.

**2. UUID values are stored in 16 bytes. How many bytes does the VARCHAR representation use?**

A UUID written as text looks like `2339ba19-2563-4cc5-...`: it has 32 hexadecimal characters and 4 dashes, so 36 characters. Each character uses 1 byte, so the VARCHAR uses 36 bytes, more than 2 times the 16 bytes of the UUID type (and in practice a bit more, because DuckDB also stores the length of the string). With millions of rows, using the UUID type saves a lot of space and comparisons are faster.


## Data tests

Here are my answers to the questions about the data tests.

**1. Which strategy would you choose for a dashboard displaying the age of the customers? For a dashboard displaying the sales per region?**

For the age dashboard, the birthdate is the main information, so a wrong birthdate gives a wrong age (sometimes negative). Here I would replace the invalid birthdates with NULL in the silver layer. Like this, we keep the user and his orders, but these users are not used to calculate the ages. Rejecting the users is not a good idea, because we would also lose their orders.

For the sales per region dashboard, the birthdate is not used at all. The orders are still real sales, so we must keep them, otherwise the sales numbers would be too low. In this case, I would just keep the test as a warning to monitor the problem, and ask the producer of the data to fix the generator.

**2. The addresses associate states with random zip codes (KY 01352 for example). Which test would detect it, and which reference data would it require?**

We need a test that checks if the zip code really belongs to the state. It can be a singular test that joins `stg_users` with a reference table and returns the users where the zip code is not in the range of their state. For the reference data, we need a table with the valid zip codes (or zip ranges / the first 3 digits) for each state, for example from the US postal service (USPS) or the US Census. In dbt, this small reference table can be stored as a seed (a CSV file in the `seeds/` folder).

I also noticed that the `relationships` test on `state` uses `ref('states')`, but this model does not exist in the project. dbt only shows a warning and the test is not executed, so we must be careful and always read the warnings.


## Lineage

dbt knows the dependencies between the resources thanks to `source()` and `ref()`. `source()` is used for raw data that dbt does not create (here the CSV files in bronze), dbt only reads them. `ref()` is used for another model of the project, that dbt builds itself. Because of `ref()`, dbt knows in which order it must build the models, and it can draw the lineage graph. In our project the graph is simple: `bronze.users` goes to `stg_users`, and `bronze.orders` goes to `stg_orders`. With `dbt build`, dbt follows this graph: it builds a model and then runs its tests directly, without waiting for the other models.

## Export of the silver layer to Parquet

In this part, we exported the silver models from DuckDB to Parquet files on S3, in `silver/dbt/`.

To check the export of `stg_orders`, I compared the view and the Parquet file:

- number of rows: 2829 in the view and 2829 in the Parquet file
- with an `EXCEPT` query, there are 0 different rows, so the content is the same

I also compared the size of the files:

| File | CSV (bronze) | Parquet (silver) |
|------|--------------|-----------------|
| orders | 331 568 bytes | 72 466 bytes |
| users | 7 351 bytes | 7 018 bytes |

For the orders, the Parquet file is about 4.6 times smaller than the CSV (78% less). For the users, the difference is very small, because there are only 50 rows and Parquet adds some metadata (schema, statistics) in the file.

The difference between the two approaches: the view only stores the SQL query, so the transformation is done again each time we query it (logical materialization). The Parquet file stores the result, so the transformation is done only one time during the export, and after we just read the data (physical materialization). But if the bronze CSV changes, the view gives the new data directly, while the Parquet file stays the same until we export it again.