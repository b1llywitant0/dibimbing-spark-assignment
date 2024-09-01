import pyspark
import os

from dotenv import load_dotenv
from pathlib import Path
from pyspark.sql.types import StructType
from pyspark.sql.functions import to_timestamp,col,when,count,isnan,month
import pyspark.sql.functions as F

dotenv_path = Path('/resources/.env')
load_dotenv(dotenv_path=dotenv_path)

postgres_host = os.getenv('POSTGRES_CONTAINER_NAME')
postgres_port = os.getenv('POSTGRES_PORT')
postgres_user = os.getenv('POSTGRES_USER')
postgres_password = os.getenv('POSTGRES_PASSWORD')

sparkcontext = pyspark.SparkContext.getOrCreate(conf=(
        pyspark
        .SparkConf()
        .setAppName('Dibimbing')
        .setMaster('local')
    ))
    
sparkcontext.setLogLevel("WARN")

spark = pyspark.sql.SparkSession(sparkcontext.getOrCreate())

jdbc_url = f'jdbc:postgresql://{postgres_host}:{postgres_port}/warehouse'
jdbc_properties = {
    'user': postgres_user,
    'password': postgres_password,
    'driver': 'org.postgresql.Driver'
}

print(jdbc_url)

retail_df = spark.read.jdbc(
    jdbc_url,
    'public.retail',
    properties=jdbc_properties
)

retail_df.show(5)

retail_df.printSchema()

retail_df = retail_df.withColumn('trxvalue', F.round(F.col('unitprice')*F.col('quantity'),2))

retail_df.createOrReplaceTempView('retail')

# I don't want to speculate about the data and making transformation based on assumptions only.
# Thus, for safety measures, I will exclude data based on outliers of quantity and unitprice.
# Also, excluding NULL rows on customerid, invoiceno, stockcode.

# unitprice
retail_df = spark.sql('''
    WITH 
    q1 AS (
        SELECT 
        MAX(unitprice) AS quartile1_break
        FROM ( 
            SELECT *,
            NTILE(4) OVER (ORDER BY unitprice) AS unitprice_quartile
            FROM retail ) AS quartiles
        WHERE unitprice_quartile IN (1)
        GROUP BY unitprice_quartile
    ),
    q3 AS (
        SELECT 
        MAX(unitprice) AS quartile3_break
        FROM ( 
            SELECT *,
            NTILE(4) OVER (ORDER BY unitprice) AS unitprice_quartile
            FROM retail ) AS quartiles
        WHERE unitprice_quartile IN (3)
        GROUP BY unitprice_quartile
    ),
    iqr AS (
        SELECT *, q3.quartile3_break - q1.quartile1_break AS IQR
        FROM q1, q3
    )
    SELECT 
    retail.* 
    FROM retail, iqr
    WHERE unitprice BETWEEN iqr.quartile1_break-1.5*iqr.IQR AND iqr.quartile3_break+1.5*iqr.IQR
    AND unitprice != 0
''')

retail_df.createOrReplaceTempView('retail')

# quantity
retail_df = spark.sql('''
    WITH 
    q1 AS (
        SELECT 
        MAX(quantity) AS quartile1_break
        FROM ( 
            SELECT *,
            NTILE(4) OVER (ORDER BY quantity) AS quantity_quartile
            FROM retail ) AS quartiles
        WHERE quantity_quartile IN (1)
        GROUP BY quantity_quartile
    ),
    q3 AS (
        SELECT 
        MAX(quantity) AS quartile3_break
        FROM ( 
            SELECT *,
            NTILE(4) OVER (ORDER BY quantity) AS quantity_quartile
            FROM retail ) AS quartiles
        WHERE quantity_quartile IN (3)
        GROUP BY quantity_quartile
    ),
    iqr AS (
        SELECT *, q3.quartile3_break - q1.quartile1_break AS IQR
        FROM q1, q3
    )
    SELECT 
    retail.* 
    FROM retail, iqr
    WHERE quantity BETWEEN iqr.quartile1_break-1.5*iqr.IQR AND iqr.quartile3_break+1.5*iqr.IQR
    AND quantity > 0
''')

retail_df = retail_df.na.drop(subset=['customerid','invoiceno','stockcode'])

retail_df.createOrReplaceTempView('retail')

# Descriptive Analytics
spark.sql('''
    SELECT 
    DISTINCT(YEAR(invoicedate)) AS year,
    COUNT(DISTINCT(MONTH(invoicedate))) AS n_month,
    COUNT(DISTINCT(customerid)) AS total_users,
    COUNT(DISTINCT(invoiceno)) AS total_transaction, 
    ROUND(SUM(trxvalue),2) AS total_transaction_value,
    ROUND(SUM(trxvalue)/COUNT(DISTINCT(invoiceno)),2) AS value_per_transaction,
    ROUND(SUM(trxvalue)/COUNT(DISTINCT(MONTH(invoicedate))),2) AS value_per_month
    FROM retail
    GROUP BY year
''').write.jdbc(url=jdbc_url, table='public.descriptive', mode="overwrite", properties=jdbc_properties)

# Top 10 Countries with The Highest Transaction Value
spark.sql('''
    SELECT 
    country,
    ROUND(SUM(trxvalue),2) AS total_transaction_value
    FROM retail
    GROUP BY country
    ORDER BY total_transaction_value DESC
    LIMIT 10
''').write.jdbc(url=jdbc_url, table='public.top10countries', mode="overwrite", properties=jdbc_properties)

# Top 10 Countries with Highest YoY % Incremental in Dec 2011
spark.sql('''
WITH 
data AS (
    SELECT 
    DISTINCT(YEAR(invoicedate)) AS year,
    MONTH(invoicedate) AS month,
    country AS country,
    ROUND(SUM(unitprice),2) AS total_transaction_value
    FROM retail
    GROUP BY country, year, month
    HAVING month==12    
    ORDER BY country, year
    )

SELECT
    country,
    ROUND(MAX(
        CASE
            WHEN prev_year_revenue IS NOT NULL
            THEN (total_transaction_value - prev_year_revenue) * 100.0 / prev_year_revenue
            ELSE NULL
        END
    ),2) AS YoY_percent_incremental
FROM (
    SELECT
        country,
        year,
        total_transaction_value,
        LAG(total_transaction_value) OVER (PARTITION BY country ORDER BY year) AS prev_year_revenue
    FROM data
) AS RevenueWithPrevious
GROUP BY country
HAVING YoY_percent_incremental > 0
ORDER BY YoY_percent_incremental DESC
LIMIT 10
''').write.jdbc(url=jdbc_url, table='public.top10countries_incremental', mode="overwrite", properties=jdbc_properties)

# Churn rate per quarter
spark.sql('''
WITH 
quarters AS (
    SELECT DISTINCT
        customerid,
        CASE
            WHEN EXTRACT(MONTH FROM invoicedate) BETWEEN 1 AND 3 THEN CONCAT(EXTRACT(YEAR FROM invoicedate), '-Q1')
            WHEN EXTRACT(MONTH FROM invoicedate) BETWEEN 4 AND 6 THEN CONCAT(EXTRACT(YEAR FROM invoicedate), '-Q2')
            WHEN EXTRACT(MONTH FROM invoicedate) BETWEEN 7 AND 9 THEN CONCAT(EXTRACT(YEAR FROM invoicedate), '-Q3')
            ELSE CONCAT(EXTRACT(YEAR FROM invoicedate), '-Q4')
        END AS quarter
    FROM retail
    ORDER BY customerid
),
userid AS (
    SELECT DISTINCT 
        customerid
    FROM retail
),
period AS (
    SELECT DISTINCT
        quarter
    FROM Quarters
),
cross AS (
    SELECT * FROM userid CROSS JOIN period
),
joined AS (
    SELECT 
        c.customerid, 
        c.quarter AS sort_q, 
        q.quarter AS active_q 
    FROM cross c
    LEFT JOIN quarters q ON c.customerid = q.customerid 
    AND c.quarter = q.quarter
),
record AS (
    SELECT 
        *,
        LEAD(active_q) OVER (PARTITION BY customerid ORDER BY sort_q) AS next_q
    FROM joined
),
almost AS (
    SELECT 
        customerid, sort_q, active_q, next_q 
    FROM record
    WHERE active_q IS NOT NULL
    ORDER BY customerid, active_q
),
active AS (
    SELECT 
        sort_q, COUNT(DISTINCT(customerid)) AS active 
    FROM almost
    WHERE active_q IS NOT NULL
    GROUP BY sort_q
    ORDER BY sort_q
),
churned AS (
    SELECT
        active_q, COUNT(DISTINCT(customerid)) AS churned
    FROM almost
    WHERE next_q IS NULL
    GROUP BY active_q
    ORDER BY active_q
)
SELECT
    a.sort_q AS quarter,
    COALESCE(c.churned, 0) AS churned_customers,
    COALESCE(a.active, 0) AS total_active_customers,
    CASE
        WHEN COALESCE(a.active, 0) = 0 THEN 0
        ELSE COALESCE(c.churned, 0) * 100.0 / COALESCE(a.active, 0)
    END AS churn_rate
FROM active a
LEFT JOIN churned c
ON a.sort_q = c.active_q
ORDER BY quarter;
''').write.jdbc(url=jdbc_url, table='public.churn_rate_quarterly', mode="overwrite", properties=jdbc_properties)