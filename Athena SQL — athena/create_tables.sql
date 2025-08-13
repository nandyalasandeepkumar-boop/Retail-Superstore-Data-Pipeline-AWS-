-- Replace <YOUR_BUCKET> below

-- Create external table with partition projection (no MSCK needed)
CREATE EXTERNAL TABLE IF NOT EXISTS retail_superstore.sales_curated (
  order_id        string,
  order_date      date,
  ship_date       date,
  customer_id     string,
  customer_name   string,
  segment         string,
  city            string,
  state           string,
  postal_code     string,
  region          string,
  product_id      string,
  category        string,
  sub_category    string,
  product_name    string,
  sales           double,
  quantity        int,
  discount        double,
  profit          double
)
PARTITIONED BY (order_year int, order_month int)
STORED AS PARQUET
LOCATION 's3://<YOUR_BUCKET>/curated/sales/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.order_year.type'='integer',
  'projection.order_year.range'='2015,2035',
  'projection.order_month.type'='integer',
  'projection.order_month.range'='1,12',
  'storage.location.template'='s3://<YOUR_BUCKET>/curated/sales/order_year=${order_year}/order_month=${order_month}/'
);

-- (Optional) Helpful views for QuickSight
CREATE OR REPLACE VIEW retail_superstore.v_monthly_sales AS
SELECT date_trunc('month', order_date) AS month_dt,
       sum(sales) AS total_sales,
       sum(profit) AS total_profit
FROM retail_superstore.sales_curated
GROUP BY 1
ORDER BY 1;

CREATE OR REPLACE VIEW retail_superstore.v_category_perf AS
SELECT category, sub_category,
       sum(sales) AS sales,
       sum(profit) AS profit
FROM retail_superstore.sales_curated
GROUP BY 1,2
ORDER BY sales DESC;
