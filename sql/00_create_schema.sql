CREATE SCHEMA IF NOT EXISTS analytics;

DROP TABLE IF EXISTS analytics.fact_sales;
DROP TABLE IF EXISTS analytics.dim_customer;
DROP TABLE IF EXISTS analytics.dim_product;
DROP TABLE IF EXISTS analytics.dim_date;
DROP TABLE IF EXISTS analytics.dim_country;

CREATE TABLE analytics.dim_customer (
  customer_key SERIAL PRIMARY KEY,
  customer_id  TEXT UNIQUE
);

CREATE TABLE analytics.dim_product (
  product_key  SERIAL PRIMARY KEY,
  stockcode    TEXT,
  description  TEXT,
  UNIQUE(stockcode, description)
);

CREATE TABLE analytics.dim_country (
  country_key SERIAL PRIMARY KEY,
  country     TEXT UNIQUE
);

CREATE TABLE analytics.dim_date (
  date_key    INTEGER PRIMARY KEY,     -- yyyymmdd
  date_value  DATE UNIQUE,
  year        INTEGER,
  month       INTEGER,
  month_name  TEXT,
  day         INTEGER,
  day_of_week INTEGER
);

CREATE TABLE analytics.fact_sales (
  sales_key    BIGSERIAL PRIMARY KEY,
  invoiceno    TEXT,
  customer_key INTEGER REFERENCES analytics.dim_customer(customer_key),
  product_key  INTEGER REFERENCES analytics.dim_product(product_key),
  country_key  INTEGER REFERENCES analytics.dim_country(country_key),
  date_key     INTEGER REFERENCES analytics.dim_date(date_key),
  quantity     INTEGER,
  unitprice    NUMERIC(10,2),
  revenue      NUMERIC(12,2)
);

CREATE INDEX IF NOT EXISTS idx_fact_sales_date ON analytics.fact_sales(date_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_customer ON analytics.fact_sales(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_sales_product ON analytics.fact_sales(product_key);

