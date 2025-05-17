-- продажи по дням
CREATE MATERIALIZED VIEW mv_sales_daily
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY date
AS
SELECT
    date,
    count() AS sales_count,
    sum(quantity) AS total_quantity,
    sum(total_price) AS total_revenue
FROM sales
GROUP BY date;

-- продажи по продовцам
CREATE MATERIALIZED VIEW mv_sales_by_seller
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (seller_id, date)
AS
SELECT
    seller_id,
    date,
    count() AS sales_count,
    sum(quantity) AS total_quantity,
    sum(total_price) AS total_revenue
FROM sales
GROUP BY seller_id, date;

-- продажи по клиентам
CREATE MATERIALIZED VIEW mv_sales_by_customer
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (customer_id, date)
AS
SELECT
    customer_id,
    date,
    count() AS sales_count,
    sum(quantity) AS total_quantity,
    sum(total_price) AS total_revenue
FROM sales
GROUP BY customer_id, date;

-- топ товары по продажам
CREATE MATERIALIZED VIEW mv_top_products
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (product_id, date)
AS
SELECT
    product_id,
    date,
    sum(quantity) AS total_quantity,
    sum(total_price) AS total_revenue
FROM sales
GROUP BY product_id, date;

-- повторные покупки клиентов
CREATE MATERIALIZED VIEW mv_customer_repeat_count
ENGINE = AggregatingMergeTree()
PARTITION BY customer_id % 10
ORDER BY customer_id
AS
SELECT
    customer_id,
    countDistinct(date) AS active_days,
    count() AS total_orders
FROM sales
GROUP BY customer_id;

-- средний чек по продовцам
CREATE MATERIALIZED VIEW mv_avg_check_per_seller
ENGINE = AggregatingMergeTree()
PARTITION BY seller_id % 10
ORDER BY seller_id
AS
SELECT
    seller_id,
    avg(total_price) AS avg_check,
    count() AS total_sales
FROM sales
GROUP BY seller_id;

-- продажи по неделям
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_sales_weekly
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY week
AS
SELECT
    toStartOfWeek(date) AS week,
    date,
    sum(quantity) AS total_quantity,
    sum(total_price) AS total_revenue,
    count() AS total_sales
FROM sales
GROUP BY week, date;

-- средний обьем заказа
CREATE MATERIALIZED VIEW mv_avg_order_volume
ENGINE = AggregatingMergeTree()
ORDER BY tuple()
AS
SELECT
    avg(quantity) AS avg_quantity,
    avg(total_price) AS avg_price
FROM sales;
