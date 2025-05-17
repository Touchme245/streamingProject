CREATE TABLE sales
(
    sale_id UInt64,               -- если задавать вручную, иначе можно убрать
    product_id UInt32,
    customer_id UInt32,
    seller_id UInt32,
    quantity UInt32,
    total_price Float32,
    date Date
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, product_id, customer_id, seller_id);