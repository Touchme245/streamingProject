package ru.bigdata.flink;

public class PostgresTableUtils {

    public static String DROP_SQEMA_DDL = "DROP SCHEMA snowflake CASCADE;";
    public static String CREATE_SQEMA_DDL = "CREATE SCHEMA IF NOT EXISTS snowflake;";

    public static String FACT_SALES_DDL =  "CREATE TABLE IF NOT EXISTS snowflake.fact_sales (\n" +
            "    sale_id SERIAL PRIMARY KEY,\n" +
            "    product_id INTEGER NOT NULL,\n" +
            "    customer_id INTEGER NOT NULL,\n" +
            "    seller_id INTEGER NOT NULL,\n" +
            "    quantity INTEGER NOT NULL,\n" +
            "    total_price NUMERIC(10, 2) NOT NULL,\n" +
            "    date DATE NOT NULL\n" +
            ");";

}
