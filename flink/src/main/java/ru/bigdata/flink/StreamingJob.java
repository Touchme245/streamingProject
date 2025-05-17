package ru.bigdata.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import ru.bigdata.flink.configuration.FlinkConnectionUtils;
import ru.bigdata.flink.dto.SaleDto;
import ru.bigdata.flink.mapper.SaleMapper;
import java.sql.Date;
import static ru.bigdata.flink.PostgresTableUtils.*;

public class StreamingJob {

    final static String inputTopic = "streaming";
    final static String jobTitle = "KafkaToPostgresTest";

    public static void main(String[] args) throws Exception {
        final String bootstrapServers = "localhost:29092";
        final String jdbcUrl = "jdbc:postgresql://localhost:5438/db";
        final String jdbcUser = "user";
        final String jdbcPassword = "password";

        FlinkConnectionUtils flinkConnectionUtils = new FlinkConnectionUtils(bootstrapServers, inputTopic, jdbcUrl, jdbcUser, jdbcPassword);
        PostgresCommandsExecutor commandsExecutor = new PostgresCommandsExecutor(jdbcUrl, jdbcUser, jdbcPassword);

        JdbcConnectionOptions postgresConnectorOptions = flinkConnectionUtils.getPostgresConnectionOptions();
        KafkaSource<String> source = flinkConnectionUtils.getKafkaSource();

        executeSchemaSetup(commandsExecutor);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> text = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        DataStream<SaleDto> dtoDataStream = text.map(new SaleMapper());

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO snowflake.fact_sales " +
                                "(product_id, customer_id, seller_id, quantity, total_price, date) " +
                                "VALUES (?, ?, ?, ?, ?, ?)",

                        (ps, sale) -> {
                            ps.setInt(1, sale.getProduct_id());
                            ps.setInt(2, sale.getCustomer_id());
                            ps.setInt(3, sale.getSeller_id());
                            ps.setInt(4, sale.getQuantity());
                            ps.setFloat(5, sale.getTotal_price());
                            ps.setDate(6, Date.valueOf(sale.getDate()));
                            System.out.println("запись данных в постгрес");
                        },

                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(0)
                                .build(),

                        postgresConnectorOptions
                )
        );

        dtoDataStream.addSink(
                JdbcSink.sink(
                        "INSERT INTO sales (product_id, customer_id, seller_id, quantity, total_price, date) " +
                                "VALUES (?, ?, ?, ?, ?, ?)",

                        (ps, sale) -> {
                            ps.setInt(1, sale.getProduct_id());
                            ps.setInt(2, sale.getCustomer_id());
                            ps.setInt(3, sale.getSeller_id());
                            ps.setInt(4, sale.getQuantity());
                            ps.setFloat(5, sale.getTotal_price());
                            ps.setDate(6, Date.valueOf(sale.getDate()));
                            System.out.println("запись данных в кликхаус");
                        },

                        JdbcExecutionOptions.builder()
                                .withBatchSize(1)
                                .withBatchIntervalMs(0)
                                .build(),

                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:clickhouse://localhost:8123/default?user=user&password=password")
                                .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                                .build()
                )
        );

        env.execute(jobTitle);
    }

    private static void executeSchemaSetup(PostgresCommandsExecutor commandsExecutor) {
        try {
            commandsExecutor.executeSqlQuery(DROP_SQEMA_DDL);
            commandsExecutor.executeSqlQuery(CREATE_SQEMA_DDL);
            commandsExecutor.executeSqlQuery(FACT_SALES_DDL);
        } catch (Exception e) {
            System.err.println("Ошибка при выполнении SQL DDL запросов: ");
            e.printStackTrace();
        }
    }
}
