package ru.bigdata.flink.configuration;

import lombok.AllArgsConstructor;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

@AllArgsConstructor
public class FlinkConnectionUtils {

    private final String bootstrapServers;
    private final String inputTopic;
    private final String jdbcUrl;
    private final String jdbcUser;
    private final String jdbcPassword;

    public KafkaSource<String> getKafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(inputTopic)
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }

    public JdbcConnectionOptions getPostgresConnectionOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(jdbcUser)
                .withPassword(jdbcPassword)
                .build();
    }

}
