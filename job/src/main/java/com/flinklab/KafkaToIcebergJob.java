package com.flinklab;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
// import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.sink.dynamic.DynamicIcebergSink;

import org.apache.iceberg.jdbc.JdbcCatalog;
// import org.apache.kafka.clients.consumer.OffsetResetStrategy;


/**
 * Kafka JSON -&gt; Iceberg DynamicIcebergSink (JDBC catalog + S3/MinIO warehouse).
 *
 * <p>All endpoints come from environment variables (see env/local.flink.env).
 */
public final class KafkaToIcebergJob {
  private KafkaToIcebergJob() {}

  public static void main(String[] args) throws Exception {
    String bootstrap = Env.require("KAFKA_BOOTSTRAP_SERVERS");
    String topic = Env.get("KAFKA_TOPIC", "events");
    String groupId = Env.get("KAFKA_GROUP_ID", "flink-iceberg-lab");
    String starting = Env.get("KAFKA_STARTING_OFFSETS", "earliest");

    String catalogName = Env.get("ICEBERG_CATALOG_NAME", "iceberg");
    String jdbcUri = Env.require("ICEBERG_JDBC_URI");
    String jdbcUser = Env.require("ICEBERG_JDBC_USER");
    String jdbcPassword = Env.require("ICEBERG_JDBC_PASSWORD");
    String warehouse = Env.require("ICEBERG_WAREHOUSE");
    String namespace = Env.get("ICEBERG_NAMESPACE", "lake");
    String ioImpl = Env.get("ICEBERG_IO_IMPL", "org.apache.iceberg.aws.s3.S3FileIO");

    String s3Endpoint = Env.require("S3_ENDPOINT");
    String s3AccessKey = Env.require("S3_ACCESS_KEY");
    String s3SecretKey = Env.require("S3_SECRET_KEY");
    boolean pathStyle = Env.getBool("S3_PATH_STYLE_ACCESS", true);
    String s3Region = Env.get("S3_REGION", "us-east-1");

    long checkpointMs = Env.getLong("FLINK_CHECKPOINT_INTERVAL_MS", 10_000L);
    int parallelism = Env.getInt("FLINK_PARALLELISM", 1);

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(parallelism);
    env.enableCheckpointing(checkpointMs);
    CheckpointConfig checkpointConfig = env.getCheckpointConfig();
    checkpointConfig.setMinPauseBetweenCheckpoints(checkpointMs / 2);
    checkpointConfig.setCheckpointTimeout(checkpointMs * 6);

    // OffsetsInitializer offsets =
    //     "latest".equalsIgnoreCase(starting)
    //         ? OffsetsInitializer.latest()
    //         : OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);

    Properties kafkaProps = new Properties();
    kafkaProps.setProperty("auto.offset.reset", starting.toLowerCase());

    KafkaSource<String> kafkaSource =
        KafkaSource.<String>builder()
            .setBootstrapServers(bootstrap)
            .setTopics(topic)
            .setGroupId(groupId)
            // .setStartingOffsets(offsets)
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .setProperties(kafkaProps)
            .build();

    DataStream<String> stream =
        env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-" + topic);

    CatalogLoader catalogLoader =
        buildCatalogLoader(
            catalogName,
            jdbcUri,
            jdbcUser,
            jdbcPassword,
            warehouse,
            ioImpl,
            s3Endpoint,
            s3AccessKey,
            s3SecretKey,
            pathStyle,
            s3Region);

    DynamicIcebergSink.forInput(stream)
        .generator(new JsonToDynamicRecordGenerator(namespace))
        .catalogLoader(catalogLoader)
        .immediateTableUpdate(true)
        .uidPrefix("iceberg-dynamic")
        .append();

    env.execute("kafka-to-iceberg-dynamic");
  }

  private static CatalogLoader buildCatalogLoader(
      String catalogName,
      String jdbcUri,
      String jdbcUser,
      String jdbcPassword,
      String warehouse,
      String ioImpl,
      String s3Endpoint,
      String s3AccessKey,
      String s3SecretKey,
      boolean pathStyle,
      String s3Region) {

    Map<String, String> props = new HashMap<>();
    props.put(CatalogProperties.URI, jdbcUri);
    props.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
    props.put(CatalogProperties.CATALOG_IMPL, JdbcCatalog.class.getName());
    props.put("jdbc.user", jdbcUser);
    props.put("jdbc.password", jdbcPassword);
    props.put("jdbc.driver", "org.postgresql.Driver");
    props.put(CatalogProperties.FILE_IO_IMPL, ioImpl);

    // Iceberg S3FileIO / AWS SDK settings
    props.put("s3.endpoint", s3Endpoint);
    props.put("s3.path-style-access", Boolean.toString(pathStyle));
    props.put("s3.access-key-id", s3AccessKey);
    props.put("s3.secret-access-key", s3SecretKey);
    props.put("client.region", s3Region);
    props.put("s3.region", s3Region);

    Configuration hadoopConf = new Configuration(false);
    hadoopConf.set("fs.s3a.endpoint", s3Endpoint);
    hadoopConf.set("fs.s3a.path.style.access", Boolean.toString(pathStyle));
    hadoopConf.set("fs.s3a.access.key", s3AccessKey);
    hadoopConf.set("fs.s3a.secret.key", s3SecretKey);
    hadoopConf.set("fs.s3a.connection.ssl.enabled", s3Endpoint.startsWith("https") ? "true" : "false");

    return CatalogLoader.custom(catalogName, props, hadoopConf, JdbcCatalog.class.getName());
  }
}
