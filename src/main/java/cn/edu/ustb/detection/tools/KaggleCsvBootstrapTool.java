package cn.edu.ustb.detection.tools;

import cn.edu.ustb.detection.model.UserBehavior;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.BufferedWriter;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Convert Kaggle CSV files to project-standard UserBehavior JSON and optionally
 * push to Kafka.
 *
 * <p>
 * Example:
 *
 * <pre>
 * java -cp target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar \
 *   cn.edu.ustb.detection.tools.KaggleCsvBootstrapTool \
 *   --input D:/data/events.csv \
 *   --output-jsonl D:/data/events.jsonl \
 *   --kafka-bootstrap localhost:9092 \
 *   --kafka-topic user-behavior \
 *   --profile auto
 * </pre>
 */
public class KaggleCsvBootstrapTool {

    private static final Logger LOG = LoggerFactory.getLogger(KaggleCsvBootstrapTool.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String input = required(params, "input");
        String profileValue = params.getOrDefault("profile", "auto").trim().toUpperCase();
        KaggleUserBehaviorMapper.Profile profile = KaggleUserBehaviorMapper.Profile.valueOf(profileValue);
        String outputJsonl = params.get("output-jsonl");
        String kafkaBootstrap = params.get("kafka-bootstrap");
        String kafkaTopic = params.get("kafka-topic");
        long maxRows = parseLong(params.getOrDefault("max-rows", "-1"));

        boolean enableKafka = kafkaBootstrap != null && kafkaTopic != null;
        boolean enableFile = outputJsonl != null;
        if (!enableKafka && !enableFile) {
            throw new IllegalArgumentException(
                    "At least one output is required: --output-jsonl <file> or --kafka-bootstrap + --kafka-topic");
        }

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);

        KaggleUserBehaviorMapper mapper = new KaggleUserBehaviorMapper();
        KafkaProducer<String, String> producer = enableKafka ? createProducer(kafkaBootstrap) : null;
        BufferedWriter writer = enableFile ? Files.newBufferedWriter(Paths.get(outputJsonl)) : null;

        long total = 0L;
        long converted = 0L;
        long invalid = 0L;
        long sentKafka = 0L;
        long writtenFile = 0L;

        try (Reader csvReader = Files.newBufferedReader(Paths.get(input));
                CSVParser parser = CSVFormat.DEFAULT.builder().setHeader().setSkipHeaderRecord(true).setTrim(true)
                        .setIgnoreSurroundingSpaces(true).build().parse(csvReader)) {

            for (CSVRecord record : parser) {
                total++;
                if (maxRows > 0 && converted >= maxRows) {
                    break;
                }

                Map<String, String> row = record.toMap();
                UserBehavior behavior = mapper.map(row, profile);
                if (behavior == null || !behavior.isValid()) {
                    invalid++;
                    continue;
                }

                String json = objectMapper.writeValueAsString(behavior);
                converted++;

                if (writer != null) {
                    writer.write(json);
                    writer.newLine();
                    writtenFile++;
                }

                if (producer != null) {
                    ProducerRecord<String, String> message = new ProducerRecord<>(kafkaTopic, behavior.getUserId(),
                            json);
                    Future<?> future = producer.send(message);
                    future.get();
                    sentKafka++;
                }

                if (converted % 10000 == 0) {
                    LOG.info("Progress: total={}, converted={}, invalid={}, kafkaSent={}, fileWritten={}", total,
                            converted, invalid, sentKafka, writtenFile);
                }
            }
        } finally {
            if (writer != null) {
                writer.flush();
                writer.close();
            }
            if (producer != null) {
                producer.flush();
                producer.close(Duration.ofSeconds(5));
            }
        }

        LOG.info("Bootstrap finished: total={}, converted={}, invalid={}, kafkaSent={}, fileWritten={}, profile={}",
                total, converted, invalid, sentKafka, writtenFile, profile);
    }

    private static KafkaProducer<String, String> createProducer(String bootstrapServers) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "1");
        props.put("retries", "3");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(props);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> map = new HashMap<>();
        if (args == null) {
            return map;
        }
        for (int i = 0; i < args.length; i++) {
            String token = args[i];
            if (!token.startsWith("--")) {
                continue;
            }
            String key = token.substring(2);
            if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                map.put(key, args[++i]);
            } else {
                map.put(key, "true");
            }
        }
        return map;
    }

    private static String required(Map<String, String> params, String key) {
        String value = params.get(key);
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException("Missing required parameter: --" + key);
        }
        return value;
    }

    private static long parseLong(String raw) {
        try {
            return Long.parseLong(raw);
        } catch (Exception e) {
            return -1L;
        }
    }
}
