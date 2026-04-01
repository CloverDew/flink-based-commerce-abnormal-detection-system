package cn.edu.ustb.detection.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Future;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Publish JSON or JSONL file contents to Kafka topic.
 */
public class JsonFileKafkaPublisher {

    private static final Logger LOG = LoggerFactory.getLogger(JsonFileKafkaPublisher.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String input = required(params, "input");
        String bootstrapServers = params.getOrDefault("kafka-bootstrap", "localhost:9092");
        String topic = required(params, "kafka-topic");
        String keyField = params.getOrDefault("key-field", "");

        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "1");
        props.put("retries", "3");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        ObjectMapper mapper = new ObjectMapper();
        long sent = 0L;

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            String firstNonBlank = firstNonBlankLine(input);
            if (firstNonBlank != null && firstNonBlank.startsWith("[")) {
                JsonNode root = mapper.readTree(Files.newBufferedReader(Paths.get(input), StandardCharsets.UTF_8));
                if (root != null && root.isArray()) {
                    for (JsonNode node : root) {
                        String json = mapper.writeValueAsString(node);
                        String key = extractKey(node, keyField);
                        Future<?> future = producer.send(new ProducerRecord<>(topic, key, json));
                        future.get();
                        sent++;
                    }
                }
            } else {
                try (BufferedReader reader = Files.newBufferedReader(Paths.get(input), StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String json = line.trim();
                        if (json.isEmpty()) {
                            continue;
                        }
                        String key = null;
                        if (!keyField.isEmpty()) {
                            JsonNode node = mapper.readTree(json);
                            key = extractKey(node, keyField);
                        }
                        Future<?> future = producer.send(new ProducerRecord<>(topic, key, json));
                        future.get();
                        sent++;
                    }
                }
            }
            producer.flush();
        }

        LOG.info("Published {} messages to topic={} from file={}", sent, topic, input);
    }

    private static String firstNonBlankLine(String file) throws Exception {
        try (BufferedReader reader = Files.newBufferedReader(Paths.get(file), StandardCharsets.UTF_8)) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.trim().isEmpty()) {
                    return line.trim();
                }
            }
        }
        return null;
    }

    private static String extractKey(JsonNode node, String keyField) {
        if (node == null || keyField == null || keyField.trim().isEmpty()) {
            return null;
        }
        JsonNode keyNode = node.get(keyField);
        return keyNode == null || keyNode.isNull() ? null : keyNode.asText();
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
}
