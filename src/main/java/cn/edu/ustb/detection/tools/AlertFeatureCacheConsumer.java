package cn.edu.ustb.detection.tools;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * Consume AlertEvent JSON from Kafka and cache extracted features in Redis.
 *
 * <p>
 * This is the "feature cache" part of the feedback layer. It stores the {@code extra} JSON (and optionally a few
 * hot fields) keyed by user/session for fast lookups by downstream applications.
 */
public class AlertFeatureCacheConsumer {

    private static final Logger LOG = LoggerFactory.getLogger(AlertFeatureCacheConsumer.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> params = parseArgs(args);
        String bootstrapServers = params.getOrDefault("kafka-bootstrap", "localhost:9092");
        String topic = params.getOrDefault("kafka-topic", "alerts");
        String groupId = params.getOrDefault("kafka-group", "feature-cache-consumer");

        String redisHost = params.getOrDefault("redis-host", "localhost");
        int redisPort = parseInt(params.getOrDefault("redis-port", "6379"), 6379);
        int ttlSeconds = parseInt(params.getOrDefault("ttl-seconds", "1800"), 1800);

        LOG.info("Starting feature cache consumer: kafka={}, topic={}, groupId={}, redis={}:{} ttl={}s",
                bootstrapServers, topic, groupId, redisHost, redisPort, ttlSeconds);

        ObjectMapper mapper = new ObjectMapper();
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(16);
        poolConfig.setMaxIdle(8);
        poolConfig.setMinIdle(0);
        JedisPool jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 5000);

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(java.util.Collections.singletonList(topic));

            long cached = 0;
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> r : records) {
                    String json = r.value();
                    if (json == null || json.trim().isEmpty()) {
                        continue;
                    }
                    try {
                        JsonNode root = mapper.readTree(json);
                        String userId = text(root, "userId");
                        String ruleType = text(root, "ruleType");
                        String ip = text(root, "ip");
                        String extra = text(root, "extra");

                        if ((userId == null || userId.isEmpty()) && (ip == null || ip.isEmpty())) {
                            continue;
                        }

                        Map<String, String> payload = new HashMap<>();
                        if (ruleType != null) {
                            payload.put("ruleType", ruleType);
                        }
                        if (ip != null) {
                            payload.put("ip", ip);
                        }
                        if (extra != null) {
                            payload.put("extra", extra);
                        }

                        try (Jedis jedis = jedisPool.getResource()) {
                            if (userId != null && !userId.isEmpty()) {
                                writeHash(jedis, "features:user:" + userId, payload, ttlSeconds);
                            }
                            if (ip != null && !ip.isEmpty()) {
                                writeHash(jedis, "features:ip:" + ip, payload, ttlSeconds);
                            }
                        }

                        cached++;
                        if (cached % 1000 == 0) {
                            LOG.info("Cached {} alerts into redis", cached);
                        }
                    } catch (Exception e) {
                        LOG.warn("Failed to cache alert record (skipping). err={}", e.getMessage());
                    }
                }
            }
        } finally {
            try {
                jedisPool.close();
            } catch (Exception ignore) {
            }
        }
    }

    private static void writeHash(Jedis jedis, String key, Map<String, String> map, int ttlSeconds) {
        if (map == null || map.isEmpty()) {
            return;
        }
        jedis.hset(key.getBytes(StandardCharsets.UTF_8), toByteMap(map));
        jedis.expire(key, ttlSeconds);
    }

    private static Map<byte[], byte[]> toByteMap(Map<String, String> map) {
        Map<byte[], byte[]> out = new HashMap<>();
        for (Map.Entry<String, String> e : map.entrySet()) {
            if (e.getKey() == null || e.getValue() == null) {
                continue;
            }
            out.put(e.getKey().getBytes(StandardCharsets.UTF_8), e.getValue().getBytes(StandardCharsets.UTF_8));
        }
        return out;
    }

    private static String text(JsonNode root, String field) {
        if (root == null || field == null) {
            return null;
        }
        JsonNode node = root.get(field);
        return node == null || node.isNull() ? null : node.asText();
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

    private static int parseInt(String raw, int defaultValue) {
        try {
            return Integer.parseInt(raw.trim());
        } catch (Exception e) {
            return defaultValue;
        }
    }
}

