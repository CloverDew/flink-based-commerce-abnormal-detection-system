package cn.edu.ustb.detection;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import cn.edu.ustb.detection.model.AlertEvent;
import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import cn.edu.ustb.detection.serialization.AlertEventSerializationSchema;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import lombok.extern.slf4j.Slf4j;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
@EnabledIfSystemProperty(named = "it.testcontainers", matches = "true")
@Slf4j
public class KafkaE2ETestcontainersTest {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final String BEHAVIOR_TOPIC = "tc-user-behavior";
    private static final String RULE_TOPIC = "tc-risk-rules";
    private static final String ALERT_TOPIC = "tc-alerts";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Test
    @DisplayName("E2E with Kafka Testcontainers should produce alerts")
    void testKafkaE2EAlertFlow() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        createTopics(bootstrap, BEHAVIOR_TOPIC, RULE_TOPIC, ALERT_TOPIC);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<AlertEvent> alertStream = AbnormalBehaviorDetectionJob.buildPipeline(env, bootstrap, BEHAVIOR_TOPIC,
                RULE_TOPIC, "tc-group", false, false);
        alertStream.sinkTo(buildAlertKafkaSink(bootstrap, ALERT_TOPIC)).name("tc-alert-kafka-sink");

        JobClient jobClient = env.executeAsync("tc-e2e-kafka-alert-flow");
        Thread.sleep(3000L);

        publishRulesAndEvents(bootstrap);
        List<String> alerts = consumeAlerts(bootstrap, ALERT_TOPIC, 30);

        assertFalse(alerts.isEmpty(), "Expected at least one alert event in Kafka topic");
        assertTrue(alerts.stream().anyMatch(v -> v.contains("tc-rule-credential-stuffing")),
                "Expected alert to include rule id");

        log.info("Captured alerts: {}", alerts.size());
        log.info("Alert summaries: {}", summarizeAlerts(alerts));
        log.info("Rule aggregate table:\n{}", buildRuleAggregateTable(alerts));
        jobClient.cancel().get(10, TimeUnit.SECONDS);
    }

    private static KafkaSink<AlertEvent> buildAlertKafkaSink(String bootstrap, String topic) {
        return KafkaSink.<AlertEvent>builder().setBootstrapServers(bootstrap)
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setRecordSerializer(new AlertEventSerializationSchema(topic)).build();
    }

    private static void createTopics(String bootstrap, String... topics) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(props)) {
            List<NewTopic> newTopics = new ArrayList<>();
            for (String topic : topics) {
                newTopics.add(new NewTopic(topic, 1, (short) 1));
            }
            admin.createTopics(newTopics).all().get(15, TimeUnit.SECONDS);
        }
    }

    private static void publishRulesAndEvents(String bootstrap) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            RiskRule rule = RiskRule.builder().ruleId("tc-rule-credential-stuffing").ruleName("tc-credential-stuffing")
                    .ruleType(RiskRule.RuleType.CREDENTIAL_STUFFING).targetActionType("LOGIN_FAIL")
                    .windowSizeMs(60_000L).threshold(3).groupKeyType(RiskRule.GroupKeyType.BY_IP).version(1).build();

            producer.send(new ProducerRecord<>(RULE_TOPIC, rule.getRuleId(), MAPPER.writeValueAsString(rule))).get();

            long base = System.currentTimeMillis();
            UserBehavior e1 = UserBehavior.builder().userId("u1").actionType("LOGIN_FAIL").ip("10.0.0.1")
                    .timestamp(base).sessionId(UUID.randomUUID().toString()).build();
            UserBehavior e2 = UserBehavior.builder().userId("u2").actionType("LOGIN_FAIL").ip("10.0.0.1")
                    .timestamp(base + 5_000).sessionId(UUID.randomUUID().toString()).build();
            UserBehavior e3 = UserBehavior.builder().userId("u3").actionType("LOGIN_FAIL").ip("10.0.0.1")
                    .timestamp(base + 10_000).sessionId(UUID.randomUUID().toString()).build();
            UserBehavior e4 = UserBehavior.builder().userId("u4").actionType("LOGIN_FAIL").ip("10.0.0.1")
                    .timestamp(base + 12_000).sessionId(UUID.randomUUID().toString()).build();

            for (UserBehavior event : new UserBehavior[]{e1, e2, e3, e4}) {
                producer.send(new ProducerRecord<>(BEHAVIOR_TOPIC, event.getUserId(), MAPPER.writeValueAsString(event)))
                        .get();
            }
            producer.flush();
        }
    }

    private static List<String> consumeAlerts(String bootstrap, String topic, int timeoutSeconds) {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("group.id", "tc-consumer-" + UUID.randomUUID());
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        List<String> values = new ArrayList<>();
        long deadline = System.currentTimeMillis() + timeoutSeconds * 1000L;
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(java.util.Collections.singletonList(topic));
            while (System.currentTimeMillis() < deadline && values.isEmpty()) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    values.add(record.value());
                }
            }
        }
        return values;
    }

    private static List<String> summarizeAlerts(List<String> rawAlerts) {
        List<String> summaries = new ArrayList<>();
        for (String raw : rawAlerts) {
            try {
                JsonNode node = MAPPER.readTree(raw);
                String ruleId = node.path("ruleId").asText("unknown-rule");
                String userId = node.path("userId").asText("unknown-user");
                String ip = node.path("ip").asText("unknown-ip");
                int matchCount = node.path("matchCount").asInt(-1);
                String message = node.path("message").asText("");
                summaries.add(String.format("rule=%s user=%s ip=%s matchCount=%d msg=%s", ruleId, userId, ip,
                        matchCount, message));
            } catch (Exception e) {
                summaries.add("parse-failed: " + raw);
            }
        }
        return summaries;
    }

    private static String buildRuleAggregateTable(List<String> rawAlerts) {
        Map<String, Integer> ruleCount = new LinkedHashMap<>();
        Map<String, Integer> ruleMatchTotal = new LinkedHashMap<>();

        for (String raw : rawAlerts) {
            try {
                JsonNode node = MAPPER.readTree(raw);
                String ruleId = node.path("ruleId").asText("unknown-rule");
                int matchCount = node.path("matchCount").asInt(0);
                ruleCount.put(ruleId, ruleCount.getOrDefault(ruleId, 0) + 1);
                ruleMatchTotal.put(ruleId, ruleMatchTotal.getOrDefault(ruleId, 0) + matchCount);
            } catch (Exception ignore) {
                ruleCount.put("parse-failed", ruleCount.getOrDefault("parse-failed", 0) + 1);
            }
        }

        StringBuilder table = new StringBuilder();
        table.append(String.format("%-34s | %-12s | %-16s%n", "ruleId", "alertCount", "totalMatchCount"));
        table.append("--------------------------------------------------------------------------\n");
        for (Map.Entry<String, Integer> entry : ruleCount.entrySet()) {
            String ruleId = entry.getKey();
            int alertCount = entry.getValue();
            int totalMatchCount = ruleMatchTotal.getOrDefault(ruleId, 0);
            table.append(String.format("%-34s | %-12d | %-16d%n", ruleId, alertCount, totalMatchCount));
        }
        return table.toString();
    }
}
