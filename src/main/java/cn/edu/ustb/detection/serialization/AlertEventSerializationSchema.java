package cn.edu.ustb.detection.serialization;

import cn.edu.ustb.detection.model.AlertEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nullable;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 告警事件 Kafka 序列化器
 *
 * <p>
 * 将 AlertEvent 对象序列化为 JSON 格式并发送到 Kafka。 使用 userId 作为分区键，确保同一用户的告警发送到同一分区。
 */
public class AlertEventSerializationSchema implements KafkaRecordSerializationSchema<AlertEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AlertEventSerializationSchema.class);

    private final String topic;
    private transient ObjectMapper objectMapper;

    public AlertEventSerializationSchema(String topic) {
        if (topic == null || topic.isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        this.topic = topic;
    }

    private void initObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, true);
        }
    }

    @Nullable
    @Override
    public ProducerRecord<byte[], byte[]> serialize(AlertEvent alert, KafkaSinkContext context, Long timestamp) {
        if (alert == null) {
            LOG.warn("Received null alert event, skipping serialization");
            return null;
        }

        initObjectMapper();

        try {
            byte[] value = objectMapper.writeValueAsBytes(alert);
            byte[] key = alert.getUserId() != null
                    ? alert.getUserId().getBytes(StandardCharsets.UTF_8)
                    : alert.getAlertId().getBytes(StandardCharsets.UTF_8);

            if (LOG.isDebugEnabled()) {
                LOG.debug("Serialized alert event: alertId={}, ruleId={}, userId={}", alert.getAlertId(),
                        alert.getRuleId(), alert.getUserId());
            }

            return new ProducerRecord<>(topic, null, alert.getAlertTimestamp(), key, value);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize alert event: {}, error: {}", alert.getAlertId(), e.getMessage());
            return null;
        }
    }
}
