package cn.edu.ustb.detection.serialization;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import lombok.extern.slf4j.Slf4j;

/**
 * 通用 JSON 反序列化器
 *
 * <p>
 * 用于将 Kafka 中的 JSON 字节数组反序列化为指定的 Java 对象。 支持容错处理，对于无法解析的消息返回 null 并记录日志。
 *
 * @param <T>
 *            目标类型
 */
@Slf4j
public class JsonDeserializationSchema<T> implements DeserializationSchema<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> targetClass;
    private transient ObjectMapper objectMapper;

    public JsonDeserializationSchema(Class<T> targetClass) {
        if (targetClass == null) {
            throw new IllegalArgumentException("Target class cannot be null");
        }
        this.targetClass = targetClass;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        initObjectMapper();
    }

    private void initObjectMapper() {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            objectMapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
        }
    }

    @Override
    public T deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            log.warn("Received null or empty message, skipping");
            return null;
        }

        initObjectMapper();

        try {
            T result = objectMapper.readValue(message, targetClass);
            if (log.isDebugEnabled()) {
                log.debug("Successfully deserialized message to {}: {}", targetClass.getSimpleName(), result);
            }
            return result;
        } catch (Exception e) {
            String rawMessage = new String(message, StandardCharsets.UTF_8);
            log.error("Failed to deserialize message to {}: [{}], error: {}", targetClass.getSimpleName(),
                    truncateMessage(rawMessage, 200), e.getMessage());
            return null;
        }
    }

    @Override
    public boolean isEndOfStream(T nextElement) {
        return false;
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(targetClass);
    }

    private String truncateMessage(String message, int maxLength) {
        if (message == null) {
            return "null";
        }
        if (message.length() <= maxLength) {
            return message;
        }
        return message.substring(0, maxLength) + "...(truncated)";
    }
}
