package cn.edu.ustb.detection.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 用户行为事件实体类
 *
 * <p>
 * 表示从 Kafka 接收的用户行为日志，包含用户ID、行为类型、IP地址、时间戳等信息。 用于 Flink DataStream 处理和 CEP
 * 模式匹配。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor(access = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class UserBehavior implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final long IEEE_REFERENCE_EPOCH_MS = 1512086400000L;
    private static final DateTimeFormatter[] TIME_FORMATS = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")};

    @EqualsAndHashCode.Include
    private String userId;

    @EqualsAndHashCode.Include
    @Setter(AccessLevel.NONE)
    private String actionType;

    @EqualsAndHashCode.Include
    private String ip;

    @EqualsAndHashCode.Include
    private long timestamp;

    @EqualsAndHashCode.Include
    private String sessionId;

    private String deviceId;
    private String productId;
    private Double amount;
    private String extra;

    public UserBehavior(String userId, String actionType, String ip, long timestamp) {
        this.userId = userId;
        setActionType(actionType);
        this.ip = ip;
        this.timestamp = timestamp;
    }

    public void setActionType(String actionType) {
        this.actionType = normalizeActionType(actionType);
    }

    @JsonSetter("user_id")
    public void setUserIdAlias(Object userId) {
        this.userId = userId == null ? null : String.valueOf(userId);
    }

    @JsonSetter("event_type")
    public void setEventTypeAlias(String eventType) {
        setActionType(eventType);
    }

    @JsonSetter("session_id")
    public void setSessionIdAlias(String sessionId) {
        this.sessionId = sessionId;
    }

    @JsonSetter("user_session")
    public void setUserSessionAlias(String sessionId) {
        this.sessionId = sessionId;
    }

    @JsonSetter("product_id")
    public void setProductIdAlias(Object productId) {
        this.productId = productId == null ? null : String.valueOf(productId);
    }

    @JsonSetter("ip_address")
    public void setIpAddressAlias(String ipAddress) {
        this.ip = ipAddress;
    }

    @JsonSetter("event_time")
    public void setEventTimeAlias(Object eventTime) {
        long parsed = parseToEpochMs(eventTime);
        if (parsed > 0) {
            this.timestamp = parsed;
        }
    }

    @JsonSetter("TransactionDT")
    public void setTransactionDtAlias(Object transactionDt) {
        long deltaSeconds = parseNumberValue(transactionDt);
        if (deltaSeconds > 0) {
            this.timestamp = IEEE_REFERENCE_EPOCH_MS + (deltaSeconds * 1000L);
        }
    }

    /** 校验事件数据是否有效 */
    public boolean isValid() {
        return userId != null && !userId.isEmpty() && actionType != null && !actionType.isEmpty() && timestamp > 0;
    }

    private static String normalizeActionType(String actionType) {
        if (actionType == null) {
            return null;
        }
        String normalized = actionType.trim().toUpperCase();
        switch (normalized) {
            case "REMOVE_FROM_CART" :
                return "REMOVE_CART";
            case "ADD_TO_CART" :
                return "ADD_CART";
            default :
                return normalized;
        }
    }

    private static long parseToEpochMs(Object value) {
        if (value == null) {
            return -1L;
        }
        if (value instanceof Number) {
            long raw = ((Number) value).longValue();
            return normalizeUnixTime(raw);
        }
        String text = String.valueOf(value).trim();
        if (text.isEmpty()) {
            return -1L;
        }
        try {
            return normalizeUnixTime(Long.parseLong(text));
        } catch (NumberFormatException ignore) {
        }
        try {
            return Instant.parse(text).toEpochMilli();
        } catch (DateTimeParseException ignore) {
        }
        for (DateTimeFormatter formatter : TIME_FORMATS) {
            try {
                return LocalDateTime.parse(text, formatter).toInstant(ZoneOffset.UTC).toEpochMilli();
            } catch (DateTimeParseException ignore) {
            }
        }
        return -1L;
    }

    private static long parseNumberValue(Object value) {
        if (value == null) {
            return -1L;
        }
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        try {
            return Long.parseLong(String.valueOf(value).trim());
        } catch (Exception e) {
            return -1L;
        }
    }

    private static long normalizeUnixTime(long raw) {
        if (raw <= 0) {
            return -1L;
        }
        if (raw < 10_000_000_000L) {
            return raw * 1000L;
        }
        return raw;
    }

    /** Ensure {@link Builder} normalizes action types the same way as {@link #setActionType(String)}. */
    public static class UserBehaviorBuilder {
        public UserBehaviorBuilder actionType(String actionType) {
            this.actionType = normalizeActionType(actionType);
            return this;
        }
    }
}
