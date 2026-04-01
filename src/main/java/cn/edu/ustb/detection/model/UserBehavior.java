package cn.edu.ustb.detection.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonSetter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.io.Serializable;
import java.util.Objects;

/**
 * 用户行为事件实体类
 *
 * <p>
 * 表示从 Kafka 接收的用户行为日志，包含用户ID、行为类型、IP地址、时间戳等信息。 用于 Flink DataStream 处理和 CEP
 * 模式匹配。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class UserBehavior implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final long IEEE_REFERENCE_EPOCH_MS = 1512086400000L;
    private static final DateTimeFormatter[] TIME_FORMATS = new DateTimeFormatter[]{
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"), DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS")};

    private String userId;
    private String actionType;
    private String ip;
    private long timestamp;
    private String sessionId;
    private String deviceId;
    private String productId;
    private Double amount;
    private String extra;

    public UserBehavior() {
    }

    public UserBehavior(String userId, String actionType, String ip, long timestamp) {
        this.userId = userId;
        this.actionType = actionType;
        this.ip = ip;
        this.timestamp = timestamp;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getActionType() {
        return actionType;
    }

    public void setActionType(String actionType) {
        this.actionType = normalizeActionType(actionType);
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public Double getAmount() {
        return amount;
    }

    public void setAmount(Double amount) {
        this.amount = amount;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
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

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        UserBehavior that = (UserBehavior) o;
        return timestamp == that.timestamp && Objects.equals(userId, that.userId)
                && Objects.equals(actionType, that.actionType) && Objects.equals(ip, that.ip)
                && Objects.equals(sessionId, that.sessionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, actionType, ip, timestamp, sessionId);
    }

    @Override
    public String toString() {
        return "UserBehavior{" + "userId='" + userId + '\'' + ", actionType='" + actionType + '\'' + ", ip='" + ip
                + '\'' + ", timestamp=" + timestamp + ", sessionId='" + sessionId + '\'' + ", deviceId='" + deviceId
                + '\'' + ", productId='" + productId + '\'' + ", amount=" + amount + '}';
    }

    public static class Builder {
        private final UserBehavior behavior = new UserBehavior();

        public Builder userId(String userId) {
            behavior.setUserId(userId);
            return this;
        }

        public Builder actionType(String actionType) {
            behavior.setActionType(actionType);
            return this;
        }

        public Builder ip(String ip) {
            behavior.setIp(ip);
            return this;
        }

        public Builder timestamp(long timestamp) {
            behavior.setTimestamp(timestamp);
            return this;
        }

        public Builder sessionId(String sessionId) {
            behavior.setSessionId(sessionId);
            return this;
        }

        public Builder deviceId(String deviceId) {
            behavior.setDeviceId(deviceId);
            return this;
        }

        public Builder productId(String productId) {
            behavior.setProductId(productId);
            return this;
        }

        public Builder amount(Double amount) {
            behavior.setAmount(amount);
            return this;
        }

        public Builder extra(String extra) {
            behavior.setExtra(extra);
            return this;
        }

        public UserBehavior build() {
            return behavior;
        }
    }
}
