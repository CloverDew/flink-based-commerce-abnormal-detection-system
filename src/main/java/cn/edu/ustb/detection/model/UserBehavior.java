package cn.edu.ustb.detection.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 用户行为事件实体类
 *
 * <p>
 * 表示从 Kafka 接收的用户行为日志，包含用户ID、行为类型、IP地址、时间戳等信息。 用于 Flink DataStream 处理和 CEP
 * 模式匹配。
 */
public class UserBehavior implements Serializable {

    private static final long serialVersionUID = 1L;

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
        this.actionType = actionType;
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

    /** 校验事件数据是否有效 */
    public boolean isValid() {
        return userId != null && !userId.isEmpty() && actionType != null && !actionType.isEmpty() && timestamp > 0;
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
