package cn.edu.ustb.detection.model;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

/**
 * 告警事件实体类
 * 
 * 表示检测到异常行为后生成的告警事件，包含触发规则、匹配事件、告警级别等信息。
 * 用于输出到下游系统（如 Kafka、数据库、告警平台等）。
 */
public class AlertEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    /**
     * 告警唯一标识
     */
    private String alertId;

    /**
     * 触发的规则ID
     */
    private String ruleId;

    /**
     * 规则类型
     */
    private RiskRule.RuleType ruleType;

    /**
     * 告警级别
     */
    private AlertLevel level;

    /**
     * 关联的用户ID
     */
    private String userId;

    /**
     * 关联的IP地址
     */
    private String ip;

    /**
     * 关联的设备ID
     */
    private String deviceId;

    /**
     * 触发告警的事件列表（匹配到的行为序列）
     */
    private List<UserBehavior> triggerEvents;

    /**
     * 匹配到的事件数量
     */
    private int matchCount;

    /**
     * 告警生成时间戳
     */
    private long alertTimestamp;

    /**
     * 第一个触发事件的时间戳
     */
    private long firstEventTimestamp;

    /**
     * 最后一个触发事件的时间戳
     */
    private long lastEventTimestamp;

    /**
     * 告警描述信息
     */
    private String message;

    /**
     * 扩展信息（JSON格式）
     */
    private String extra;

    public AlertEvent() {
        this.alertId = UUID.randomUUID().toString();
        this.alertTimestamp = System.currentTimeMillis();
        this.level = AlertLevel.MEDIUM;
    }

    public static AlertEvent fromRuleMatch(RiskRule rule, List<UserBehavior> matchedEvents) {
        if (rule == null || matchedEvents == null || matchedEvents.isEmpty()) {
            throw new IllegalArgumentException("Rule and matched events cannot be null or empty");
        }

        AlertEvent alert = new AlertEvent();
        alert.setRuleId(rule.getRuleId());
        alert.setRuleType(rule.getRuleType());
        alert.setMatchCount(matchedEvents.size());
        alert.setTriggerEvents(matchedEvents);

        UserBehavior firstEvent = matchedEvents.get(0);
        UserBehavior lastEvent = matchedEvents.get(matchedEvents.size() - 1);

        alert.setUserId(firstEvent.getUserId());
        alert.setIp(firstEvent.getIp());
        alert.setDeviceId(firstEvent.getDeviceId());
        alert.setFirstEventTimestamp(firstEvent.getTimestamp());
        alert.setLastEventTimestamp(lastEvent.getTimestamp());

        alert.setLevel(determineAlertLevel(rule, matchedEvents.size()));
        alert.setMessage(buildAlertMessage(rule, matchedEvents));

        return alert;
    }

    private static AlertLevel determineAlertLevel(RiskRule rule, int matchCount) {
        double ratio = (double) matchCount / rule.getThreshold();
        if (ratio >= 3.0) {
            return AlertLevel.CRITICAL;
        } else if (ratio >= 2.0) {
            return AlertLevel.HIGH;
        } else if (ratio >= 1.0) {
            return AlertLevel.MEDIUM;
        }
        return AlertLevel.LOW;
    }

    private static String buildAlertMessage(RiskRule rule, List<UserBehavior> events) {
        UserBehavior firstEvent = events.get(0);
        String timeRange = String.format("%s ~ %s",
                FORMATTER.format(Instant.ofEpochMilli(events.get(0).getTimestamp())),
                FORMATTER.format(Instant.ofEpochMilli(events.get(events.size() - 1).getTimestamp())));

        return String.format("[%s] 检测到异常行为：用户[%s] IP[%s] 在时间段[%s]内触发规则[%s]，匹配事件数：%d，阈值：%d",
                rule.getRuleType(),
                firstEvent.getUserId(),
                firstEvent.getIp(),
                timeRange,
                rule.getRuleName(),
                events.size(),
                rule.getThreshold());
    }

    public String getAlertId() {
        return alertId;
    }

    public void setAlertId(String alertId) {
        this.alertId = alertId;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public RiskRule.RuleType getRuleType() {
        return ruleType;
    }

    public void setRuleType(RiskRule.RuleType ruleType) {
        this.ruleType = ruleType;
    }

    public AlertLevel getLevel() {
        return level;
    }

    public void setLevel(AlertLevel level) {
        this.level = level;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public List<UserBehavior> getTriggerEvents() {
        return triggerEvents;
    }

    public void setTriggerEvents(List<UserBehavior> triggerEvents) {
        this.triggerEvents = triggerEvents;
    }

    public int getMatchCount() {
        return matchCount;
    }

    public void setMatchCount(int matchCount) {
        this.matchCount = matchCount;
    }

    public long getAlertTimestamp() {
        return alertTimestamp;
    }

    public void setAlertTimestamp(long alertTimestamp) {
        this.alertTimestamp = alertTimestamp;
    }

    public long getFirstEventTimestamp() {
        return firstEventTimestamp;
    }

    public void setFirstEventTimestamp(long firstEventTimestamp) {
        this.firstEventTimestamp = firstEventTimestamp;
    }

    public long getLastEventTimestamp() {
        return lastEventTimestamp;
    }

    public void setLastEventTimestamp(long lastEventTimestamp) {
        this.lastEventTimestamp = lastEventTimestamp;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getExtra() {
        return extra;
    }

    public void setExtra(String extra) {
        this.extra = extra;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AlertEvent that = (AlertEvent) o;
        return Objects.equals(alertId, that.alertId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(alertId);
    }

    @Override
    public String toString() {
        return "AlertEvent{" +
                "alertId='" + alertId + '\'' +
                ", ruleId='" + ruleId + '\'' +
                ", ruleType=" + ruleType +
                ", level=" + level +
                ", userId='" + userId + '\'' +
                ", ip='" + ip + '\'' +
                ", matchCount=" + matchCount +
                ", alertTimestamp=" + FORMATTER.format(Instant.ofEpochMilli(alertTimestamp)) +
                ", message='" + message + '\'' +
                '}';
    }

    /**
     * 告警级别枚举
     */
    public enum AlertLevel {
        LOW(1, "低"),
        MEDIUM(2, "中"),
        HIGH(3, "高"),
        CRITICAL(4, "严重");

        private final int value;
        private final String description;

        AlertLevel(int value, String description) {
            this.value = value;
            this.description = description;
        }

        public int getValue() {
            return value;
        }

        public String getDescription() {
            return description;
        }
    }
}
