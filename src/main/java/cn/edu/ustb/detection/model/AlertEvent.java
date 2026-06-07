package cn.edu.ustb.detection.model;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.UUID;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 告警事件实体类
 *
 * <p>
 * 表示检测到异常行为后生成的告警事件，包含触发规则、匹配事件、告警级别等信息。 用于输出到下游系统（如 Kafka、数据库、告警平台等）。
 */
@Getter
@Setter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString
public class AlertEvent implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    /** 告警唯一标识 */
    @EqualsAndHashCode.Include
    private String alertId = UUID.randomUUID().toString();

    /** 触发的规则ID */
    private String ruleId;

    /** 规则类型 */
    private RiskRule.RuleType ruleType;

    /** 告警级别 */
    private AlertLevel level = AlertLevel.MEDIUM;

    /** 关联的用户ID */
    private String userId;

    /** 关联的IP地址 */
    private String ip;

    /** 关联的设备ID */
    private String deviceId;

    /** 触发告警的事件列表（匹配到的行为序列） */
    private List<UserBehavior> triggerEvents;

    /** 匹配到的事件数量 */
    private int matchCount;

    /** 风险评分（用于定量评估与下发决策） */
    private double riskScore = 0.0;

    /** 告警生成时间戳 */
    private long alertTimestamp = System.currentTimeMillis();

    /** 第一个触发事件的时间戳 */
    private long firstEventTimestamp;

    /** 最后一个触发事件的时间戳 */
    private long lastEventTimestamp;

    /**
     * 处理滞后（毫秒）：告警发出时的墙钟时间与末条触发事件的事件时间之差（下限为 0）。
     *
     * <p>用于离线评估；避免将 {@link #alertTimestamp}（墙钟）与 {@link #lastEventTimestamp}（事件时间）直接相减产生误导性的负数或积压假象。
     */
    private long processingLagMs;

    /** 告警描述信息 */
    private String message;

    /** 扩展信息（JSON格式） */
    private String extra;

    public AlertEvent() {
    }

    public static AlertEvent fromRuleMatch(RiskRule rule, List<UserBehavior> matchedEvents) {
        if (rule == null || matchedEvents == null || matchedEvents.isEmpty()) {
            throw new IllegalArgumentException("Rule and matched events cannot be null or empty");
        }
        List<UserBehavior> sortedEvents = new ArrayList<>(matchedEvents);
        sortedEvents.sort(Comparator.comparingLong(UserBehavior::getTimestamp));

        AlertEvent alert = new AlertEvent();
        alert.setRuleId(rule.getRuleId());
        alert.setRuleType(rule.getRuleType());
        alert.setMatchCount(sortedEvents.size());
        alert.setTriggerEvents(sortedEvents);

        UserBehavior firstEvent = sortedEvents.get(0);
        UserBehavior lastEvent = sortedEvents.get(sortedEvents.size() - 1);

        alert.setUserId(firstEvent.getUserId());
        alert.setIp(firstEvent.getIp());
        alert.setDeviceId(firstEvent.getDeviceId());
        alert.setFirstEventTimestamp(firstEvent.getTimestamp());
        alert.setLastEventTimestamp(lastEvent.getTimestamp());

        alert.setLevel(determineAlertLevel(rule, sortedEvents.size()));
        alert.setMessage(buildAlertMessage(rule, sortedEvents));

        long emittedAt = System.currentTimeMillis();
        alert.setAlertTimestamp(emittedAt);
        alert.setProcessingLagMs(Math.max(0L, emittedAt - lastEvent.getTimestamp()));

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

        return String.format("[%s] 检测到异常行为：用户[%s] IP[%s] 在时间段[%s]内触发规则[%s]，匹配事件数：%d，阈值：%d", rule.getRuleType(),
                firstEvent.getUserId(), firstEvent.getIp(), timeRange, rule.getRuleName(), events.size(),
                rule.getThreshold());
    }

    /** 告警级别枚举 */
    @Getter
    @RequiredArgsConstructor
    public enum AlertLevel {
        LOW(1, "低"), MEDIUM(2, "中"), HIGH(3, "高"), CRITICAL(4, "严重");

        private final int value;
        private final String description;
    }
}
