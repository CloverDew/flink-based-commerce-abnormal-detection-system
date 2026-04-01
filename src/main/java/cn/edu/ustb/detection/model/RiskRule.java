package cn.edu.ustb.detection.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * 风控规则实体类
 *
 * <p>
 * 用于动态下发的风控规则配置，支持通过 Broadcast State 热加载。 规则可以定义异常行为的匹配条件，如时间窗口、触发次数、行为类型等。
 */
public class RiskRule implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 规则唯一标识 */
    private String ruleId;

    /** 规则名称 */
    private String ruleName;

    /** 规则类型：CREDENTIAL_STUFFING(撞库)、ORDER_BRUSH(刷单)、ABNORMAL_LOGIN(异常登录)等 */
    private RuleType ruleType;

    /** 规则状态：ENABLED(启用)、DISABLED(禁用) */
    private RuleStatus status;

    /** 目标行为类型（如 LOGIN_FAIL, ORDER, PAYMENT 等） */
    private String targetActionType;

    /** 时间窗口大小（毫秒） */
    private long windowSizeMs;

    /** 触发阈值（在窗口内达到此次数则触发告警） */
    private int threshold;

    /** 分组键类型：BY_USER_ID, BY_IP, BY_DEVICE_ID */
    private GroupKeyType groupKeyType;

    /** 规则优先级（数值越小优先级越高） */
    private int priority;

    /** 规则描述 */
    private String description;

    /** 规则版本号（用于版本控制） */
    private long version;

    /** 规则创建/更新时间戳 */
    private long updateTimestamp;

    public RiskRule() {
        this.status = RuleStatus.ENABLED;
        this.priority = 100;
        this.groupKeyType = GroupKeyType.BY_USER_ID;
    }

    public String getRuleId() {
        return ruleId;
    }

    public void setRuleId(String ruleId) {
        this.ruleId = ruleId;
    }

    public String getRuleName() {
        return ruleName;
    }

    public void setRuleName(String ruleName) {
        this.ruleName = ruleName;
    }

    public RuleType getRuleType() {
        return ruleType;
    }

    public void setRuleType(RuleType ruleType) {
        this.ruleType = ruleType;
    }

    public RuleStatus getStatus() {
        return status;
    }

    public void setStatus(RuleStatus status) {
        this.status = status;
    }

    public String getTargetActionType() {
        return targetActionType;
    }

    public void setTargetActionType(String targetActionType) {
        this.targetActionType = targetActionType;
    }

    public long getWindowSizeMs() {
        return windowSizeMs;
    }

    public void setWindowSizeMs(long windowSizeMs) {
        this.windowSizeMs = windowSizeMs;
    }

    public int getThreshold() {
        return threshold;
    }

    public void setThreshold(int threshold) {
        this.threshold = threshold;
    }

    public GroupKeyType getGroupKeyType() {
        return groupKeyType;
    }

    public void setGroupKeyType(GroupKeyType groupKeyType) {
        this.groupKeyType = groupKeyType;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public long getVersion() {
        return version;
    }

    public void setVersion(long version) {
        this.version = version;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    /** 校验规则是否有效 */
    public boolean isValid() {
        return ruleId != null && !ruleId.isEmpty() && ruleType != null && targetActionType != null
                && !targetActionType.isEmpty() && windowSizeMs > 0 && threshold > 0;
    }

    /** 判断规则是否启用 */
    public boolean isEnabled() {
        return RuleStatus.ENABLED.equals(status);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        RiskRule riskRule = (RiskRule) o;
        return Objects.equals(ruleId, riskRule.ruleId) && version == riskRule.version;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ruleId, version);
    }

    @Override
    public String toString() {
        return "RiskRule{" + "ruleId='" + ruleId + '\'' + ", ruleName='" + ruleName + '\'' + ", ruleType=" + ruleType
                + ", status=" + status + ", targetActionType='" + targetActionType + '\'' + ", windowSizeMs="
                + windowSizeMs + ", threshold=" + threshold + ", groupKeyType=" + groupKeyType + ", priority="
                + priority + ", version=" + version + '}';
    }

    /** 规则类型枚举 */
    public enum RuleType {
        CREDENTIAL_STUFFING, // 撞库攻击
        ORDER_BRUSH, // 刷单
        ABNORMAL_LOGIN, // 异常登录
        HIGH_FREQ_ACCESS, // 高频访问
        PAYMENT_FRAUD, // 支付欺诈
        CUSTOM // 自定义规则
    }

    /** 规则状态枚举 */
    public enum RuleStatus {
        ENABLED, // 启用
        DISABLED // 禁用
    }

    /** 分组键类型枚举 */
    public enum GroupKeyType {
        BY_USER_ID, // 按用户ID分组
        BY_IP, // 按IP地址分组
        BY_DEVICE_ID, // 按设备ID分组
        BY_SESSION_ID // 按会话ID分组
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final RiskRule rule = new RiskRule();

        public Builder ruleId(String ruleId) {
            rule.setRuleId(ruleId);
            return this;
        }

        public Builder ruleName(String ruleName) {
            rule.setRuleName(ruleName);
            return this;
        }

        public Builder ruleType(RuleType ruleType) {
            rule.setRuleType(ruleType);
            return this;
        }

        public Builder status(RuleStatus status) {
            rule.setStatus(status);
            return this;
        }

        public Builder targetActionType(String targetActionType) {
            rule.setTargetActionType(targetActionType);
            return this;
        }

        public Builder windowSizeMs(long windowSizeMs) {
            rule.setWindowSizeMs(windowSizeMs);
            return this;
        }

        public Builder threshold(int threshold) {
            rule.setThreshold(threshold);
            return this;
        }

        public Builder groupKeyType(GroupKeyType groupKeyType) {
            rule.setGroupKeyType(groupKeyType);
            return this;
        }

        public Builder priority(int priority) {
            rule.setPriority(priority);
            return this;
        }

        public Builder description(String description) {
            rule.setDescription(description);
            return this;
        }

        public Builder version(long version) {
            rule.setVersion(version);
            return this;
        }

        public Builder updateTimestamp(long updateTimestamp) {
            rule.setUpdateTimestamp(updateTimestamp);
            return this;
        }

        public RiskRule build() {
            return rule;
        }
    }
}
