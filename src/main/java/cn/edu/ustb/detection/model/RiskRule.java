package cn.edu.ustb.detection.model;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 风控规则实体类
 *
 * <p>
 * 用于动态下发的风控规则配置，支持通过 Broadcast State 热加载。 规则可以定义异常行为的匹配条件，如时间窗口、触发次数、行为类型等。
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
public class RiskRule implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 规则唯一标识 */
    @EqualsAndHashCode.Include
    private String ruleId;

    /** 规则名称 */
    private String ruleName;

    /** 规则类型：CREDENTIAL_STUFFING(撞库)、ORDER_BRUSH(刷单)、ABNORMAL_LOGIN(异常登录)等 */
    private RuleType ruleType;

    /** 规则状态：ENABLED(启用)、DISABLED(禁用) */
    @Builder.Default
    private RuleStatus status = RuleStatus.ENABLED;

    /** 目标行为类型（如 LOGIN_FAIL, ORDER, PAYMENT 等） */
    private String targetActionType;

    /** 时间窗口大小（毫秒） */
    private long windowSizeMs;

    /** 触发阈值（在窗口内达到此次数则触发告警） */
    private int threshold;

    /** 分组键类型：BY_USER_ID, BY_IP, BY_DEVICE_ID */
    @Builder.Default
    private GroupKeyType groupKeyType = GroupKeyType.BY_USER_ID;

    /** 规则优先级（数值越小优先级越高） */
    @Builder.Default
    private int priority = 100;

    /**
     * 严重程度权重（用于风险评分）。
     *
     * <p>
     * 取值越大表示该规则命中后的风险更高。缺省为 1.0。
     */
    @Builder.Default
    private double severityWeight = 1.0;

    /**
     * 评分阈值（用于风险评分的拦截）。
     *
     * <p>
     * 当计算得到的风险评分 >= 该阈值时，才输出高风险告警。缺省为 1.0。
     */
    @Builder.Default
    private double scoreThreshold = 1.0;

    /** 规则描述 */
    private String description;

    /** 规则版本号（用于版本控制） */
    @EqualsAndHashCode.Include
    private long version;

    /** 规则创建/更新时间戳 */
    private long updateTimestamp;

    /** 校验规则是否有效 */
    public boolean isValid() {
        return ruleId != null && !ruleId.isEmpty() && ruleType != null && targetActionType != null
                && !targetActionType.isEmpty() && windowSizeMs > 0 && threshold > 0;
    }

    /** 判断规则是否启用 */
    public boolean isEnabled() {
        return RuleStatus.ENABLED.equals(status);
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
}
