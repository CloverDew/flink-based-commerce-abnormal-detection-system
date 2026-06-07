package cn.edu.ustb.detection.cep;

import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import lombok.extern.slf4j.Slf4j;

/**
 * CEP 模式工厂
 *
 * <p>
 * 根据风控规则动态生成 Flink CEP Pattern。 支持常见的异常模式：撞库攻击、刷单、异常登录等。
 */
@Slf4j
public class CepPatternFactory {

    private CepPatternFactory() {
    }

    public static boolean isLoginFail(String actionType) {
        return equalsIgnoreCase(actionType, "LOGIN_FAIL");
    }

    public static boolean isLoginSuccess(String actionType) {
        return equalsIgnoreCase(actionType, "LOGIN_SUCCESS") || equalsIgnoreCase(actionType, "LOGIN");
    }

    public static boolean isSensitiveAction(String actionType) {
        return equalsIgnoreCase(actionType, "CHANGE_PASSWORD") || equalsIgnoreCase(actionType, "BIND_PHONE")
                || equalsIgnoreCase(actionType, "WITHDRAW") || equalsIgnoreCase(actionType, "LARGE_TRANSFER");
    }

    public static boolean isPayment(String actionType) {
        return equalsIgnoreCase(actionType, "PAYMENT");
    }

    public static boolean isPaymentFraud(String actionType) {
        return equalsIgnoreCase(actionType, "PAYMENT_FRAUD");
    }

    private static boolean equalsIgnoreCase(String a, String b) {
        if (a == null || b == null) {
            return false;
        }
        return a.equalsIgnoreCase(b);
    }

    /** 根据规则类型创建对应的 CEP Pattern */
    public static Pattern<UserBehavior, ?> createPattern(RiskRule rule) {
        if (rule == null || !rule.isValid()) {
            throw new IllegalArgumentException("Invalid rule: " + rule);
        }

        switch (rule.getRuleType()) {
            case CREDENTIAL_STUFFING :
                return createCredentialStuffingPattern(rule);
            case ORDER_BRUSH :
                return createOrderBrushPattern(rule);
            case ABNORMAL_LOGIN :
                return createAbnormalLoginPattern(rule);
            case HIGH_FREQ_ACCESS :
                return createHighFrequencyPattern(rule);
            case PAYMENT_FRAUD :
                return createPaymentFraudPattern(rule);
            default :
                return createGenericPattern(rule);
        }
    }

    /** 撞库攻击检测模式 同一 IP 在时间窗口内连续多次登录失败 */
    public static Pattern<UserBehavior, ?> createCredentialStuffingPattern(RiskRule rule) {
        log.info("Creating credential stuffing pattern: windowSize={}ms, threshold={}", rule.getWindowSizeMs(),
                rule.getThreshold());

        return Pattern.<UserBehavior>begin("first").where(new SimpleCondition<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior event) {
                return event != null && isLoginFail(event.getActionType());
            }
        }).timesOrMore(rule.getThreshold()).greedy().within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /** 刷单检测模式 同一用户在时间窗口内高频下单 */
    public static Pattern<UserBehavior, ?> createOrderBrushPattern(RiskRule rule) {
        log.info("Creating order brush pattern: windowSize={}ms, threshold={}", rule.getWindowSizeMs(),
                rule.getThreshold());

        return Pattern.<UserBehavior>begin("first").where(new SimpleCondition<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior event) {
                if (event == null) {
                    return false;
                }
                String a = event.getActionType();
                return equalsIgnoreCase(a, "ORDER") || equalsIgnoreCase(a, "PLACE_ORDER");
            }
        }).timesOrMore(rule.getThreshold()).greedy().within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /** 异常登录检测模式 登录后短时间内发生敏感操作 */
    public static Pattern<UserBehavior, ?> createAbnormalLoginPattern(RiskRule rule) {
        log.info("Creating abnormal login pattern: windowSize={}ms, threshold={}", rule.getWindowSizeMs(),
                rule.getThreshold());

        return Pattern.<UserBehavior>begin("login").where(new SimpleCondition<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior event) {
                return event != null && isLoginSuccess(event.getActionType());
            }
        }).followedBy("sensitiveAction").where(new SimpleCondition<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior event) {
                return event != null && isSensitiveAction(event.getActionType());
            }
        }).within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /** 高频访问检测模式 通用高频行为检测 */
    public static Pattern<UserBehavior, ?> createHighFrequencyPattern(RiskRule rule) {
        log.info("Creating high frequency pattern: targetAction={}, windowSize={}ms, threshold={}",
                rule.getTargetActionType(), rule.getWindowSizeMs(), rule.getThreshold());

        final String targetAction = rule.getTargetActionType();

        return Pattern.<UserBehavior>begin("first").where(new SimpleCondition<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior event) {
                return event != null && targetAction != null && targetAction.equalsIgnoreCase(event.getActionType());
            }
        }).timesOrMore(rule.getThreshold()).greedy().within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /** 通用模式（用于自定义规则） */
    public static Pattern<UserBehavior, ?> createGenericPattern(RiskRule rule) {
        log.info("Creating generic pattern: targetAction={}, windowSize={}ms, threshold={}", rule.getTargetActionType(),
                rule.getWindowSizeMs(), rule.getThreshold());

        final String targetAction = rule.getTargetActionType();

        return Pattern.<UserBehavior>begin("events").where(new SimpleCondition<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior event) {
                if (targetAction == null || targetAction.isEmpty()) {
                    return true;
                }
                return event != null && targetAction.equalsIgnoreCase(event.getActionType());
            }
        }).timesOrMore(rule.getThreshold()).greedy().within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /** 创建支付欺诈检测模式 短时间内多次小额支付后进行大额支付 */
    public static Pattern<UserBehavior, ?> createPaymentFraudPattern(RiskRule rule) {
        log.info("Creating payment fraud pattern: windowSize={}ms", rule.getWindowSizeMs());

        return Pattern.<UserBehavior>begin("smallPayments").where(new SimpleCondition<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior event) {
                return event != null && isPayment(event.getActionType()) && event.getAmount() != null
                        && event.getAmount() < 100.0;
            }
        }).timesOrMore(3).greedy().followedBy("largePayment").where(new SimpleCondition<UserBehavior>() {
            @Override
            public boolean filter(UserBehavior event) {
                return event != null && isPayment(event.getActionType()) && event.getAmount() != null
                        && event.getAmount() >= 1000.0;
            }
        }).within(Time.milliseconds(rule.getWindowSizeMs()));
    }
}
