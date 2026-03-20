package cn.edu.ustb.detection.cep;

import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.IterativeCondition;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CEP 模式工厂
 * 
 * 根据风控规则动态生成 Flink CEP Pattern。
 * 支持常见的异常模式：撞库攻击、刷单、异常登录等。
 */
public class CepPatternFactory {

    private static final Logger LOG = LoggerFactory.getLogger(CepPatternFactory.class);

    private CepPatternFactory() {
    }

    /**
     * 根据规则类型创建对应的 CEP Pattern
     */
    public static Pattern<UserBehavior, ?> createPattern(RiskRule rule) {
        if (rule == null || !rule.isValid()) {
            throw new IllegalArgumentException("Invalid rule: " + rule);
        }

        switch (rule.getRuleType()) {
            case CREDENTIAL_STUFFING:
                return createCredentialStuffingPattern(rule);
            case ORDER_BRUSH:
                return createOrderBrushPattern(rule);
            case ABNORMAL_LOGIN:
                return createAbnormalLoginPattern(rule);
            case HIGH_FREQ_ACCESS:
                return createHighFrequencyPattern(rule);
            default:
                return createGenericPattern(rule);
        }
    }

    /**
     * 撞库攻击检测模式
     * 同一 IP 在时间窗口内连续多次登录失败
     */
    public static Pattern<UserBehavior, ?> createCredentialStuffingPattern(RiskRule rule) {
        LOG.info("Creating credential stuffing pattern: windowSize={}ms, threshold={}",
                rule.getWindowSizeMs(), rule.getThreshold());

        return Pattern.<UserBehavior>begin("first")
                .where(new SimpleCondition<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior event) {
                        return "LOGIN_FAIL".equalsIgnoreCase(event.getActionType());
                    }
                })
                .timesOrMore(rule.getThreshold())
                .greedy()
                .within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /**
     * 刷单检测模式
     * 同一用户在时间窗口内高频下单
     */
    public static Pattern<UserBehavior, ?> createOrderBrushPattern(RiskRule rule) {
        LOG.info("Creating order brush pattern: windowSize={}ms, threshold={}",
                rule.getWindowSizeMs(), rule.getThreshold());

        return Pattern.<UserBehavior>begin("first")
                .where(new SimpleCondition<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior event) {
                        return "ORDER".equalsIgnoreCase(event.getActionType())
                                || "PLACE_ORDER".equalsIgnoreCase(event.getActionType());
                    }
                })
                .timesOrMore(rule.getThreshold())
                .greedy()
                .within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /**
     * 异常登录检测模式
     * 登录后短时间内发生敏感操作
     */
    public static Pattern<UserBehavior, ?> createAbnormalLoginPattern(RiskRule rule) {
        LOG.info("Creating abnormal login pattern: windowSize={}ms, threshold={}",
                rule.getWindowSizeMs(), rule.getThreshold());

        return Pattern.<UserBehavior>begin("login")
                .where(new SimpleCondition<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior event) {
                        return "LOGIN".equalsIgnoreCase(event.getActionType())
                                || "LOGIN_SUCCESS".equalsIgnoreCase(event.getActionType());
                    }
                })
                .followedBy("sensitiveAction")
                .where(new SimpleCondition<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior event) {
                        String action = event.getActionType();
                        return "CHANGE_PASSWORD".equalsIgnoreCase(action)
                                || "BIND_PHONE".equalsIgnoreCase(action)
                                || "WITHDRAW".equalsIgnoreCase(action)
                                || "LARGE_TRANSFER".equalsIgnoreCase(action);
                    }
                })
                .within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /**
     * 高频访问检测模式
     * 通用高频行为检测
     */
    public static Pattern<UserBehavior, ?> createHighFrequencyPattern(RiskRule rule) {
        LOG.info("Creating high frequency pattern: targetAction={}, windowSize={}ms, threshold={}",
                rule.getTargetActionType(), rule.getWindowSizeMs(), rule.getThreshold());

        final String targetAction = rule.getTargetActionType();

        return Pattern.<UserBehavior>begin("first")
                .where(new SimpleCondition<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior event) {
                        return targetAction.equalsIgnoreCase(event.getActionType());
                    }
                })
                .timesOrMore(rule.getThreshold())
                .greedy()
                .within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /**
     * 通用模式（用于自定义规则）
     */
    public static Pattern<UserBehavior, ?> createGenericPattern(RiskRule rule) {
        LOG.info("Creating generic pattern: targetAction={}, windowSize={}ms, threshold={}",
                rule.getTargetActionType(), rule.getWindowSizeMs(), rule.getThreshold());

        final String targetAction = rule.getTargetActionType();

        return Pattern.<UserBehavior>begin("events")
                .where(new SimpleCondition<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior event) {
                        if (targetAction == null || targetAction.isEmpty()) {
                            return true;
                        }
                        return targetAction.equalsIgnoreCase(event.getActionType());
                    }
                })
                .timesOrMore(rule.getThreshold())
                .greedy()
                .within(Time.milliseconds(rule.getWindowSizeMs()));
    }

    /**
     * 创建支付欺诈检测模式
     * 短时间内多次小额支付后进行大额支付
     */
    public static Pattern<UserBehavior, ?> createPaymentFraudPattern(RiskRule rule) {
        LOG.info("Creating payment fraud pattern: windowSize={}ms", rule.getWindowSizeMs());

        return Pattern.<UserBehavior>begin("smallPayments")
                .where(new SimpleCondition<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior event) {
                        return "PAYMENT".equalsIgnoreCase(event.getActionType())
                                && event.getAmount() != null
                                && event.getAmount() < 100.0;
                    }
                })
                .timesOrMore(3)
                .greedy()
                .followedBy("largePayment")
                .where(new SimpleCondition<UserBehavior>() {
                    @Override
                    public boolean filter(UserBehavior event) {
                        return "PAYMENT".equalsIgnoreCase(event.getActionType())
                                && event.getAmount() != null
                                && event.getAmount() >= 1000.0;
                    }
                })
                .within(Time.milliseconds(rule.getWindowSizeMs()));
    }
}
