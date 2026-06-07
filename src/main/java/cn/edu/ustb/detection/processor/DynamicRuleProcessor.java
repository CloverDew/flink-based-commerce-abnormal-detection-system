package cn.edu.ustb.detection.processor;

import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import java.util.Map;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import lombok.extern.slf4j.Slf4j;

/**
 * 动态规则处理器
 *
 * <p>
 * 使用 Broadcast State 模式实现风控规则的热加载。
 *
 * <p>
 * 处理流程： 1. 规则流（Broadcast Stream）：将规则广播到所有并行实例，存储在 Broadcast State 中 2. 事件流（Data
 * Stream）：读取当前规则集，对每个事件进行规则匹配预处理
 *
 * <p>
 * 输出：Tuple2<UserBehavior, RiskRule> 表示事件与匹配的规则对
 */
@Slf4j
public class DynamicRuleProcessor
        extends
            BroadcastProcessFunction<UserBehavior, RiskRule, Tuple2<UserBehavior, RiskRule>> {

    private static final long serialVersionUID = 1L;

    /** 规则状态描述符（Broadcast State） */
    public static final MapStateDescriptor<String, RiskRule> RULE_STATE_DESCRIPTOR = new MapStateDescriptor<>(
            "risk-rules", BasicTypeInfo.STRING_TYPE_INFO, TypeInformation.of(new TypeHint<RiskRule>() {
            }));

    @Override
    public void processElement(UserBehavior event,
            BroadcastProcessFunction<UserBehavior, RiskRule, Tuple2<UserBehavior, RiskRule>>.ReadOnlyContext ctx,
            Collector<Tuple2<UserBehavior, RiskRule>> out) throws Exception {

        if (event == null || !event.isValid()) {
            log.warn("Received invalid user behavior event, skipping: {}", event);
            return;
        }

        ReadOnlyBroadcastState<String, RiskRule> ruleState = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);

        if (ruleState == null) {
            log.debug("Rule state is null, no rules loaded yet");
            return;
        }

        int matchedRuleCount = 0;

        for (Map.Entry<String, RiskRule> entry : ruleState.immutableEntries()) {
            RiskRule rule = entry.getValue();

            if (rule == null || !rule.isEnabled() || !rule.isValid()) {
                continue;
            }

            if (isEventMatchRule(event, rule)) {
                out.collect(Tuple2.of(event, rule));
                matchedRuleCount++;

                if (log.isDebugEnabled()) {
                    log.debug("Event matched rule: userId={}, actionType={}, ruleId={}, ruleType={}", event.getUserId(),
                            event.getActionType(), rule.getRuleId(), rule.getRuleType());
                }
            }
        }

        if (matchedRuleCount == 0 && log.isTraceEnabled()) {
            log.trace("No rule matched for event: userId={}, actionType={}", event.getUserId(), event.getActionType());
        }
    }

    @Override
    public void processBroadcastElement(RiskRule rule,
            BroadcastProcessFunction<UserBehavior, RiskRule, Tuple2<UserBehavior, RiskRule>>.Context ctx,
            Collector<Tuple2<UserBehavior, RiskRule>> out) throws Exception {

        if (rule == null) {
            log.warn("Received null rule from broadcast stream, skipping");
            return;
        }

        BroadcastState<String, RiskRule> ruleState = ctx.getBroadcastState(RULE_STATE_DESCRIPTOR);

        if (rule.getRuleId() == null || rule.getRuleId().isEmpty()) {
            log.warn("Rule ID is null or empty, skipping: {}", rule);
            return;
        }

        RiskRule existingRule = ruleState.get(rule.getRuleId());

        if (RiskRule.RuleStatus.DISABLED.equals(rule.getStatus())) {
            if (existingRule != null) {
                ruleState.remove(rule.getRuleId());
                log.info("Rule disabled and removed from state: ruleId={}, ruleName={}", rule.getRuleId(),
                        rule.getRuleName());
            }
            return;
        }

        if (!rule.isValid()) {
            log.warn("Rule validation failed, skipping: {}", rule);
            return;
        }

        if (existingRule != null && existingRule.getVersion() >= rule.getVersion()) {
            log.info("Received older or same version rule, skipping: ruleId={}, existingVersion={}, newVersion={}",
                    rule.getRuleId(), existingRule.getVersion(), rule.getVersion());
            return;
        }

        ruleState.put(rule.getRuleId(), rule);

        log.info(
                "Rule {} in state: ruleId={}, ruleName={}, ruleType={}, targetActionType={}, "
                        + "windowSize={}ms, threshold={}, version={}",
                existingRule == null ? "added" : "updated", rule.getRuleId(), rule.getRuleName(), rule.getRuleType(),
                rule.getTargetActionType(), rule.getWindowSizeMs(), rule.getThreshold(), rule.getVersion());

        logCurrentRuleState(ruleState);
    }

    /** 判断事件是否匹配规则的行为类型 */
    private boolean isEventMatchRule(UserBehavior event, RiskRule rule) {
        if (event.getActionType() == null) {
            return false;
        }
        if (RiskRule.RuleType.ABNORMAL_LOGIN.equals(rule.getRuleType())) {
            String action = event.getActionType();
            return "LOGIN".equalsIgnoreCase(action) || "LOGIN_SUCCESS".equalsIgnoreCase(action)
                    || "CHANGE_PASSWORD".equalsIgnoreCase(action) || "BIND_PHONE".equalsIgnoreCase(action)
                    || "WITHDRAW".equalsIgnoreCase(action) || "LARGE_TRANSFER".equalsIgnoreCase(action);
        }
        if (RiskRule.RuleType.CREDENTIAL_STUFFING.equals(rule.getRuleType())) {
            // allow sequence detection: multiple fails followed by success
            String action = event.getActionType();
            return "LOGIN_FAIL".equalsIgnoreCase(action) || "LOGIN_SUCCESS".equalsIgnoreCase(action)
                    || "LOGIN".equalsIgnoreCase(action);
        }
        if (RiskRule.RuleType.PAYMENT_FRAUD.equals(rule.getRuleType())) {
            // allow rule to observe both normal payments and fraud-labelled payments
            String action = event.getActionType();
            return "PAYMENT".equalsIgnoreCase(action) || "PAYMENT_FRAUD".equalsIgnoreCase(action);
        }
        if (rule.getTargetActionType() == null) {
            return false;
        }
        return event.getActionType().equalsIgnoreCase(rule.getTargetActionType());
    }

    /** 记录当前规则状态（调试用） */
    private void logCurrentRuleState(BroadcastState<String, RiskRule> ruleState) throws Exception {
        if (!log.isDebugEnabled()) {
            return;
        }

        int count = 0;
        StringBuilder sb = new StringBuilder("Current rules in state: [");
        for (Map.Entry<String, RiskRule> entry : ruleState.entries()) {
            if (count > 0) {
                sb.append(", ");
            }
            sb.append(entry.getKey());
            count++;
        }
        sb.append("], total: ").append(count);
        log.debug(sb.toString());
    }
}
