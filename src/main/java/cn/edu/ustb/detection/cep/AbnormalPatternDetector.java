package cn.edu.ustb.detection.cep;

import cn.edu.ustb.detection.model.AlertEvent;
import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异常行为模式检测器（CEP/NFA）
 *
 * <p>
 * 负责将业务规则翻译为 Flink CEP 可执行的时序匹配网络（NFA），并将匹配到的事件序列转化为带上下文证据的告警事件。
 */
public class AbnormalPatternDetector {

    private static final Logger LOG = LoggerFactory.getLogger(AbnormalPatternDetector.class);

    private AbnormalPatternDetector() {
    }

    public static DataStream<AlertEvent> buildAlertStream(
            SingleOutputStreamOperator<Tuple2<UserBehavior, RiskRule>> matchedStream,
            KeySelector<Tuple2<UserBehavior, RiskRule>, String> keySelector) {

        KeyedStream<Tuple2<UserBehavior, RiskRule>, String> keyed = matchedStream.keyBy(keySelector);

        Pattern<Tuple2<UserBehavior, RiskRule>, ?> credentialStuffing = Pattern
                .<Tuple2<UserBehavior, RiskRule>>begin("fail")
                .where(matchRuleTypeAction(RiskRule.RuleType.CREDENTIAL_STUFFING, "LOGIN_FAIL"))
                .oneOrMore()
                .consecutive()
                .followedBy("success")
                .where(new SimpleCondition<Tuple2<UserBehavior, RiskRule>>() {
                    @Override
                    public boolean filter(Tuple2<UserBehavior, RiskRule> v) {
                        return v != null && v.f0 != null && v.f1 != null
                                && RiskRule.RuleType.CREDENTIAL_STUFFING.equals(v.f1.getRuleType())
                                && CepPatternFactory.isLoginSuccess(v.f0.getActionType());
                    }
                })
                .within(Time.hours(1));

        Pattern<Tuple2<UserBehavior, RiskRule>, ?> abnormalLogin = Pattern
                .<Tuple2<UserBehavior, RiskRule>>begin("login")
                .where(new SimpleCondition<Tuple2<UserBehavior, RiskRule>>() {
                    @Override
                    public boolean filter(Tuple2<UserBehavior, RiskRule> v) {
                        return v != null && v.f0 != null && v.f1 != null
                                && RiskRule.RuleType.ABNORMAL_LOGIN.equals(v.f1.getRuleType())
                                && CepPatternFactory.isLoginSuccess(v.f0.getActionType());
                    }
                })
                .followedBy("sensitiveAction")
                .where(new SimpleCondition<Tuple2<UserBehavior, RiskRule>>() {
                    @Override
                    public boolean filter(Tuple2<UserBehavior, RiskRule> v) {
                        return v != null && v.f0 != null && v.f1 != null
                                && RiskRule.RuleType.ABNORMAL_LOGIN.equals(v.f1.getRuleType())
                                && CepPatternFactory.isSensitiveAction(v.f0.getActionType());
                    }
                })
                .within(Time.hours(1));

        // Paper/thesis baseline patterns:
        // - ORDER_BRUSH: 1 user places >=5 orders within window
        // - HIGH_FREQ_ACCESS: 1 user views >=100 times within window
        Pattern<Tuple2<UserBehavior, RiskRule>, ?> orderBrush = Pattern
                .<Tuple2<UserBehavior, RiskRule>>begin("orders")
                .where(matchRuleTypeAction(RiskRule.RuleType.ORDER_BRUSH, "ORDER"))
                .timesOrMore(5)
                .greedy()
                .within(Time.hours(1));

        Pattern<Tuple2<UserBehavior, RiskRule>, ?> highFreqAccess = Pattern
                .<Tuple2<UserBehavior, RiskRule>>begin("views")
                .where(matchRuleTypeAction(RiskRule.RuleType.HIGH_FREQ_ACCESS, "VIEW"))
                .timesOrMore(100)
                .greedy()
                .within(Time.hours(1));

        DataStream<AlertEvent> csAlerts = CEP.pattern(keyed, credentialStuffing)
                .select(new PatternSelectFunction<Tuple2<UserBehavior, RiskRule>, AlertEvent>() {
                    @Override
                    public AlertEvent select(Map<String, List<Tuple2<UserBehavior, RiskRule>>> pattern) {
                        List<Tuple2<UserBehavior, RiskRule>> all = new ArrayList<>();
                        all.addAll(pattern.getOrDefault("fail", Collections.emptyList()));
                        all.addAll(pattern.getOrDefault("success", Collections.emptyList()));
                        return toAlertEvent(all);
                    }
                })
                .filter(a -> a != null)
                .name("CEP Credential Stuffing");

        DataStream<AlertEvent> abAlerts = CEP.pattern(keyed, abnormalLogin)
                .select(new PatternSelectFunction<Tuple2<UserBehavior, RiskRule>, AlertEvent>() {
                    @Override
                    public AlertEvent select(Map<String, List<Tuple2<UserBehavior, RiskRule>>> pattern) {
                        List<Tuple2<UserBehavior, RiskRule>> all = new ArrayList<>();
                        all.addAll(pattern.getOrDefault("login", Collections.emptyList()));
                        all.addAll(pattern.getOrDefault("sensitiveAction", Collections.emptyList()));
                        return toAlertEvent(all);
                    }
                })
                .filter(a -> a != null)
                .name("CEP Abnormal Login");

        DataStream<AlertEvent> obAlerts = CEP.pattern(keyed, orderBrush)
                .select(new PatternSelectFunction<Tuple2<UserBehavior, RiskRule>, AlertEvent>() {
                    @Override
                    public AlertEvent select(Map<String, List<Tuple2<UserBehavior, RiskRule>>> pattern) {
                        return toAlertEvent(pattern.getOrDefault("orders", Collections.emptyList()));
                    }
                })
                .filter(a -> a != null)
                .name("CEP Order Brush");

        DataStream<AlertEvent> hfAlerts = CEP.pattern(keyed, highFreqAccess)
                .select(new PatternSelectFunction<Tuple2<UserBehavior, RiskRule>, AlertEvent>() {
                    @Override
                    public AlertEvent select(Map<String, List<Tuple2<UserBehavior, RiskRule>>> pattern) {
                        return toAlertEvent(pattern.getOrDefault("views", Collections.emptyList()));
                    }
                })
                .filter(a -> a != null)
                .name("CEP High Frequency Access");

        LOG.info("CEP detector wired: credentialStuffing + abnormalLogin + orderBrush + highFreqAccess");
        return csAlerts.union(abAlerts).union(obAlerts).union(hfAlerts);
    }

    private static SimpleCondition<Tuple2<UserBehavior, RiskRule>> matchRuleTypeAction(RiskRule.RuleType type, String action) {
        return new SimpleCondition<Tuple2<UserBehavior, RiskRule>>() {
            @Override
            public boolean filter(Tuple2<UserBehavior, RiskRule> v) {
                return v != null && v.f0 != null && v.f1 != null && type.equals(v.f1.getRuleType())
                        && action != null && action.equalsIgnoreCase(v.f0.getActionType());
            }
        };
    }

    private static AlertEvent toAlertEvent(List<Tuple2<UserBehavior, RiskRule>> tuples) {
        if (tuples == null || tuples.isEmpty()) {
            return null;
        }
        List<Tuple2<UserBehavior, RiskRule>> sorted = tuples.stream()
                .filter(t -> t != null && t.f0 != null && t.f1 != null)
                .sorted(Comparator.comparingLong(t -> t.f0.getTimestamp()))
                .collect(Collectors.toList());
        if (sorted.isEmpty()) {
            return null;
        }

        RiskRule rule = sorted.get(sorted.size() - 1).f1;
        List<UserBehavior> events = sorted.stream().map(t -> t.f0).collect(Collectors.toList());

        long firstTs = events.get(0).getTimestamp();
        long lastTs = events.get(events.size() - 1).getTimestamp();
        if (lastTs - firstTs > rule.getWindowSizeMs()) {
            return null;
        }

        if (RiskRule.RuleType.CREDENTIAL_STUFFING.equals(rule.getRuleType())) {
            long failCount = events.stream().filter(e -> e != null && CepPatternFactory.isLoginFail(e.getActionType()))
                    .count();
            if (failCount < rule.getThreshold()) {
                return null;
            }
        } else if (events.size() < rule.getThreshold()) {
            return null;
        }

        AlertEvent alert = AlertEvent.fromRuleMatch(rule, events);
        double densityPerSec = events.size() / Math.max(1.0, rule.getWindowSizeMs() / 1000.0);
        double score = rule.getSeverityWeight()
                * ((double) events.size() / Math.max(1, rule.getThreshold()) + Math.min(1.5, densityPerSec / 10.0));
        alert.setRiskScore(score);
        return score >= rule.getScoreThreshold() ? alert : null;
    }
}
