package cn.edu.ustb.detection.cep;

import cn.edu.ustb.detection.model.AlertEvent;
import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
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

        // Use keyed state detectors for paper experiments so that window/threshold can follow dynamic rules.
        // (CEP patterns with timesOrMore() can postpone emits until window expiry; this makes experiments "no alerts".)

        DataStream<AlertEvent> abAlerts = keyed.process(new AbnormalLoginProcess()).name("Stateful Abnormal Login");
        DataStream<AlertEvent> obAlerts = keyed.process(new CountWithinWindowProcess(RiskRule.RuleType.ORDER_BRUSH))
                .name("Stateful Order Brush");
        DataStream<AlertEvent> hfAlerts = keyed.process(new CountWithinWindowProcess(RiskRule.RuleType.HIGH_FREQ_ACCESS))
                .name("Stateful High Frequency Access");

        // Keep credential stuffing detector in CEP module (not used by thesis experiments but retained).
        DataStream<AlertEvent> csAlerts = keyed.process(new CredentialStuffingProcess()).name("Stateful Credential Stuffing");

        LOG.info("Stateful detector wired: credentialStuffing + abnormalLogin + orderBrush + highFreqAccess");
        return csAlerts.union(abAlerts).union(obAlerts).union(hfAlerts);
    }

    private static class CountWithinWindowProcess extends KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent> {
        private final RiskRule.RuleType type;
        private transient ListState<UserBehavior> eventsState;
        private transient ValueState<Long> lastEmittedAt;

        private CountWithinWindowProcess(RiskRule.RuleType type) {
            this.type = type;
        }

        @Override
        public void open(Configuration parameters) {
            eventsState = getRuntimeContext().getListState(new ListStateDescriptor<>("events", UserBehavior.class));
            lastEmittedAt = getRuntimeContext().getState(new ValueStateDescriptor<>("lastEmittedAt", Long.class));
        }

        @Override
        public void processElement(Tuple2<UserBehavior, RiskRule> v, Context ctx, Collector<AlertEvent> out)
                throws Exception {
            if (v == null || v.f0 == null || v.f1 == null) {
                return;
            }
            RiskRule rule = v.f1;
            UserBehavior e = v.f0;
            if (!type.equals(rule.getRuleType())) {
                return;
            }
            long ts = e.getTimestamp();
            long window = Math.max(1L, rule.getWindowSizeMs());
            int threshold = Math.max(1, rule.getThreshold());

            LinkedList<UserBehavior> buf = new LinkedList<>();
            for (UserBehavior old : eventsState.get()) {
                if (old != null && (ts - old.getTimestamp()) <= window) {
                    buf.add(old);
                }
            }
            buf.add(e);
            eventsState.update(buf);

            // Emit once per key per window to avoid huge duplicates.
            Long last = lastEmittedAt.value();
            boolean recentlyEmitted = last != null && (ts - last) <= window;
            if (!recentlyEmitted && buf.size() >= threshold) {
                AlertEvent alert = AlertEvent.fromRuleMatch(rule, new ArrayList<>(buf));
                out.collect(alert);
                lastEmittedAt.update(ts);
                // Keep buffer (allows downstream latency metrics); but cap memory by clearing after emit.
                eventsState.clear();
            }
        }
    }

    private static class AbnormalLoginProcess extends KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent> {
        private transient ValueState<UserBehavior> lastLogin;

        @Override
        public void open(Configuration parameters) {
            lastLogin = getRuntimeContext().getState(new ValueStateDescriptor<>("lastLogin", UserBehavior.class));
        }

        @Override
        public void processElement(Tuple2<UserBehavior, RiskRule> v, Context ctx, Collector<AlertEvent> out)
                throws Exception {
            if (v == null || v.f0 == null || v.f1 == null) {
                return;
            }
            RiskRule rule = v.f1;
            UserBehavior e = v.f0;
            if (!RiskRule.RuleType.ABNORMAL_LOGIN.equals(rule.getRuleType())) {
                return;
            }
            String action = e.getActionType();
            if (CepPatternFactory.isLoginSuccess(action)) {
                lastLogin.update(e);
                return;
            }
            if (!CepPatternFactory.isSensitiveAction(action)) {
                return;
            }
            UserBehavior login = lastLogin.value();
            if (login == null) {
                return;
            }
            long dt = e.getTimestamp() - login.getTimestamp();
            if (dt < 0 || dt > rule.getWindowSizeMs()) {
                return;
            }
            List<UserBehavior> events = new ArrayList<>();
            events.add(login);
            events.add(e);
            out.collect(AlertEvent.fromRuleMatch(rule, events));
            lastLogin.clear();
        }
    }

    private static class CredentialStuffingProcess extends KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent> {
        private transient ListState<UserBehavior> fails;

        @Override
        public void open(Configuration parameters) {
            fails = getRuntimeContext().getListState(new ListStateDescriptor<>("fails", UserBehavior.class));
        }

        @Override
        public void processElement(Tuple2<UserBehavior, RiskRule> v, Context ctx, Collector<AlertEvent> out)
                throws Exception {
            if (v == null || v.f0 == null || v.f1 == null) {
                return;
            }
            RiskRule rule = v.f1;
            UserBehavior e = v.f0;
            if (!RiskRule.RuleType.CREDENTIAL_STUFFING.equals(rule.getRuleType())) {
                return;
            }
            String action = e.getActionType();
            if (CepPatternFactory.isLoginFail(action)) {
                LinkedList<UserBehavior> buf = new LinkedList<>();
                for (UserBehavior old : fails.get()) {
                    if (old != null) {
                        buf.add(old);
                    }
                }
                buf.add(e);
                fails.update(buf);
                return;
            }
            if (CepPatternFactory.isLoginSuccess(action)) {
                List<UserBehavior> buf = new ArrayList<>();
                for (UserBehavior old : fails.get()) {
                    if (old != null) {
                        buf.add(old);
                    }
                }
                if (buf.size() >= rule.getThreshold()) {
                    buf.add(e);
                    out.collect(AlertEvent.fromRuleMatch(rule, buf));
                }
                fails.clear();
            }
        }
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
