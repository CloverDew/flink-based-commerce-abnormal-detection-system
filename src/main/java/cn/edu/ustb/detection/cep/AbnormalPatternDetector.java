package cn.edu.ustb.detection.cep;

import cn.edu.ustb.detection.model.AlertEvent;
import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
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
import lombok.extern.slf4j.Slf4j;

/**
 * 异常行为模式检测器（CEP/NFA）
 *
 * <p>
 * 负责将业务规则翻译为 Flink CEP 可执行的时序匹配网络（NFA），并将匹配到的事件序列转化为带上下文证据的告警事件。
 */
@Slf4j
public class AbnormalPatternDetector {
    private static final long ALLOWED_OUT_OF_ORDERNESS_MS = 5_000L;

    private AbnormalPatternDetector() {
    }

    public static DataStream<AlertEvent> buildAlertStream(
            SingleOutputStreamOperator<Tuple2<UserBehavior, RiskRule>> matchedStream,
            KeySelector<Tuple2<UserBehavior, RiskRule>, String> keySelector) {

        KeyedStream<Tuple2<UserBehavior, RiskRule>, String> keyed = matchedStream.keyBy(keySelector);

        DataStream<AlertEvent> abAlerts = keyed.process(new AbnormalLoginProcess()).name("Stateful Abnormal Login");
        DataStream<AlertEvent> obAlerts = keyed.process(new CountWithinWindowProcess(RiskRule.RuleType.ORDER_BRUSH))
                .name("Stateful Order Brush");
        DataStream<AlertEvent> hfAlerts = keyed.process(new CountWithinWindowProcess(RiskRule.RuleType.HIGH_FREQ_ACCESS))
                .name("Stateful High Frequency Access");
        DataStream<AlertEvent> csAlerts = keyed.process(new CredentialStuffingProcess()).name("Stateful Credential Stuffing");
        DataStream<AlertEvent> pfAlerts = keyed.process(new PaymentFraudProcess()).name("Stateful Payment Fraud");

        log.info("Stateful detector wired: credentialStuffing + abnormalLogin + orderBrush + highFreqAccess + paymentFraud");
        return csAlerts.union(abAlerts).union(obAlerts).union(hfAlerts).union(pfAlerts);
    }

    private static class CountWithinWindowProcess extends KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent> {
        private final RiskRule.RuleType type;
        private transient ListState<UserBehavior> eventsState;
        private transient ValueState<Long> maxSeenTimestamp;
        private transient ValueState<Long> lastEmittedAt;

        private CountWithinWindowProcess(RiskRule.RuleType type) {
            this.type = type;
        }

        @Override
        public void open(Configuration parameters) {
            eventsState = getRuntimeContext().getListState(new ListStateDescriptor<>("events", UserBehavior.class));
            maxSeenTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("maxSeenTimestamp", Long.class));
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
            List<UserBehavior> buf = loadEvents(eventsState);
            buf.add(e);
            long newMax = updateMaxTimestamp(maxSeenTimestamp, ts);
            buf = pruneRecent(buf, newMax, window);

            List<UserBehavior> matched = findCountWindow(buf, window, threshold);
            if (matched != null && isNewMatch(lastEmittedAt, matched)) {
                emitAlert(rule, matched, out);
                lastEmittedAt.update(matched.get(matched.size() - 1).getTimestamp());
                eventsState.clear();
                maxSeenTimestamp.clear();
                return;
            }
            eventsState.update(buf);
        }
    }

    private static class AbnormalLoginProcess extends KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent> {
        private transient ListState<UserBehavior> relevantEvents;
        private transient ValueState<Long> maxSeenTimestamp;
        private transient ValueState<Long> lastEmittedAt;

        @Override
        public void open(Configuration parameters) {
            relevantEvents = getRuntimeContext()
                    .getListState(new ListStateDescriptor<>("abnormalLoginEvents", UserBehavior.class));
            maxSeenTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("abnormalLoginMaxSeen", Long.class));
            lastEmittedAt = getRuntimeContext().getState(new ValueStateDescriptor<>("abnormalLoginLastEmit", Long.class));
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
            List<UserBehavior> buf = loadEvents(relevantEvents);
            buf.add(e);
            long newMax = updateMaxTimestamp(maxSeenTimestamp, e.getTimestamp());
            buf = pruneRecent(buf, newMax, Math.max(1L, rule.getWindowSizeMs()));

            List<UserBehavior> matched = findAbnormalLoginMatch(buf, rule.getWindowSizeMs());
            if (matched != null && isNewMatch(lastEmittedAt, matched)) {
                emitAlert(rule, matched, out);
                lastEmittedAt.update(matched.get(matched.size() - 1).getTimestamp());
                relevantEvents.clear();
                maxSeenTimestamp.clear();
                return;
            }
            relevantEvents.update(buf);
        }
    }

    private static class CredentialStuffingProcess extends KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent> {
        private transient ListState<UserBehavior> fails;
        private transient ValueState<Long> maxSeenTimestamp;
        private transient ValueState<Long> lastEmittedAt;

        @Override
        public void open(Configuration parameters) {
            fails = getRuntimeContext().getListState(new ListStateDescriptor<>("fails", UserBehavior.class));
            maxSeenTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("credentialMaxSeen", Long.class));
            lastEmittedAt = getRuntimeContext().getState(new ValueStateDescriptor<>("credentialLastEmit", Long.class));
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
            if (!CepPatternFactory.isLoginFail(e.getActionType())) {
                return;
            }
            List<UserBehavior> buf = loadEvents(fails);
            buf.add(e);
            long newMax = updateMaxTimestamp(maxSeenTimestamp, e.getTimestamp());
            buf = pruneRecent(buf, newMax, Math.max(1L, rule.getWindowSizeMs()));
            List<UserBehavior> matched = findCountWindow(buf, rule.getWindowSizeMs(), Math.max(1, rule.getThreshold()));
            if (matched != null && isNewMatch(lastEmittedAt, matched)) {
                emitAlert(rule, matched, out);
                lastEmittedAt.update(matched.get(matched.size() - 1).getTimestamp());
                fails.clear();
                maxSeenTimestamp.clear();
                return;
            }
            fails.update(buf);
        }
    }

    private static class PaymentFraudProcess extends KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent> {
        private transient ListState<UserBehavior> payments;
        private transient ValueState<Long> maxSeenTimestamp;
        private transient ValueState<Long> lastEmittedAt;

        @Override
        public void open(Configuration parameters) {
            payments = getRuntimeContext().getListState(new ListStateDescriptor<>("payments", UserBehavior.class));
            maxSeenTimestamp = getRuntimeContext().getState(new ValueStateDescriptor<>("paymentFraudMaxSeen", Long.class));
            lastEmittedAt = getRuntimeContext().getState(new ValueStateDescriptor<>("paymentFraudLastEmit", Long.class));
        }

        @Override
        public void processElement(Tuple2<UserBehavior, RiskRule> v, Context ctx, Collector<AlertEvent> out)
                throws Exception {
            if (v == null || v.f0 == null || v.f1 == null) {
                return;
            }
            RiskRule rule = v.f1;
            UserBehavior e = v.f0;
            if (!RiskRule.RuleType.PAYMENT_FRAUD.equals(rule.getRuleType())) {
                return;
            }
            List<UserBehavior> buf = loadEvents(payments);
            buf.add(e);
            long newMax = updateMaxTimestamp(maxSeenTimestamp, e.getTimestamp());
            buf = pruneRecent(buf, newMax, Math.max(1L, rule.getWindowSizeMs()));
            List<UserBehavior> matched = findPaymentFraudMatch(buf, rule.getWindowSizeMs(), Math.max(1, rule.getThreshold()));
            if (matched != null && isNewMatch(lastEmittedAt, matched)) {
                emitAlert(rule, matched, out);
                lastEmittedAt.update(matched.get(matched.size() - 1).getTimestamp());
                payments.clear();
                maxSeenTimestamp.clear();
                return;
            }
            payments.update(buf);
        }
    }

    private static List<UserBehavior> loadEvents(ListState<UserBehavior> state) throws Exception {
        List<UserBehavior> rows = new ArrayList<>();
        for (UserBehavior event : state.get()) {
            if (event != null) {
                rows.add(event);
            }
        }
        return rows;
    }

    private static long updateMaxTimestamp(ValueState<Long> maxSeenState, long eventTs) throws Exception {
        Long current = maxSeenState.value();
        long next = current == null ? eventTs : Math.max(current, eventTs);
        maxSeenState.update(next);
        return next;
    }

    private static List<UserBehavior> pruneRecent(List<UserBehavior> events, long maxSeenTs, long window) {
        long minKeepTs = Math.max(0L, maxSeenTs - window - ALLOWED_OUT_OF_ORDERNESS_MS);
        List<UserBehavior> kept = new ArrayList<>();
        for (UserBehavior event : events) {
            if (event != null && event.getTimestamp() >= minKeepTs) {
                kept.add(event);
            }
        }
        kept.sort(Comparator.comparingLong(UserBehavior::getTimestamp));
        return kept;
    }

    private static boolean isNewMatch(ValueState<Long> lastEmittedAt, List<UserBehavior> matched) throws Exception {
        if (matched == null || matched.isEmpty()) {
            return false;
        }
        Long last = lastEmittedAt.value();
        long lastMatchedTs = matched.get(matched.size() - 1).getTimestamp();
        return last == null || lastMatchedTs > last.longValue();
    }

    private static List<UserBehavior> findCountWindow(List<UserBehavior> events, long window, int threshold) {
        if (events == null || events.size() < threshold) {
            return null;
        }
        int left = 0;
        for (int right = 0; right < events.size(); right++) {
            while (left < right && events.get(right).getTimestamp() - events.get(left).getTimestamp() > window) {
                left++;
            }
            if (right - left + 1 >= threshold) {
                return new ArrayList<>(events.subList(left, right + 1));
            }
        }
        return null;
    }

    private static List<UserBehavior> findAbnormalLoginMatch(List<UserBehavior> events, long window) {
        if (events == null || events.size() < 2) {
            return null;
        }
        UserBehavior latestLogin = null;
        for (UserBehavior event : events) {
            if (event == null) {
                continue;
            }
            String action = event.getActionType();
            if (CepPatternFactory.isLoginSuccess(action)) {
                latestLogin = event;
                continue;
            }
            if (!CepPatternFactory.isSensitiveAction(action) || latestLogin == null) {
                continue;
            }
            long delta = event.getTimestamp() - latestLogin.getTimestamp();
            if (delta >= 0L && delta <= window) {
                List<UserBehavior> matched = new ArrayList<>();
                matched.add(latestLogin);
                matched.add(event);
                return matched;
            }
        }
        return null;
    }

    private static List<UserBehavior> findPaymentFraudMatch(List<UserBehavior> events, long window, int threshold) {
        if (events == null || events.isEmpty()) {
            return null;
        }
        for (int i = 0; i < events.size(); i++) {
            UserBehavior candidate = events.get(i);
            if (!isSuspiciousPayment(candidate)) {
                continue;
            }
            List<UserBehavior> smalls = new ArrayList<>();
            for (int j = 0; j < i; j++) {
                UserBehavior previous = events.get(j);
                if (previous == null) {
                    continue;
                }
                long delta = candidate.getTimestamp() - previous.getTimestamp();
                if (delta < 0L || delta > window) {
                    continue;
                }
                if (isSmallPayment(previous)) {
                    smalls.add(previous);
                }
            }
            if (smalls.size() >= threshold) {
                List<UserBehavior> matched = new ArrayList<>(smalls);
                matched.add(candidate);
                return matched;
            }
        }
        return null;
    }

    private static boolean isSmallPayment(UserBehavior event) {
        return event != null && CepPatternFactory.isPayment(event.getActionType()) && event.getAmount() != null
                && event.getAmount().doubleValue() < 100.0d;
    }

    private static boolean isSuspiciousPayment(UserBehavior event) {
        if (event == null) {
            return false;
        }
        if (CepPatternFactory.isPaymentFraud(event.getActionType())) {
            return true;
        }
        return CepPatternFactory.isPayment(event.getActionType()) && event.getAmount() != null
                && event.getAmount().doubleValue() >= 1000.0d;
    }

    private static void emitAlert(RiskRule rule, List<UserBehavior> matchedEvents, Collector<AlertEvent> out) {
        AlertEvent alert = toAlertEvent(rule, matchedEvents);
        if (alert != null) {
            out.collect(alert);
        }
    }

    private static AlertEvent toAlertEvent(RiskRule rule, List<UserBehavior> matchedEvents) {
        if (rule == null || matchedEvents == null || matchedEvents.isEmpty()) {
            return null;
        }
        List<UserBehavior> events = new ArrayList<>(matchedEvents);
        events.sort(Comparator.comparingLong(UserBehavior::getTimestamp));
        long firstTs = events.get(0).getTimestamp();
        long lastTs = events.get(events.size() - 1).getTimestamp();
        if (lastTs < firstTs || lastTs - firstTs > rule.getWindowSizeMs()) {
            return null;
        }
        AlertEvent alert = AlertEvent.fromRuleMatch(rule, events);
        double densityPerSec = events.size() / Math.max(1.0d, rule.getWindowSizeMs() / 1000.0d);
        double score = rule.getSeverityWeight()
                * (1.0d + ((double) events.size() / Math.max(1, rule.getThreshold()))
                        + Math.min(1.5d, densityPerSec / 10.0d));
        alert.setRiskScore(score);
        return score >= rule.getScoreThreshold() ? alert : null;
    }
}
