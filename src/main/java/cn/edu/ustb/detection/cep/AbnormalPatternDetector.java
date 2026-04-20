package cn.edu.ustb.detection.cep;

import cn.edu.ustb.detection.model.AlertEvent;
import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 异常行为模式检测器
 *
 * <p>
 * 基于 Keyed State 实现滑动窗口内的事件计数和模式匹配。 对于每个分组键（如 userId 或 IP），维护时间窗口内的事件列表，
 * 当事件数量达到规则阈值时触发告警。
 *
 * <p>
 * 此实现使用 KeyedProcessFunction 配合定时器实现滑动窗口效果， 比传统 CEP 更灵活，支持动态规则配置。
 */
public class AbnormalPatternDetector extends KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent> {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(AbnormalPatternDetector.class);

    /** 存储每个规则对应的事件列表 Key: ruleId, Value: 事件列表（按时间戳排序） */
    private transient MapState<String, List<UserBehavior>> eventsByRuleState;

    /** 存储当前有效的规则配置 Key: ruleId, Value: 规则 */
    private transient MapState<String, RiskRule> activeRulesState;

    /** 防重复告警：存储最近触发告警的时间戳 Key: ruleId, Value: 最后告警时间 */
    private transient MapState<String, Long> lastAlertTimeState;

    /** 告警冷却时间（同一规则在此时间内不重复告警） */
    private static final long ALERT_COOLDOWN_MS = 30_000L;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        eventsByRuleState = getRuntimeContext().getMapState(new MapStateDescriptor<>("events-by-rule",
                TypeInformation.of(String.class), TypeInformation.of(new TypeHint<List<UserBehavior>>() {
                })));

        activeRulesState = getRuntimeContext().getMapState(new MapStateDescriptor<>("active-rules",
                TypeInformation.of(String.class), TypeInformation.of(RiskRule.class)));

        lastAlertTimeState = getRuntimeContext().getMapState(new MapStateDescriptor<>("last-alert-time",
                TypeInformation.of(String.class), TypeInformation.of(Long.class)));

        LOG.info("AbnormalPatternDetector initialized");
    }

    @Override
    public void processElement(Tuple2<UserBehavior, RiskRule> value,
            KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent>.Context ctx,
            Collector<AlertEvent> out) throws Exception {

        UserBehavior event = value.f0;
        RiskRule rule = value.f1;

        if (event == null || rule == null) {
            LOG.warn("Received null event or rule, skipping");
            return;
        }

        String ruleId = rule.getRuleId();
        long currentTime = event.getTimestamp();

        activeRulesState.put(ruleId, rule);

        List<UserBehavior> events = eventsByRuleState.get(ruleId);
        if (events == null) {
            events = new ArrayList<>();
        }

        events.add(event);

        long windowStart = currentTime - rule.getWindowSizeMs();
        events = cleanExpiredEvents(events, windowStart);

        eventsByRuleState.put(ruleId, events);

        if (LOG.isDebugEnabled()) {
            LOG.debug("Added event for rule {}: key={}, eventCount={}, windowStart={}, currentTime={}", ruleId,
                    ctx.getCurrentKey(), events.size(), windowStart, currentTime);
        }

        if (events.size() >= rule.getThreshold()) {
            if (shouldTriggerAlert(ruleId, currentTime)) {
                AlertEvent alert = AlertEvent.fromRuleMatch(rule, new ArrayList<>(events));
                out.collect(alert);

                lastAlertTimeState.put(ruleId, currentTime);

                LOG.info("Alert triggered: ruleId={}, ruleType={}, key={}, matchCount={}, threshold={}", ruleId,
                        rule.getRuleType(), ctx.getCurrentKey(), events.size(), rule.getThreshold());

                events.clear();
                eventsByRuleState.put(ruleId, events);
            } else {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Alert suppressed (cooldown): ruleId={}, key={}", ruleId, ctx.getCurrentKey());
                }
            }
        }

        scheduleCleanupTimer(ctx, currentTime, rule.getWindowSizeMs());
    }

    @Override
    public void onTimer(long timestamp,
            KeyedProcessFunction<String, Tuple2<UserBehavior, RiskRule>, AlertEvent>.OnTimerContext ctx,
            Collector<AlertEvent> out) throws Exception {

        if (LOG.isDebugEnabled()) {
            LOG.debug("Cleanup timer fired: key={}, timestamp={}", ctx.getCurrentKey(), timestamp);
        }

        for (Map.Entry<String, RiskRule> ruleEntry : activeRulesState.entries()) {
            String ruleId = ruleEntry.getKey();
            RiskRule rule = ruleEntry.getValue();

            if (rule == null) {
                continue;
            }

            List<UserBehavior> events = eventsByRuleState.get(ruleId);
            if (events == null || events.isEmpty()) {
                continue;
            }

            long windowStart = timestamp - rule.getWindowSizeMs();
            List<UserBehavior> cleanedEvents = cleanExpiredEvents(events, windowStart);

            if (cleanedEvents.size() < events.size()) {
                eventsByRuleState.put(ruleId, cleanedEvents);
                LOG.debug("Cleaned expired events for rule {}: before={}, after={}", ruleId, events.size(),
                        cleanedEvents.size());
            }
        }
    }

    /** 清理过期事件 */
    private List<UserBehavior> cleanExpiredEvents(List<UserBehavior> events, long windowStart) {
        List<UserBehavior> validEvents = new ArrayList<>();
        for (UserBehavior event : events) {
            if (event.getTimestamp() >= windowStart) {
                validEvents.add(event);
            }
        }
        return validEvents;
    }

    /** 判断是否应该触发告警（防重复告警） */
    private boolean shouldTriggerAlert(String ruleId, long currentTime) throws Exception {
        Long lastAlertTime = lastAlertTimeState.get(ruleId);
        if (lastAlertTime == null) {
            return true;
        }
        return (currentTime - lastAlertTime) >= ALERT_COOLDOWN_MS;
    }

    /** 注册清理定时器 */
    private void scheduleCleanupTimer(Context ctx, long currentTime, long windowSize) {
        long cleanupTime = currentTime + windowSize + 1000L;
        ctx.timerService().registerEventTimeTimer(cleanupTime);
    }
}
