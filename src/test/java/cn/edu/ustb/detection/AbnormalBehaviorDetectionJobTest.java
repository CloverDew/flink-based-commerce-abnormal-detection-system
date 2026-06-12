package cn.edu.ustb.detection;

import static org.junit.jupiter.api.Assertions.*;

import cn.edu.ustb.detection.cep.AbnormalPatternDetector;
import cn.edu.ustb.detection.model.AlertEvent;
import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import cn.edu.ustb.detection.processor.DynamicRuleProcessor;
import cn.edu.ustb.detection.util.KeySelectorFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.RegisterExtension;
import lombok.extern.slf4j.Slf4j;

/**
 * 异常行为检测系统集成测试
 *
 * <p>
 * 使用 Flink MiniCluster 进行端到端测试，验证： 1. 动态规则加载功能 2. 异常模式检测功能 3. 告警生成功能 4. 边界情况处理
 */
@Slf4j
public class AbnormalBehaviorDetectionJobTest {

    @RegisterExtension
    public static final MiniClusterExtension MINI_CLUSTER_EXTENSION = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder().setNumberSlotsPerTaskManager(4).setNumberTaskManagers(1)
                    .build());

    private StreamExecutionEnvironment env;

    @BeforeEach
    void setup() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        CollectSink.clear();
    }

    @AfterEach
    void tearDown() {
        CollectSink.clear();
    }

    @Test
    @DisplayName("Test credential stuffing attack detection")
    void testCredentialStuffingDetection() throws Exception {
        RiskRule rule = createCredentialStuffingRule();
        long baseTime = System.currentTimeMillis();
        List<UserBehavior> events = new ArrayList<>();
        events.add(createLoginFailEvent("user1", "192.168.1.100", baseTime));
        events.add(createLoginFailEvent("user2", "192.168.1.100", baseTime + 10_000));
        events.add(createLoginFailEvent("user3", "192.168.1.100", baseTime + 20_000));
        events.add(createLoginFailEvent("user4", "192.168.1.100", baseTime + 30_000));

        DataStream<AlertEvent> alertStream = buildDetectorOnlyPipeline(env, events, rule);
        alertStream.addSink(new CollectSink());
        env.execute("Credential Stuffing Test");
        List<AlertEvent> alerts = CollectSink.getValues();
        assertFalse(alerts.isEmpty(), "Expected credential stuffing alert to be emitted");
        AlertEvent first = alerts.get(0);
        assertNotNull(first.getAlertId());
        assertEquals(rule.getRuleId(), first.getRuleId());
        assertEquals(RiskRule.RuleType.CREDENTIAL_STUFFING, first.getRuleType());
        assertTrue(first.getMatchCount() >= rule.getThreshold());
        assertTrue(first.getRiskScore() >= rule.getScoreThreshold());
    }

    /** 测试刷单检测 场景：同一用户在 1 分钟内下单 5 次 */
    @Test
    @DisplayName("Test order brush detection")
    void testOrderBrushDetection() throws Exception {
        RiskRule rule = createOrderBrushRule();

        long baseTime = System.currentTimeMillis();
        List<UserBehavior> events = new ArrayList<>();

        for (int i = 0; i < 6; i++) {
            events.add(createOrderEvent("user123", "192.168.1.1", baseTime + i * 5_000));
        }

        DataStream<AlertEvent> alertStream = buildDetectorOnlyPipeline(env, events, rule);
        alertStream.addSink(new CollectSink());
        env.execute("Order Brush Test");
        List<AlertEvent> alerts = CollectSink.getValues();
        assertFalse(alerts.isEmpty(), "Expected order brush alert to be emitted");
        assertTrue(alerts.stream().allMatch(alert -> rule.getRuleId().equals(alert.getRuleId())));
        assertTrue(alerts.stream().allMatch(alert -> RiskRule.RuleType.ORDER_BRUSH.equals(alert.getRuleType())));
    }

    /** 测试规则热更新 场景：先加载规则 A，然后更新为规则 B */
    @Test
    @DisplayName("Test dynamic rule update")
    void testDynamicRuleUpdate() throws Exception {
        RiskRule ruleV1 = RiskRule.builder().ruleId("rule-dynamic-001").ruleName("Dynamic Rule V1")
                .ruleType(RiskRule.RuleType.HIGH_FREQ_ACCESS).targetActionType("VIEW").windowSizeMs(60_000)
                .threshold(10).groupKeyType(RiskRule.GroupKeyType.BY_USER_ID).version(1).build();

        RiskRule ruleV2 = RiskRule.builder().ruleId("rule-dynamic-001").ruleName("Dynamic Rule V2")
                .ruleType(RiskRule.RuleType.HIGH_FREQ_ACCESS).targetActionType("VIEW").windowSizeMs(60_000).threshold(5)
                .groupKeyType(RiskRule.GroupKeyType.BY_USER_ID).version(2).build();

        List<RiskRule> rules = new ArrayList<>();
        rules.add(ruleV1);
        rules.add(ruleV2);

        long baseTime = System.currentTimeMillis();
        List<UserBehavior> events = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            events.add(createViewEvent("user-test", "192.168.1.1", baseTime + i * 5_000));
        }

        DataStream<AlertEvent> alertStream = buildTestPipelineWithMultipleRules(env, events, rules);
        alertStream.addSink(new CollectSink());
        env.execute("Dynamic Rule Update Test");
        List<AlertEvent> alerts = CollectSink.getValues();
        assertFalse(alerts.isEmpty(), "Expected at least one alert after rule hot update");
        assertTrue(alerts.stream().allMatch(alert -> "rule-dynamic-001".equals(alert.getRuleId())));
        assertTrue(alerts.stream().allMatch(alert -> alert.getMatchCount() >= 5));
    }

    /** 测试无效事件过滤 */
    @Test
    @DisplayName("Test invalid event filtering")
    void testInvalidEventFiltering() throws Exception {
        RiskRule rule = createCredentialStuffingRule();

        long baseTime = System.currentTimeMillis();
        List<UserBehavior> events = new ArrayList<>();

        events.add(new UserBehavior(null, "LOGIN_FAIL", "192.168.1.1", baseTime));
        events.add(new UserBehavior("user1", null, "192.168.1.1", baseTime + 1000));
        events.add(new UserBehavior("user2", "LOGIN_FAIL", "192.168.1.1", 0));

        events.add(createLoginFailEvent("user3", "192.168.1.100", baseTime + 2000));

        DataStream<AlertEvent> alertStream = buildTestPipeline(env, events, rule);
        alertStream.addSink(new CollectSink());

        env.execute("Invalid Event Filtering Test");

        List<AlertEvent> alerts = CollectSink.getValues();
        assertTrue(alerts.isEmpty(), "Invalid events should be filtered out before detection");
    }

    /** 测试跨窗口事件不触发告警 */
    @Test
    @DisplayName("Test events outside window should not trigger alert")
    void testEventsOutsideWindow() throws Exception {
        RiskRule rule = createCredentialStuffingRule();

        long baseTime = System.currentTimeMillis();
        List<UserBehavior> events = new ArrayList<>();

        events.add(createLoginFailEvent("user1", "192.168.1.100", baseTime));
        events.add(createLoginFailEvent("user2", "192.168.1.100", baseTime + 120_000));
        events.add(createLoginFailEvent("user3", "192.168.1.100", baseTime + 240_000));

        DataStream<AlertEvent> alertStream = buildDetectorOnlyPipeline(env, events, rule);
        alertStream.addSink(new CollectSink());

        env.execute("Events Outside Window Test");

        List<AlertEvent> alerts = CollectSink.getValues();
        assertTrue(alerts.isEmpty(), "Events spread across windows should not trigger alert");
    }

    @Test
    @DisplayName("Test payment fraud detection")
    void testPaymentFraudDetection() throws Exception {
        RiskRule rule = RiskRule.builder().ruleId("rule-payment-001").ruleName("Payment Fraud")
                .ruleType(RiskRule.RuleType.PAYMENT_FRAUD).targetActionType("PAYMENT_FRAUD").windowSizeMs(60_000)
                .threshold(3).groupKeyType(RiskRule.GroupKeyType.BY_USER_ID).severityWeight(2.5).scoreThreshold(2.5)
                .version(1).build();

        long baseTime = System.currentTimeMillis();
        List<UserBehavior> events = new ArrayList<>();
        events.add(createLoginSuccessEvent("user-pay", "10.0.0.1", baseTime));
        events.add(createPaymentEvent("user-pay", baseTime + 2_000, 12.0));
        events.add(createPaymentEvent("user-pay", baseTime + 5_000, 25.0));
        events.add(createPaymentEvent("user-pay", baseTime + 8_000, 36.0));
        events.add(createPaymentFraudEvent("user-pay", baseTime + 12_000, 1500.0));

        DataStream<AlertEvent> alertStream = buildDetectorOnlyPipeline(env, events, rule);
        alertStream.addSink(new CollectSink());

        env.execute("Payment Fraud Test");

        List<AlertEvent> alerts = CollectSink.getValues();
        assertFalse(alerts.isEmpty(), "Expected payment fraud alert to be emitted");
        AlertEvent alert = alerts.get(0);
        assertEquals(rule.getRuleId(), alert.getRuleId());
        assertEquals(RiskRule.RuleType.PAYMENT_FRAUD, alert.getRuleType());
        assertTrue(alert.getMatchCount() >= 5);
    }

    @Test
    @DisplayName("Payment fraud without remote login should not trigger alert")
    void testPaymentFraudWithoutRemoteLogin() throws Exception {
        RiskRule rule = RiskRule.builder().ruleId("rule-payment-002").ruleName("Payment Fraud")
                .ruleType(RiskRule.RuleType.PAYMENT_FRAUD).targetActionType("PAYMENT_FRAUD").windowSizeMs(60_000)
                .threshold(3).groupKeyType(RiskRule.GroupKeyType.BY_USER_ID).severityWeight(2.5).scoreThreshold(2.5)
                .version(1).build();

        long baseTime = System.currentTimeMillis();
        List<UserBehavior> events = new ArrayList<>();
        events.add(createPaymentEvent("user-pay-2", baseTime, 12.0));
        events.add(createPaymentEvent("user-pay-2", baseTime + 5_000, 25.0));
        events.add(createPaymentEvent("user-pay-2", baseTime + 8_000, 36.0));
        events.add(createPaymentFraudEvent("user-pay-2", baseTime + 12_000, 1500.0));

        DataStream<AlertEvent> alertStream = buildDetectorOnlyPipeline(env, events, rule);
        alertStream.addSink(new CollectSink());

        env.execute("Payment Fraud Without Remote Login Test");

        assertTrue(CollectSink.getValues().isEmpty(), "Same-IP payments without remote login should not alert");
    }

    @Test
    @DisplayName("Test out-of-order credential stuffing still triggers alert")
    void testOutOfOrderCredentialStuffingDetection() throws Exception {
        RiskRule rule = createCredentialStuffingRule();
        long baseTime = System.currentTimeMillis();
        List<UserBehavior> events = new ArrayList<>();
        events.add(createLoginFailEvent("late-user-2", "192.168.1.101", baseTime + 20_000));
        events.add(createLoginFailEvent("late-user-3", "192.168.1.101", baseTime + 30_000));
        events.add(createLoginFailEvent("late-user-1", "192.168.1.101", baseTime + 10_000));

        DataStream<AlertEvent> alertStream = buildDetectorOnlyPipeline(env, events, rule);
        alertStream.addSink(new CollectSink());

        env.execute("Out Of Order Credential Stuffing Test");

        List<AlertEvent> alerts = CollectSink.getValues();
        assertFalse(alerts.isEmpty(), "Expected alert even when fail events arrive out of order");
        assertEquals(RiskRule.RuleType.CREDENTIAL_STUFFING, alerts.get(0).getRuleType());
    }

    /** 构建测试处理管道 */
    private DataStream<AlertEvent> buildTestPipeline(StreamExecutionEnvironment env, List<UserBehavior> events,
            RiskRule rule) {

        return buildTestPipelineWithMultipleRules(env, events, Collections.singletonList(rule));
    }

    private DataStream<AlertEvent> buildDetectorOnlyPipeline(StreamExecutionEnvironment env, List<UserBehavior> events,
            RiskRule rule) {
        WatermarkStrategy<Tuple2<UserBehavior, RiskRule>> watermarkStrategy = WatermarkStrategy
                .<Tuple2<UserBehavior, RiskRule>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((pair, ts) -> pair != null && pair.f0 != null ? pair.f0.getTimestamp() : 0L);

        List<Tuple2<UserBehavior, RiskRule>> tuples = events.stream().filter(event -> event != null && event.isValid())
                .map(event -> Tuple2.of(event, rule)).collect(Collectors.toList());

        SingleOutputStreamOperator<Tuple2<UserBehavior, RiskRule>> matchedStream = env.fromCollection(tuples)
                .assignTimestampsAndWatermarks(watermarkStrategy);
        return AbnormalPatternDetector.buildAlertStream(matchedStream, KeySelectorFactory.createKeySelector());
    }

    /** 构建支持多规则的测试管道 */
    private DataStream<AlertEvent> buildTestPipelineWithMultipleRules(StreamExecutionEnvironment env,
            List<UserBehavior> events, List<RiskRule> rules) {

        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((event, ts) -> event != null ? event.getTimestamp() : 0);

        SingleOutputStreamOperator<UserBehavior> behaviorStream = env.fromCollection(events)
                .assignTimestampsAndWatermarks(watermarkStrategy).filter(event -> event != null && event.isValid());

        DataStream<RiskRule> ruleStream = env.fromCollection(rules);

        BroadcastStream<RiskRule> broadcastRuleStream = ruleStream
                .broadcast(DynamicRuleProcessor.RULE_STATE_DESCRIPTOR);

        SingleOutputStreamOperator<Tuple2<UserBehavior, RiskRule>> matchedStream = behaviorStream
                .connect(broadcastRuleStream).process(new DynamicRuleProcessor());

        return AbnormalPatternDetector.buildAlertStream(matchedStream, KeySelectorFactory.createKeySelector());
    }

    private RiskRule createCredentialStuffingRule() {
        return RiskRule.builder().ruleId("rule-001").ruleName("Credential Stuffing Detection")
                .ruleType(RiskRule.RuleType.CREDENTIAL_STUFFING).targetActionType("LOGIN_FAIL").windowSizeMs(60_000)
                .threshold(3).groupKeyType(RiskRule.GroupKeyType.BY_IP).version(1).build();
    }

    private RiskRule createOrderBrushRule() {
        return RiskRule.builder().ruleId("rule-002").ruleName("Order Brush Detection")
                .ruleType(RiskRule.RuleType.ORDER_BRUSH).targetActionType("ORDER").windowSizeMs(60_000).threshold(5)
                .groupKeyType(RiskRule.GroupKeyType.BY_USER_ID).version(1).build();
    }

    private UserBehavior createLoginFailEvent(String userId, String ip, long timestamp) {
        return UserBehavior.builder().userId(userId).actionType("LOGIN_FAIL").ip(ip).timestamp(timestamp).build();
    }

    private UserBehavior createOrderEvent(String userId, String ip, long timestamp) {
        return UserBehavior.builder().userId(userId).actionType("ORDER").ip(ip).timestamp(timestamp).build();
    }

    private UserBehavior createViewEvent(String userId, String ip, long timestamp) {
        return UserBehavior.builder().userId(userId).actionType("VIEW").ip(ip).timestamp(timestamp).build();
    }

    private UserBehavior createLoginSuccessEvent(String userId, String ip, long timestamp) {
        return UserBehavior.builder().userId(userId).actionType("LOGIN_SUCCESS").ip(ip).timestamp(timestamp).build();
    }

    private UserBehavior createPaymentEvent(String userId, long timestamp, double amount) {
        return UserBehavior.builder().userId(userId).actionType("PAYMENT").ip("10.0.0.8").timestamp(timestamp)
                .amount(amount).build();
    }

    private UserBehavior createPaymentFraudEvent(String userId, long timestamp, double amount) {
        return UserBehavior.builder().userId(userId).actionType("PAYMENT_FRAUD").ip("10.0.0.8").timestamp(timestamp)
                .amount(amount).build();
    }

    /** 用于收集测试输出的 Sink */
    private static class CollectSink implements SinkFunction<AlertEvent> {
        private static final List<AlertEvent> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(AlertEvent value, Context context) {
            log.info("CollectSink received: {}", value);
            VALUES.add(value);
        }

        public static List<AlertEvent> getValues() {
            return new ArrayList<>(VALUES);
        }

        public static void clear() {
            VALUES.clear();
        }
    }
}
