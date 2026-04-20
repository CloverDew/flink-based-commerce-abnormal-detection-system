package cn.edu.ustb.detection;

import cn.edu.ustb.detection.cep.AbnormalPatternDetector;
import cn.edu.ustb.detection.model.AlertEvent;
import cn.edu.ustb.detection.model.RiskRule;
import cn.edu.ustb.detection.model.UserBehavior;
import cn.edu.ustb.detection.processor.DynamicRuleProcessor;
import cn.edu.ustb.detection.serialization.AlertEventSerializationSchema;
import cn.edu.ustb.detection.serialization.JsonDeserializationSchema;
import cn.edu.ustb.detection.util.KeySelectorFactory;
import java.time.Duration;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 电商异常用户行为实时检测系统 - 主任务入口
 *
 * <p>
 * 系统架构： 1. 用户行为事件流 (Kafka: user-behavior-topic) ↓ 2. Watermark 分配（Event Time +
 * 乱序处理） ↓ 3. 与规则流（Broadcast State）连接 ↓ 4. 动态规则匹配（DynamicRuleProcessor） ↓ 5.
 * 异常模式检测（AbnormalPatternDetector） ↓ 6. 告警输出 (Kafka: alert-topic / Print)
 *
 * <p>
 * 启动参数示例： --kafka.bootstrap.servers localhost:9092 --kafka.behavior.topic
 * user-behavior --kafka.rule.topic risk-rules --kafka.alert.topic alerts
 * --kafka.group.id flink-detection-group
 */
public class AbnormalBehaviorDetectionJob {

    private static final Logger LOG = LoggerFactory.getLogger(AbnormalBehaviorDetectionJob.class);

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String DEFAULT_BEHAVIOR_TOPIC = "user-behavior";
    private static final String DEFAULT_RULE_TOPIC = "risk-rules";
    private static final String DEFAULT_ALERT_TOPIC = "alerts";
    private static final String DEFAULT_GROUP_ID = "flink-detection-group";

    public static void main(String[] args) throws Exception {
        LOG.info("Starting Abnormal Behavior Detection Job...");

        ParameterTool params = ParameterTool.fromArgs(args);

        String bootstrapServers = params.get("kafka.bootstrap.servers", DEFAULT_BOOTSTRAP_SERVERS);
        String behaviorTopic = params.get("kafka.behavior.topic", DEFAULT_BEHAVIOR_TOPIC);
        String ruleTopic = params.get("kafka.rule.topic", DEFAULT_RULE_TOPIC);
        String alertTopic = params.get("kafka.alert.topic", DEFAULT_ALERT_TOPIC);
        String groupId = params.get("kafka.group.id", DEFAULT_GROUP_ID);
        boolean enableKafkaSink = params.getBoolean("kafka.sink.enabled", false);

        LOG.info("Configuration - bootstrapServers: {}, behaviorTopic: {}, ruleTopic: {}, alertTopic: {}, groupId: {}",
                bootstrapServers, behaviorTopic, ruleTopic, alertTopic, groupId);

        StreamExecutionEnvironment env = createExecutionEnvironment(params);

        DataStream<AlertEvent> alertStream = buildPipeline(env, bootstrapServers, behaviorTopic, ruleTopic, groupId);

        if (enableKafkaSink) {
            KafkaSink<AlertEvent> kafkaSink = createKafkaSink(bootstrapServers, alertTopic);
            alertStream.sinkTo(kafkaSink).name("Kafka Alert Sink");
            LOG.info("Alert sink configured: Kafka topic={}", alertTopic);
        } else {
            alertStream.print().name("Print Alert Sink");
            LOG.info("Alert sink configured: Console output");
        }

        env.execute("E-commerce Abnormal Behavior Detection");
    }

    /** 创建并配置 Flink 执行环境 */
    public static StreamExecutionEnvironment createExecutionEnvironment(ParameterTool params) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        int parallelism = params.getInt("parallelism", Runtime.getRuntime().availableProcessors());
        long checkpointIntervalMs = params.getLong("checkpoint.interval.ms", 60_000L);
        long checkpointTimeoutMs = params.getLong("checkpoint.timeout.ms", 120_000L);
        long checkpointMinPauseMs = params.getLong("checkpoint.min.pause.ms", 30_000L);
        int checkpointMaxConcurrent = params.getInt("checkpoint.max.concurrent", 1);
        int checkpointTolerableFailures = params.getInt("checkpoint.tolerable.failures", 3);
        boolean checkpointUnaligned = params.getBoolean("checkpoint.unaligned.enabled", false);
        boolean externalizedRetained = params.getBoolean("checkpoint.externalized.retained", true);
        String checkpointStorage = params.get("checkpoint.storage", "");
        String stateBackend = params.get("state.backend", "hashmap");

        configureStateBackend(env, stateBackend, checkpointStorage);
        configureCheckpoint(env, checkpointIntervalMs, checkpointTimeoutMs, checkpointMinPauseMs,
                checkpointMaxConcurrent, checkpointTolerableFailures, checkpointUnaligned, externalizedRetained);
        env.setParallelism(parallelism);

        LOG.info(
                "Execution environment configured: parallelism={}, stateBackend={}, ck.interval={}ms, ck.timeout={}ms, ck.minPause={}ms",
                parallelism, stateBackend, checkpointIntervalMs, checkpointTimeoutMs, checkpointMinPauseMs);

        return env;
    }

    private static void configureStateBackend(StreamExecutionEnvironment env, String backendType,
            String checkpointStorage) {
        if ("rocksdb".equalsIgnoreCase(backendType)) {
            env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        } else {
            env.setStateBackend(new HashMapStateBackend());
        }

        if (checkpointStorage != null && !checkpointStorage.trim().isEmpty()) {
            env.getCheckpointConfig().setCheckpointStorage(checkpointStorage.trim());
            LOG.info("Configured checkpoint storage: {}", checkpointStorage);
        }
    }

    private static void configureCheckpoint(StreamExecutionEnvironment env, long intervalMs, long timeoutMs,
            long minPauseMs, int maxConcurrent, int tolerableFailures, boolean unaligned,
            boolean externalizedRetained) {
        env.enableCheckpointing(intervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(timeoutMs);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseMs);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(maxConcurrent);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(tolerableFailures);
        env.getCheckpointConfig().enableUnalignedCheckpoints(unaligned);
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(externalizedRetained
                        ? ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
                        : ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
    }

    /** 构建处理拓扑 */
    public static DataStream<AlertEvent> buildPipeline(StreamExecutionEnvironment env, String bootstrapServers,
            String behaviorTopic, String ruleTopic, String groupId) {

        KafkaSource<UserBehavior> behaviorSource = createBehaviorSource(bootstrapServers, behaviorTopic, groupId);

        KafkaSource<RiskRule> ruleSource = createRuleSource(bootstrapServers, ruleTopic, groupId);

        WatermarkStrategy<UserBehavior> watermarkStrategy = WatermarkStrategy
                .<UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>() {
                    @Override
                    public long extractTimestamp(UserBehavior event, long recordTimestamp) {
                        if (event == null || event.getTimestamp() <= 0) {
                            LOG.warn("Invalid timestamp in event, using current time: {}", event);
                            return System.currentTimeMillis();
                        }
                        return event.getTimestamp();
                    }
                }).withIdleness(Duration.ofMinutes(1));

        SingleOutputStreamOperator<UserBehavior> behaviorStream = env
                .fromSource(behaviorSource, watermarkStrategy, "Kafka Behavior Source").filter(event -> {
                    if (event == null || !event.isValid()) {
                        LOG.warn("Filtered invalid event: {}", event);
                        return false;
                    }
                    return true;
                }).name("Filter Invalid Events");

        DataStream<RiskRule> ruleStream = env
                .fromSource(ruleSource, WatermarkStrategy.noWatermarks(), "Kafka Rule Source").filter(rule -> {
                    if (rule == null) {
                        LOG.warn("Filtered null rule");
                        return false;
                    }
                    return true;
                }).name("Filter Null Rules");

        BroadcastStream<RiskRule> broadcastRuleStream = ruleStream
                .broadcast(DynamicRuleProcessor.RULE_STATE_DESCRIPTOR);

        SingleOutputStreamOperator<Tuple2<UserBehavior, RiskRule>> matchedStream = behaviorStream
                .connect(broadcastRuleStream).process(new DynamicRuleProcessor()).name("Dynamic Rule Processor");

        DataStream<AlertEvent> alertStream = matchedStream.keyBy(KeySelectorFactory.createKeySelector())
                .process(new AbnormalPatternDetector()).name("Abnormal Pattern Detector");

        LOG.info("Processing pipeline built successfully");

        return alertStream;
    }

    /** 创建用户行为事件 Kafka Source */
    private static KafkaSource<UserBehavior> createBehaviorSource(String bootstrapServers, String topic,
            String groupId) {

        return KafkaSource.<UserBehavior>builder().setBootstrapServers(bootstrapServers).setTopics(topic)
                .setGroupId(groupId).setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(UserBehavior.class))
                .setProperty("partition.discovery.interval.ms", "30000").build();
    }

    /** 创建规则流 Kafka Source */
    private static KafkaSource<RiskRule> createRuleSource(String bootstrapServers, String topic, String groupId) {

        return KafkaSource.<RiskRule>builder().setBootstrapServers(bootstrapServers).setTopics(topic)
                .setGroupId(groupId + "-rules").setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new JsonDeserializationSchema<>(RiskRule.class)).build();
    }

    /** 创建告警 Kafka Sink */
    private static KafkaSink<AlertEvent> createKafkaSink(String bootstrapServers, String topic) {
        return KafkaSink.<AlertEvent>builder().setBootstrapServers(bootstrapServers)
                .setRecordSerializer(new AlertEventSerializationSchema(topic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE).build();
    }
}
