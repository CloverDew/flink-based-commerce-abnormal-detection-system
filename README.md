# 电商异常用户行为实时检测系统

基于 Apache Flink 的实时风控检测系统，用于识别电商平台中的异常用户行为（如撞库攻击、刷单、异常登录等）。

## 系统架构

```
┌─────────────────┐    ┌─────────────────┐
│  User Behavior  │    │   Risk Rules    │
│  Kafka Topic    │    │  Kafka Topic    │
└────────┬────────┘    └────────┬────────┘
         │                      │
         ▼                      ▼
┌─────────────────────────────────────────┐
│           Flink DataStream Job          │
│  ┌───────────────────────────────────┐  │
│  │   Watermark & Event Time Setup    │  │
│  └───────────────────────────────────┘  │
│                    │                    │
│  ┌─────────────────┼─────────────────┐  │
│  │     Broadcast State (Rules)       │  │
│  │  ┌─────────────────────────────┐  │  │
│  │  │   DynamicRuleProcessor      │  │  │
│  │  │   (Rule Hot Reload)         │  │  │
│  │  └─────────────────────────────┘  │  │
│  └───────────────────────────────────┘  │
│                    │                    │
│  ┌───────────────────────────────────┐  │
│  │   AbnormalPatternDetector         │  │
│  │   (Keyed State + Timer)           │  │
│  └───────────────────────────────────┘  │
└────────────────────┬────────────────────┘
                     │
                     ▼
          ┌─────────────────┐
          │  Alert Events   │
          │  (Kafka/Print)  │
          └─────────────────┘
```

## 核心功能

### 1. 数据接入与时间语义
- 从 Kafka 读取用户行为日志（JSON 格式）
- 基于 Event Time 处理
- 配置 5 秒乱序容忍的 Watermark 策略
- 支持 1 分钟空闲超时处理

### 2. 动态规则加载
- 使用 Broadcast State 模式实现规则热加载
- 支持规则版本控制，自动忽略旧版本规则
- 支持规则启用/禁用状态切换
- 规则变更实时生效，无需重启任务

### 3. 异常模式匹配

支持的规则类型：
| 规则类型 | 说明 | 典型场景 |
|---------|------|---------|
| CREDENTIAL_STUFFING | 撞库攻击 | 同一 IP 短时间内多次登录失败 |
| ORDER_BRUSH | 刷单 | 同一用户高频下单 |
| ABNORMAL_LOGIN | 异常登录 | 登录后立即进行敏感操作 |
| HIGH_FREQ_ACCESS | 高频访问 | 短时间内大量请求 |
| PAYMENT_FRAUD | 支付欺诈 | 多次小额支付后大额支付 |

### 4. 告警输出
- 支持 PrintSink（控制台输出）
- 支持 Kafka Sink（生产环境）
- 告警包含完整的上下文信息

## 数据模型

### UserBehavior（用户行为事件）
```json
{
  "userId": "user123",
  "actionType": "LOGIN_FAIL",
  "ip": "192.168.1.100",
  "timestamp": 1700000000000,
  "sessionId": "session-abc",
  "deviceId": "device-xyz",
  "productId": "prod-001",
  "amount": 99.99
}
```

### RiskRule（风控规则）
```json
{
  "ruleId": "rule-001",
  "ruleName": "撞库攻击检测",
  "ruleType": "CREDENTIAL_STUFFING",
  "status": "ENABLED",
  "targetActionType": "LOGIN_FAIL",
  "windowSizeMs": 60000,
  "threshold": 3,
  "groupKeyType": "BY_IP",
  "priority": 10,
  "version": 1
}
```

### AlertEvent（告警事件）
```json
{
  "alertId": "uuid",
  "ruleId": "rule-001",
  "ruleType": "CREDENTIAL_STUFFING",
  "level": "HIGH",
  "userId": "user123",
  "ip": "192.168.1.100",
  "matchCount": 5,
  "alertTimestamp": 1700000000000,
  "message": "检测到异常行为..."
}
```

## 快速开始

### 环境要求
- JDK 11+
- Maven 3.6+
- Kafka 2.8+（可选，测试可用内存模式）

### 编译打包
```bash
mvn clean package -DskipTests
```

### 运行测试
```bash
mvn test
```

### 本地运行（Print Sink）
```bash
# 直接运行（使用默认配置）
mvn exec:java -Dexec.mainClass="cn.edu.ustb.detection.AbnormalBehaviorDetectionJob"

# 或使用打包后的 JAR
java -jar target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar
```

### 集群运行
```bash
flink run -c cn.edu.ustb.detection.AbnormalBehaviorDetectionJob \
  target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar \
  --kafka.bootstrap.servers kafka:9092 \
  --kafka.behavior.topic user-behavior \
  --kafka.rule.topic risk-rules \
  --kafka.alert.topic alerts \
  --kafka.sink.enabled true \
  --parallelism 4
```

## 配置参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| kafka.bootstrap.servers | localhost:9092 | Kafka 集群地址 |
| kafka.behavior.topic | user-behavior | 用户行为 Topic |
| kafka.rule.topic | risk-rules | 规则配置 Topic |
| kafka.alert.topic | alerts | 告警输出 Topic |
| kafka.group.id | flink-detection-group | 消费者组 ID |
| kafka.sink.enabled | false | 是否启用 Kafka 告警输出 |
| parallelism | CPU核心数 | 任务并行度 |

## 项目结构

```
src/
├── main/java/cn/edu/ustb/detection/
│   ├── AbnormalBehaviorDetectionJob.java  # 主任务入口
│   ├── model/
│   │   ├── UserBehavior.java              # 用户行为事件
│   │   ├── RiskRule.java                  # 风控规则
│   │   └── AlertEvent.java                # 告警事件
│   ├── serialization/
│   │   ├── JsonDeserializationSchema.java # JSON 反序列化器
│   │   └── AlertEventSerializationSchema.java # 告警序列化器
│   ├── processor/
│   │   └── DynamicRuleProcessor.java      # 动态规则处理器
│   ├── cep/
│   │   ├── AbnormalPatternDetector.java   # 异常模式检测器
│   │   └── CepPatternFactory.java         # CEP 模式工厂
│   └── util/
│       └── KeySelectorFactory.java        # 键选择器工厂
├── main/resources/
│   └── log4j2.xml                         # 日志配置
└── test/java/cn/edu/ustb/detection/
    ├── AbnormalBehaviorDetectionJobTest.java # 集成测试
    ├── model/ModelTest.java               # 模型测试
    └── serialization/SerializationTest.java # 序列化测试
```

## 扩展指南

### 添加新的规则类型

1. 在 `RiskRule.RuleType` 枚举中添加新类型
2. 在 `CepPatternFactory` 中实现对应的 Pattern
3. 更新 `AbnormalPatternDetector` 中的处理逻辑（如需要）

### 添加新的分组维度

1. 在 `RiskRule.GroupKeyType` 枚举中添加新类型
2. 在 `KeySelectorFactory.createKeySelector()` 中添加对应的 case

### 自定义告警输出

实现 `SinkFunction<AlertEvent>` 接口，可以对接：
- 数据库（MySQL、ClickHouse 等）
- 消息队列（RabbitMQ、RocketMQ 等）
- 告警平台（钉钉、企业微信等）

## 测试说明

项目包含完整的测试骨架：

- **单元测试**: 数据模型、序列化器的测试
- **集成测试**: 使用 Flink MiniCluster 的端到端测试

测试场景覆盖：
- 撞库攻击检测
- 刷单行为检测
- 规则动态更新
- 无效数据过滤
- 窗口边界处理

## 注意事项

1. **生产环境部署**
   - 建议开启 Checkpoint，确保状态一致性
   - 根据数据量调整并行度和资源配置
   - 监控 Kafka 消费延迟

2. **规则配置**
   - 规则版本号递增，确保更新生效
   - 合理设置时间窗口和阈值，避免误报
   - 测试环境验证规则后再上线

3. **性能优化**
   - 大规模场景考虑使用 RocksDB 状态后端
   - 合理设置 Watermark 间隔
   - 监控 Backpressure 情况

## License

MIT License
