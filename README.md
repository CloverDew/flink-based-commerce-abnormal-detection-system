# 电商异常用户行为实时检测系统

基于 Apache Flink 的实时风控检测系统，用于识别电商平台中的异常用户行为（如撞库攻击、刷单、异常登录等）。

## 系统架构

```
Kaggle CSV
   │
   ├─(KaggleCsvBootstrapTool)──────────────► Kafka: user-behavior
   │
   └─(KaggleRuleTemplateGenerator + JsonFileKafkaPublisher)► Kafka: risk-rules
                                                   │
                                                   ▼
                                  ┌─────────────────────────────────────┐
                                  │ Flink Job (DataStream + Broadcast) │
                                  │ - Event Time + Watermark           │
                                  │ - DynamicRuleProcessor             │
                                  │ - AbnormalPatternDetector          │
                                  └────────────────┬────────────────────┘
                                                   │
                                                   ▼
                                         Kafka: alerts / PrintSink

Runtime Config:
- docker/conf/bootstrap.conf    (数据导入参数)
- docker/conf/flink-job.conf    (并行度、状态后端、checkpoint 参数)
```

## 输出与反馈层（告警推送 + 实时看板 + 特征缓存）

本项目支持一套“三位一体”的反馈机制：

- **告警推送**：Flink 任务将 `AlertEvent` 输出到 Kafka `alerts` topic（或直接 PrintSink）。
- **实时看板（ClickHouse + Grafana）**：ClickHouse 通过 Kafka Engine 订阅 `alerts` topic，写入明细表与聚合表；Grafana 读取 ClickHouse 展示吞吐、类型占比、Top 攻击源等面板。
- **特征缓存（Redis）**：可选组件。提供 `AlertFeatureCacheConsumer`，从 Kafka `alerts` 消费告警，把 `AlertEvent.extra`（特征 JSON）按用户/IP 写入 Redis（带 TTL），便于业务侧快速查询最近特征快照。

### 一键启动（含 ClickHouse/Grafana/Redis）

```bash
docker compose up -d zookeeper kafka clickhouse grafana redis jobmanager taskmanager
```

访问：

- Flink Web UI：`http://localhost:8081`
- Grafana：`http://localhost:3000`（默认 `admin/admin`）
- ClickHouse HTTP：`http://localhost:8123`

说明：

- ClickHouse 初始化 SQL 位于 `docker/clickhouse/init/`，会自动创建 `risk.alerts`、`risk.alerts_agg_1m` 等表，并从 Kafka `alerts` topic 实时入库。
- Grafana 会自动安装 ClickHouse 数据源插件并加载内置看板（`docker/grafana/dashboards/`）。

### 启动特征缓存消费者（可选）

在本机（或容器内）先构建 jar：

```bash
mvn -DskipTests package
```

然后启动 Redis 特征缓存消费者：

```bash
java -cp target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar \
  cn.edu.ustb.detection.tools.AlertFeatureCacheConsumer \
  --kafka-bootstrap localhost:9092 \
  --kafka-topic alerts \
  --redis-host localhost \
  --redis-port 6379 \
  --ttl-seconds 1800
```

## 核心功能

### 1. 数据接入与时间语义
- 从 Kafka 读取用户行为日志（JSON 格式）
- 基于 Event Time 处理
- 配置 5 秒乱序容忍的 Watermark 策略
- 支持 1 分钟空闲超时处理
- 内置 Kaggle 常见字段兼容（如 `user_id`、`event_type`、`event_time`、`user_session`、`TransactionDT`）

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

## Kaggle 数据源建议（可直接用于本项目）

下面这些数据集最贴近“电商用户行为异常检测”：

- [E-commerce Clickstream and Transaction Dataset](https://www.kaggle.com/datasets/waqi786/e-commerce-clickstream-and-transaction-dataset)
- [E-commerce User Behavior and Transaction Dataset](https://www.kaggle.com/datasets/ziya07/e-commerce-user-behavior-and-transaction-dataset)
- [E-commerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
- [Clickstream Data for Online Shopping](https://www.kaggle.com/datasets/tunguz/clickstream-data-for-online-shopping)
- [IEEE-CIS Fraud Detection](https://www.kaggle.com/c/ieee-fraud-detection/data)

## 本项目采用的数据集（推荐）

为了最容易跑通并复现实验，建议优先使用：

- [E-commerce behavior data from multi category store](https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)

最符合本项目的一条直达链接（建议优先下载这个）：

- **Kaggle 数据集直达**：<https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store>
- 下载后建议放到：`./data/kaggle-events.csv`

对应导入 profile：

- `PROFILE=multi_category`

原因：

- 字段与项目模型最贴近（`event_time`、`event_type`、`user_id`、`user_session`、`product_id`）
- 能稳定触发至少 3 类异常规则（登录失败突增/高频下单/高频加购或浏览）
- 不需要额外手工清洗即可进入 Kafka

### 字段映射（已在代码中兼容）

本项目内部标准字段：

- `userId`
- `actionType`
- `timestamp`（毫秒）
- `ip`
- `sessionId`
- `productId`
- `amount`

Kaggle 常见字段自动映射关系：

- `user_id -> userId`
- `event_type -> actionType`（自动转大写）
- `event_time -> timestamp`（支持 ISO 字符串、秒/毫秒时间戳）
- `user_session / session_id -> sessionId`
- `product_id -> productId`
- `ip_address -> ip`
- `TransactionDT -> timestamp`（按 IEEE-CIS 参考起点换算为绝对时间）

### 推荐接入方式

1. 先把 Kaggle CSV 转成 JSON（每行一个 JSON）
2. 推送到 `user-behavior` Kafka Topic
3. 启动本项目主任务，直接消费并检测

已内置工具类：`cn.edu.ustb.detection.tools.KaggleCsvBootstrapTool`

支持能力：

- 自动识别常见 Kaggle profile（`auto`）
- 指定 profile（`multi_category` / `clickstream` / `ieee_cis`）
- 输出到 JSONL 文件
- 直接推送到 Kafka `user-behavior` Topic

规则模板生成器：`cn.edu.ustb.detection.tools.KaggleRuleTemplateGenerator`

- 按数据 profile 生成可直接下发的 `risk-rules` JSON
- 适配 `multi_category` / `clickstream` / `ieee_cis`
- 可通过 `version` 参数控制规则版本号

示例（CSV -> JSONL）：

```bash
java -cp target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar \
  cn.edu.ustb.detection.tools.KaggleCsvBootstrapTool \
  --input D:/data/kaggle-events.csv \
  --output-jsonl D:/data/user-behavior.jsonl \
  --profile auto
```

示例（CSV -> Kafka）：

```bash
java -cp target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar \
  cn.edu.ustb.detection.tools.KaggleCsvBootstrapTool \
  --input D:/data/kaggle-events.csv \
  --kafka-bootstrap localhost:9092 \
  --kafka-topic user-behavior \
  --profile auto
```

示例（CSV -> JSONL + Kafka 同时）：

```bash
java -cp target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar \
  cn.edu.ustb.detection.tools.KaggleCsvBootstrapTool \
  --input D:/data/kaggle-events.csv \
  --output-jsonl D:/data/user-behavior.jsonl \
  --kafka-bootstrap localhost:9092 \
  --kafka-topic user-behavior \
  --profile multi_category \
  --max-rows 200000
```

示例：

```bash
java -cp target/flink-based-commerce-abnormal-detection-system-1.0-SNAPSHOT.jar \
  cn.edu.ustb.detection.tools.KaggleRuleTemplateGenerator \
  --profile multi_category \
  --version 2 \
  --output samples/generated-risk-rules.json
```

生成后你可以直接把 `samples/generated-risk-rules.json` 中每行（或每个对象）写入 `risk-rules` topic 做热更新。

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

## Docker 运行

已提供 `docker-compose.yml` 与 `docker/scripts/*.sh`，数据导入和规则下发都可在容器内执行。

### 1) 启动基础组件（Kafka + Flink）

```bash
docker compose up -d zookeeper kafka jobmanager taskmanager
```

### 2) 在 Docker 内构建 Jar

```bash
docker compose run --rm runner bash /workspace/docker/scripts/build-jar.sh
```

### 3) 准备配置文件（推荐）

编辑：

- `docker/conf/flink-job.conf`
- `docker/conf/bootstrap.conf`

最常用参数：

- `PROFILE=multi_category`
- `INPUT_CSV=/workspace/data/kaggle-events.csv`
- `PARALLELISM=2`
- `STATE_BACKEND=hashmap`（或 `rocksdb`）

### 4) 在 Docker 内提交 Flink 任务（自动读取 .conf）

```bash
docker compose run --rm --entrypoint /bin/bash jobmanager \
  -lc "/workspace/docker/scripts/submit-flink-job.sh"
```

### 5) 在 Docker 内导入 Kaggle 数据 + 下发规则（自动读取 .conf）

```bash
docker compose run --rm runner \
  bash /workspace/docker/scripts/bootstrap-kaggle.sh
```

### 6) 一键端到端（构建 + 提交任务 + 导数）

```bash
docker compose run --rm --entrypoint /bin/bash jobmanager \
  -lc "/workspace/docker/scripts/run-e2e.sh"
```

### 最短可执行路径（建议直接用）

```bash
# 0. 准备数据
# 把 Kaggle CSV 放到 ./data/kaggle-events.csv

# 1. 启动基础服务
docker compose up -d zookeeper kafka jobmanager taskmanager

# 2. 构建
docker compose run --rm runner bash /workspace/docker/scripts/build-jar.sh

# 3. 提交 Flink 任务
docker compose run --rm --entrypoint /bin/bash jobmanager -lc "/workspace/docker/scripts/submit-flink-job.sh"

# 4. 导入规则 + 行为数据
docker compose run --rm runner bash /workspace/docker/scripts/bootstrap-kaggle.sh
```

### 脚本说明

- `docker/scripts/build-jar.sh`：容器内构建 fat jar
- `docker/scripts/submit-flink-job.sh`：提交 Flink 作业
- `docker/scripts/bootstrap-kaggle.sh`：生成规则模板、发布规则、导入 Kaggle 行为数据
- `docker/scripts/run-e2e.sh`：一键串行执行

### `.conf` 配置化启动（推荐）

已提供两个配置文件：

- `docker/conf/flink-job.conf`：并行度、状态后端、checkpoint 参数
- `docker/conf/bootstrap.conf`：数据导入 profile、输入文件、规则版本等

脚本会在启动时自动加载：

- `submit-flink-job.sh` 默认加载 `docker/conf/flink-job.conf`
- `bootstrap-kaggle.sh` 默认加载 `docker/conf/bootstrap.conf`

你也可以指定其他配置文件：

```bash
docker compose run --rm --entrypoint /bin/bash \
  -e CONF_FILE=/workspace/docker/conf/flink-job.conf \
  jobmanager -lc "/workspace/docker/scripts/submit-flink-job.sh"
```

## 参数影响与观测建议

当前框架基于“规则 + 时间窗口计数”模型，稳定可覆盖至少 3 类异常（如登录失败突增、高频下单、高频加购/浏览）。

### 调参建议

- `PARALLELISM`：提高可提升吞吐，但会增加资源占用和 checkpoint 开销
- `STATE_BACKEND=rocksdb`：大状态更稳，延迟略升；`hashmap` 延迟更低但内存压力更大
- `CHECKPOINT_INTERVAL_MS`：更短恢复点更密集，但 I/O 开销更高
- `CHECKPOINT_TIMEOUT_MS`：过小会导致频繁 checkpoint 失败
- `CHECKPOINT_UNALIGNED_ENABLED=true`：高背压时可提升 checkpoint 成功率

### 建议重点观察指标（CK/Checkpoint）

- Checkpoint Duration（持续时间）
- Checkpoint Failed / Completed 次数
- End-to-End Latency
- Backpressure 状态
- Kafka consumer lag（规则流与行为流）

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
- **容器化端到端测试**: 使用 Testcontainers 动态拉起 Kafka，直接观测告警结果

测试场景覆盖：
- 撞库攻击检测
- 刷单行为检测
- 规则动态更新
- 无效数据过滤
- 窗口边界处理

### Testcontainers 端到端调试

如果你不想手动进入 Docker 容器，可直接运行这个测试：

```bash
mvn -Dit.testcontainers=true -Dtest=KafkaE2ETestcontainersTest test
```

该测试会自动：

1. 启动 Kafka Testcontainer
2. 启动 Flink 流作业（本地进程）
3. 写入规则和行为事件到 Kafka
4. 从 `tc-alerts` 主题消费并断言告警是否触发

前提：本机 Docker Desktop 可用。

说明：该测试默认不会在普通 `mvn test` 中执行，避免影响日常开发速度；按上面的命令显式开启即可。

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
