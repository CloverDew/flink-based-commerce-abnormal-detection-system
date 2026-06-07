# 电商异常用户行为实时检测系统！！保姆级！！运行指南

基于 Apache Flink 的实时风控检测系统，用于识别电商平台中的异常用户行为，如撞库攻击、刷单、异常登录等。

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
- /opt/app/conf/bootstrap.conf  (镜像内默认数据导入参数)
- /opt/app/conf/flink-job.conf  (镜像内默认 Flink 作业参数)
- 环境变量覆盖                (运行时覆盖 Kafka/topic/checkpoint 等)
```

## 输出与反馈层

本项目支持一套“三位一体”的反馈机制：

- **告警推送**：Flink 任务将 `AlertEvent` 输出到 Kafka `alerts` topic。
- **实时看板（ClickHouse + Grafana）**：ClickHouse 通过 Kafka Engine 订阅 `alerts` topic，写入明细表与聚合表；Grafana 读取 ClickHouse 展示吞吐、类型占比、Top 攻击源等面板。
- **特征缓存（Redis）**：提供 `AlertFeatureCacheConsumer`，从 Kafka `alerts` 消费告警，把 `AlertEvent.extra`（特征 JSON）按用户/IP 写入 Redis，便于业务侧快速查询最近特征快照。

### 一键启动

```bash
docker compose up -d zookeeper kafka clickhouse grafana redis jobmanager taskmanager
```

访问：

- Flink Web UI：`http://localhost:8081`
- Grafana：`http://localhost:3000`
- ClickHouse HTTP：`http://localhost:8123`

说明：

- ClickHouse 初始化 SQL 位于 `docker/clickhouse/init/`，会自动创建 `risk.alerts`、`risk.alerts_agg_1m` 等表，并从 Kafka `alerts` topic 实时入库。
- Grafana 会自动安装 ClickHouse 数据源插件并加载内置看板。

### 启动特征缓存消费者

在本机先构建 jar：

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

### 5. 风控规则评分

系统在 CEP 模式匹配成功后，会对每条候选命中计算**风险评分**，只有评分达到规则阈值时才输出告警（见 `AbnormalPatternDetector.toAlertEvent`）。

#### 评分公式

```
riskScore = severityWeight × (1 + matchCount/threshold + min(1.5, densityPerSec/10))
```

其中：

| 因子 | 含义 |
|------|------|
| `severityWeight` | 规则严重程度权重，越大表示该类异常危害越高 |
| `matchCount / threshold` | 超出触发阈值的倍数，匹配事件越多分越高 |
| `densityPerSec` | 窗口内事件密度（events/s），`matchCount / (windowSizeMs/1000)` |
| `scoreThreshold` | 规则配置的评分门槛，**仅当 `riskScore >= scoreThreshold` 时才下发告警** |

#### 规则权重与阈值

`RiskRule` 中两个评分相关字段：

- `severityWeight`：严重程度权重，默认 `1.0`
- `scoreThreshold`：评分拦截阈值，默认 `1.0`

`KaggleRuleTemplateGenerator` 按规则类型自动填充：

| 规则类型 | severityWeight | scoreThreshold |
|---------|----------------|----------------|
| PAYMENT_FRAUD | 2.5 | 3.0 |
| CREDENTIAL_STUFFING | 2.0 | 2.5 |
| ORDER_BRUSH | 1.8 | 2.2 |
| ABNORMAL_LOGIN | 1.6 | 2.0 |
| HIGH_FREQ_ACCESS | 1.3 | 1.8 |

合成规则（`samples/run_thesis_experiments.py`）使用略低的阈值以便在可控数据上稳定触发：

| 规则类型 | severityWeight | scoreThreshold | 触发条件 |
|---------|----------------|----------------|---------|
| ABNORMAL_LOGIN | 1.6 | 1.5 | 60s 内登录 + 敏感操作 ≥ 2 次 |
| ORDER_BRUSH | 1.8 | 1.8 | 60s 内下单 ≥ 5 次 |
| HIGH_FREQ_ACCESS | 1.3 | 1.5 | 60s 内浏览 ≥ 100 次 |

#### 告警等级

`AlertEvent` 按 `matchCount / threshold` 比值划分等级：

| 比值 | 等级 |
|------|------|
| ≥ 3.0 | CRITICAL |
| ≥ 2.0 | HIGH |
| ≥ 1.0 | MEDIUM |
| < 1.0 | LOW |

## 测试数据：来源、划分与注入

本项目有**两条数据通路**，用途不同：

| 通路 | 用途 | 数据性质 |
|------|------|---------|
| **A. Kaggle 真实数据** | Docker 演示、生产形态联调 | 真实电商点击流（无显式异常标签） |
| **B. 论文合成数据** | 毕业论文对照实验、功能/性能评估 | 带标注的合成样本（`synthetic / thesis sample`） |

### A. Kaggle 真实数据

- **数据集**：<https://www.kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store>
- **本地路径**：`./data/kaggle-events.csv`（仓库不附带，需自行下载）
- **推荐切分**：原始 CSV 体积大，建议本地截取前 **10万～20万行** 做开发调试；全量导入通过 `MAX_ROWS` 控制

#### 字段映射

本项目内部标准字段：

- `userId`
- `actionType`
- `timestamp`（毫秒）
- `ip`
- `sessionId`
- `productId`
- `amount`

字段映射关系：

- `user_id -> userId`
- `event_type -> actionType`
- `event_time -> timestamp`
- `user_session / session_id -> sessionId`
- `product_id -> productId`
- `ip_address -> ip`
- `TransactionDT -> timestamp`

#### 数据划分说明

Kaggle 通路**不做传统监督学习意义上的 train/test 切分**，而是：

1. **按行数截断**：`bootstrap.conf` 中 `MAX_ROWS`（`-1` 表示全量）控制导入规模
2. **按 Profile 映射**：`multi_category` / `clickstream` / `ieee_cis` 三套字段映射与规则模板，同一 CSV 可选用不同 profile 解释
3. **按 Kafka 分区键**：行为事件以 `userId` 为 key 写入，保证同一用户事件进入同一分区，便于 Flink keyBy 聚合

#### 注入方式

```
kaggle-events.csv
    → KaggleCsvBootstrapTool
    → Kafka: user-behavior

KaggleRuleTemplateGenerator，按 PROFILE 生成规则
    → JsonFileKafkaPublisher
    → Kafka: risk-rules（key=ruleId）
```

运行脚本：`docker/scripts/bootstrap-kaggle.sh`

```bash
docker compose run --rm --entrypoint /bin/bash tools \
  -lc "/opt/app/scripts/bootstrap-kaggle.sh"
```

### B. 论文合成数据

用于毕业论文 14 组对照实验，数据在生成阶段即完成**正负样本划分**，不依赖 Kaggle 原始标签。

#### 数据来源与构成

| 组成部分 | 生成方式 | 用户 ID 前缀 | 角色 |
|---------|---------|-------------|------|
| 正常背景流 | 从 Kaggle JSONL 随机抽样，或 `fallback_normal_events` 生成 | `normal_user_*` | 负样本（不应触发告警） |
| 异常登录 | `inject_abnormal_events` 注入 | `al_user_*` / `al_u_*` | 正样本（应触发 ABNORMAL_LOGIN） |
| 高频下单 | 同上 | `ob_user_*` / `ob_u_*` | 正样本（应触发 ORDER_BRUSH） |
| 高频访问 | 同上 | `hf_user_*` / `hf_u_*` | 正样本（应触发 HIGH_FREQ_ACCESS） |

生成脚本：

```bash
# 生成 rules + events（输出到 samples/thesis-*.json / jsonl）
python samples/generate_thesis_data.py --paper-profile

# 可选：以 Kaggle 导出的 JSONL 作为正常背景
python samples/generate_thesis_data.py \
  --base-jsonl .data/user-behavior.jsonl \
  --normal-limit 30000 \
  --abnormal-user-count 220
```

输出文件：

- `samples/thesis-risk-rules.json` — 3 条论文规则
- `samples/thesis-behavior-events.jsonl` — 正常 + 异常混合事件流

#### 实验案例划分

`run_thesis_experiments.py` 将实验按**单一变量**切分为独立 case，每组使用隔离的 Kafka Topic，避免串扰：

| 前缀 | 变量 | 取值 | 固定默认值 |
|------|------|------|-----------|
| `p1`–`p8` | 并行度 | 1, 2, 4, 8 | window=60s, backend=hashmap, events=30k |
| `w30000`–`w600000` | 规则窗口 | 30s, 60s, 300s, 600s | parallelism=4, events=30k |
| `bhashmap` / `brocksdb` | 状态后端 | HashMap / RocksDB | parallelism=4, window=60s |
| `s10000`–`s200000` | 事件规模 | 1万, 5万, 10万, 20万 | parallelism=4, backend=rocksdb |

#### 评估指标如何划分正负

实验评估以**用户级**为单位：

1. **正样本（Expected）**：生成器中标记的异常用户集合（`expected_users_by_rule_type`）
2. **预测（Detected）**：Flink 告警按 `(ruleType, userId)` 去重后的用户集合
3. **TP / FP / FN**：
   - TP = 正样本 ∩ 预测
   - FP = 预测 − 正样本
   - FN = 正样本 − 预测
4. **宏平均**：三条规则各自算 P/R/F1 后取算术平均

功能验证（`export_functional_metrics.py`）按用户 ID 前缀识别异常 cohort，默认每类取 120 个用户计算 `functional_metrics.csv`。

#### 注入方式

```
generate_rules_and_events()        # 按 case 生成 rules.json + events.jsonl
    ↓
restart_job()                      # 改写 flink-job.conf，重启 Flink
    ↓
publish_rules()                    # kafka-console-producer → exp-rules-*
publish_events_and_measure()       # kafka-console-producer → exp-behavior-*（key=userId）
    ↓
Flink 检测 → exp-alerts-*
    ↓
capture_alerts()                   # 消费告警写 alerts-*.jsonl
compute_case_quality_metrics()     # TP/FP/FN → CSV + 图表
```

完整实验：

```bash
docker compose up -d zookeeper kafka jobmanager taskmanager
python samples/run_thesis_experiments.py
# 图表：.data/experiment/figures/index.html
```

验证：

```bash
python samples/verify_thesis_detection.py   # 发布 samples/thesis-* 到共享 Topic
python samples/export_functional_metrics.py \
  --events samples/thesis-behavior-events.jsonl \
  --alerts .data/alerts-captured.jsonl
```

### 接入方式

工具类支持：

- 识别常见 Kaggle profile
- 指定 profile（`multi_category` / `clickstream` / `ieee_cis`）
- 输出到 JSONL 文件
- 直接推送到 Kafka `user-behavior` Topic

规则模板生成器：`cn.edu.ustb.detection.tools.KaggleRuleTemplateGenerator`

- 按数据 profile 生成可直接下发的 `risk-rules` JSON
- 适配 `multi_category` / `clickstream` / `ieee_cis`
- 通过 `version` 参数控制规则版本号

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

示例（CSV -> JSONL + Kafka）：

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

## 数据模型

### UserBehavior
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

### RiskRule
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

### AlertEvent
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

## 环境部署

### 开发工具
- JDK 11
- Maven 3.6
- Kafka 2.8

### 编译打包
```bash
mvn clean package -DskipTests
```

### 运行测试
```bash
mvn test
```

### 本地运行，控制台打印
```bash
# 直接运行
mvn exec:java -Dexec.mainClass="cn.edu.ustb.detection.AbnormalBehaviorDetectionJob"

# 使用打包后的 JAR
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

已有本人打包的镜像，可以直接用，如果需要特殊定制需求请自行打包镜像，不会打包镜像请自行探索，readme不赘述！！！docker 镜像在仓库搜索：

| Contains                                                     | Visibility  | Scout | Visibility | Scout    |
| :----------------------------------------------------------- | :---------- | :---- | :--------- | :------- |
| [cloverdew/flink-commerce-abnormal-detection](https://hub.docker.com/repository/docker/cloverdew/flink-commerce-abnormal-detection) | 24 days ago | image | Public     | Inactive |

### 1) 如果想使用本人构建镜像，请拉取

```bash
docker pull cloverdew/flink-commerce-abnormal-detection:latest
```

### 2) 指定 Compose 使用的镜像

PowerShell:

```powershell
$env:FLINK_APP_IMAGE="cloverdew/flink-commerce-abnormal-detection:latest"
```

Bash:

```bash
export FLINK_APP_IMAGE=cloverdew/flink-commerce-abnormal-detection:latest
```

### 3) 启动基础组件

```bash
docker compose up -d zookeeper kafka jobmanager taskmanager
```

如需连同 ClickHouse / Grafana / Redis 一起启动：

```bash
docker compose up -d zookeeper kafka clickhouse grafana redis jobmanager taskmanager
```

### 4) 准备数据文件

把 Kaggle CSV 放到本地 `./data/kaggle-events.csv`即可。项目中的`tools` 服务会把它挂载到容器内 `/opt/app/data/kaggle-events.csv`。

### 5) 在 Docker 内提交 Flink 任务

```bash
docker compose run --rm --entrypoint /bin/bash tools \
  -lc "/opt/app/scripts/submit-flink-job.sh"
```

### 6) 在 Docker 内导入 Kaggle 数据并下发规则

```bash
docker compose run --rm --entrypoint /bin/bash tools \
  -lc "/opt/app/scripts/bootstrap-kaggle.sh"
```

### 7) 端到端运行脚本

```bash
docker compose run --rm --entrypoint /bin/bash tools \
  -lc "/opt/app/scripts/run-e2e.sh"
```

### 懒人 or 零技术人士执行步骤如下：

```bash
# 0. 先把 Kaggle CSV 放到 ./data/kaggle-events.csv

# 1. 告诉 Compose 使用你的镜像
export FLINK_APP_IMAGE=cloverdew/flink-commerce-abnormal-detection:latest

# 2. 启动基础服务
docker compose up -d zookeeper kafka jobmanager taskmanager

# 3. 提交 Flink 任务
docker compose run --rm --entrypoint /bin/bash tools -lc "/opt/app/scripts/submit-flink-job.sh"

# 4. 导入规则 + 行为数据
docker compose run --rm --entrypoint /bin/bash tools -lc "/opt/app/scripts/bootstrap-kaggle.sh"
```

### 镜像配置

镜像内已提供两份默认配置文件：

- `/opt/app/conf/flink-job.conf`
- `/opt/app/conf/bootstrap.conf`

脚本：

- `submit-flink-job.sh` 优先读取 `/opt/app/conf/flink-job.conf`
- `bootstrap-kaggle.sh` 优先读取 `/opt/app/conf/bootstrap.conf`
- 本地 `./docker/conf` 会挂载到 `/opt/app/conf`
- 本地 `./data` 会挂载到 `/opt/app/data`
- 本地 `./samples` 会挂载到 `/opt/app/samples`

如果确实要改目录，用运行时环境变量即可，不推荐像傻瓜一样重新构建镜像，例如：

```bash
docker compose run --rm --entrypoint /bin/bash \
  -e PARALLELISM=4 \
  -e STATE_BACKEND=rocksdb \
  -e INPUT_CSV=/opt/app/data/kaggle-events.csv \
  tools -lc "/opt/app/scripts/bootstrap-kaggle.sh"
```

### 脚本说明

- `docker/scripts/build-jar.sh`：本地源码模式下构建 fat jar
- `docker/scripts/submit-flink-job.sh`：提交 Flink 作业，使用镜像内 jar 与配置
- `docker/scripts/bootstrap-kaggle.sh`：生成规则模板、发布规则、导入 Kaggle 行为数据
- `docker/scripts/run-e2e.sh`：一键执行

## 参数与观测

当前框架基于“规则 + 时间窗口计数”模型，稳定可覆盖至少 3 类异常，如登录失败突增、高频下单、高频加购/浏览。如果需要其他的，请自行构建谢谢，不推荐傻瓜式重写一大遍代码。

### 调参

- `PARALLELISM`：提高可提升吞吐，但会增加资源占用和 checkpoint 开销
- `STATE_BACKEND=rocksdb`：大状态更稳，延迟略升；`hashmap` 延迟更低但内存压力更大
- `CHECKPOINT_INTERVAL_MS`：更短恢复点更密集，但 I/O 开销更高
- `CHECKPOINT_TIMEOUT_MS`：过小会导致频繁 checkpoint 失败
- `CHECKPOINT_UNALIGNED_ENABLED=true`：高背压时可提升 checkpoint 成功率

### 观察指标（CK/Checkpoint）

- Checkpoint Duration
- Checkpoint Failed / Completed 次数
- End-to-End Latency
- Backpressure 状态
- Kafka consumer lag

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

## 项目结构（保姆级解说）

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

## 如需自行定制扩展：

### 添加新的规则类型

1. 在 `RiskRule.RuleType` 枚举中添加新类型
2. 在 `CepPatternFactory` 中实现对应的 Pattern
3. 更新 `AbnormalPatternDetector` 中的处理逻辑

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

如果不想手动进入 Docker 容器，直接运行：

```bash
mvn -Dit.testcontainers=true -Dtest=KafkaE2ETestcontainersTest test
```

## 注意事项

1. **生产环境部署**
   - 开启 Checkpoint，确保状态一致性
   - 根据数据量调整并行度和资源配置
   - 监控 Kafka 消费延迟

2. **规则配置**
   - 规则版本号递增，确保更新生效
   - 合理设置时间窗口和阈值，避免误报
   - 测试环境验证规则后再上！！！

3. **性能优化**
   - 大规模场景考虑使用 RocksDB 状态后端
   - 合理设置 Watermark 间隔
   - 监控 Backpressure 情况，不要问怎么观察，Flink Web UI 欢迎你

## License

MIT License
