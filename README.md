# MySQL 供应商群实时推送到 Kafka

这个 Go 程序读取 MySQL ROW binlog，按 `qq_groups.name` 包含任意一个 `group_name_filter` 关键词过滤事件（区分大小写）。`publish_tables` 决定哪些表的事件写到 Kafka；当前 `config.local.json` 设为 `["qq_messages"]`，因此只发送符合条件群的新增消息。即使不发送群表事件，程序仍读取 `qq_groups` 的新增和更新来维护群名过滤状态。

| 表（启用时） | 推送规则 |
| --- | --- |
| `qq_messages` | 仅推送符合条件群的新增消息 |
| `qq_groups` | 推送符合条件群的新增、更新、删除；群改名离开条件时发送 `filter_exit` |
| `qq_group_members` | 推送当时符合条件群的新增、更新、删除 |

首次启动从当前 binlog 位置开始，不发送历史数据，也不在群改名进入条件时补发历史成员。群编号保留为字符串。所有事件写入 `kafka.topic` 指定的主题（当前 `config.local.json` 为 `maas_wechat`），Kafka key 为群编号，因此同一群的事件落到同一分区并保持顺序。

只发送消息表时配置：

```json
"group_name_filter": ["供应商", "供货商"],
"publish_tables": ["qq_messages"]
```

省略 `publish_tables` 时维持原有行为，发送三张表的符合条件事件。也可以按需列出其中两张或三张表。

`group_name_filter` 可以写成非空关键词数组，群名包含其中任意一个关键词即匹配。现有单字符串写法（如 `"供应商"`）仍兼容；空数组、空字符串和空白关键词会被拒绝。修改关键词后，应使用新的 `state_file`，因为旧断点保存的群筛选状态对应旧关键词；新状态文件会从启动时的 binlog 位置开始，不补发历史数据。

## 准备

需要 Go 1.25+、MySQL 的 `log_bin=ON`、`binlog_format=ROW`、`binlog_row_image=FULL`，以及一个可访问的 Kafka broker。程序需要持久化 `.state/checkpoint.db`；生产环境应放到持久卷且只运行一个实例。

当前 `config.local.json` 已放入提供的 MySQL 连接信息，并被 `.gitignore` 忽略，文件权限为 `0600`。最近一次 `--check` 检查通过 MySQL 连接、binlog 设置和三张表结构，授权信息显示具备复制与 `LOCK TABLES` 权限。正式启动前仍需确保账号能实际加锁并订阅 binlog。专用账号授权示例（由 DBA 按环境调整主机、密码和权限范围）：

```sql
CREATE USER 'qq_cdc'@'%' IDENTIFIED BY 'replace-with-a-strong-password';
GRANT SELECT, LOCK TABLES ON db2.* TO 'qq_cdc'@'%';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'qq_cdc'@'%';
```

首次启动会短暂对 `qq_groups` 加读锁，以便取得一致的群名集合和 binlog 位置。取得快照后立即解锁，随后才扫描群列表。请确认 `mysql.server_id` 与其他复制客户端不冲突；binlog 保留时间应覆盖停机时间。

## 本地运行

如果本机没有 Kafka，可先安装 Docker，再使用 Apache 官方镜像启动单节点测试 broker：

```bash
docker run -d --name qq-kafka -p 9092:9092 apache/kafka:4.3.0
docker exec qq-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic qq.supplier.events --partitions 3 --replication-factor 1
```

上述单节点 broker 用于本地验证，使用它时还需把 `config.local.json` 的 `kafka.brokers` 改为 `["127.0.0.1:9092"]`，`kafka.topic` 改为 `"qq.supplier.events"`。若使用已有 Kafka 集群，请保持对应的 broker 和 topic 配置，并预先创建目标 topic。

```bash
go build -o bin/mysq2kafka .
./bin/mysq2kafka --config config.local.json --check
./bin/mysq2kafka --config config.local.json
```

`--check` 只读检查数据库连接、binlog 参数、三张表列顺序和当前账号授权，不接入 Kafka，也不创建断点。正式运行后，可从 topic 消费 JSON 事件：

```bash
docker exec qq-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic qq.supplier.events --from-beginning
```

## 消息格式与恢复

消息示例：

```json
{
  "event_id": "mysql-bin.000123:45678:0/0",
  "table": "qq_messages",
  "op": "insert",
  "group_qq": "123456",
  "data": {"id": 37218405, "group_number": "123456", "content": "示例"},
  "binlog": {"file": "mysql-bin.000123", "pos": 45678}
}
```

`data` 实际包含该表全部列。当前仅发布 `qq_messages` 时，`op` 只有 `insert`。如果启用群表和成员表，`op` 还可能是 `update`、`delete`、`filter_exit`；`filter_exit` 表示群名不再符合条件，消费端可据此移除该群的下游状态。群成员主键改变时会输出旧键 `delete` 和新键 `insert`。消息表的更新和删除始终不推送。

程序在 Kafka 确认写入后才把 binlog 断点与群过滤状态一起提交到本地 BoltDB。崩溃发生在 Kafka 写入之后、断点提交之前时，重启可能重发少量事件；消费端可用 `event_id` 去重。若 Kafka 不可用，程序不会推进断点，恢复后可继续读取，前提是 MySQL 仍保留对应 binlog。目标表结构变化时程序会停止，需要调整列映射并重新编译。

目前只支持 MySQL 文件名和位置续传，不支持跨主库切换；配置的状态文件应与同一 MySQL 实例和数据库绑定。程序没有配置 TLS/SASL，适用于当前未提供这些参数的连接环境；接入需要认证的 Kafka 集群时需补充相应客户端配置。
