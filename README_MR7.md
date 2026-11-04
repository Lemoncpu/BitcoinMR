# 比特币信任网络 MapReduce 项目

数据链路：HDFS → Hadoop MapReduce → HBase → Java HTTP API → ECharts。

## 当前任务

| 编号 | 功能 | Java入口 | 平台 |
| --- | --- | --- | --- |
| MR7 | 三种信誉算法比较 | DirectAverage、PositiveKatz、SignedKatz | Alpha、OTC |
| MR18 | 用户每日评分突增检测 | ActivityBursts | Alpha、OTC |
| MR22 | 星期与小时评分活动 | ActivityHeatmap | OTC |
| MR25 | 主动评分用户持续参与 | CohortActivity | Alpha、OTC |
| MR26 | 双向评分差异 | DirectionalAsymmetry | Alpha、OTC |
| MR27 | 正向信任链与直接负评冲突 | PathConflicts | Alpha、OTC |

## 目录与代码

- src/main/java/org/example：8个业务入口、3个共用运行类、HTTP服务及1个只读验收工具。
- AnalyticsSupport：清洗、最新快照、共享Job配置、HBase导入与查询。
- DistributedReputation：MR7分布式聚合、迭代、排名；NodeKey同时供MR25使用。
- HdfsToHbase：MR7结果的分布式HBase导入。
- HbaseEchartsServer：页面和HBase查询服务。
- PipelineAudit：读取真实Job manifest与分区统计的验收工具。
- scripts：当前部署、分任务运行与结果收集脚本，ssh_known_hosts用于SSH主机校验。
- tests：当前任务的独立Python参考、小样本、Writable/Shuffle和页面检查。
- docs：当前任务定义、实现和业务结论。
- vendor：ECharts静态资源。
- target：可重新生成的编译结果、8个独立JAR、主JAR及当前验收输出。

根目录两份CSV是原始输入，预估方案文档是原始需求资料。

源码注释集中说明类职责、输入输出，以及分区、二次排序、Join、全局排名和收敛条件。MR7使用具名消息工厂，HBase两种导入路径共用记录构造方法；普通赋值和序列化语句保留清晰命名及标准格式。

## 编译与运行

```bash
mvn -q -DskipTests package
```

Linux目录为 /tools/MR，HDFS原始输入为 /bitcoin/results/input。

```bash
cd /tools/MR
bash run_distributed.sh
# 单独运行时序任务或路径冲突任务：
bash run_additional.sh
bash run_relation_jobs.sh
```

输出已存在时需先检查处理；脚本保留固定输出路径。MR7默认最多200轮，两个平台均满足L1<0.01时停止；常规计算4个Reducer，Map-only阶段0个，MR25少量群组汇总2个。

任务实现见 [DISTRIBUTED_REFACTOR.md](docs/DISTRIBUTED_REFACTOR.md)，时序口径见 [TEMPORAL_JOBS.md](docs/TEMPORAL_JOBS.md)，路径冲突见 [RELATION_JOBS.md](docs/RELATION_JOBS.md)。

## 页面

IDEA直接运行HbaseEchartsServer.main，工作目录设为项目根目录，访问 http://127.0.0.1:8002/mr7_echarts.html 。Linux服务为 http://192.168.52.3:8002/mr7_echarts.html 。

页面按任务和平台展示汇总、图表及前十条明细。MR22自动锁定OTC，其他任务提供Alpha和OTC。任务简介说明查询目标，页面不提供用户ID查询框。

## 验证

```bash
pip install -r requirements.txt
python tests/verify_api.py
python tests/verify_temporal.py --url http://192.168.52.3:8002 --report target/temporal-verification.json
python tests/verify_relation_jobs.py --url http://192.168.52.3:8002 --report target/relation-verification.json
python tests/verify_reputation_api.py
node tests/verify_dashboard.cjs
```

页面回归读取target/api-payloads内的当前HBase响应；前述Python接口验证会生成这些响应。Java业务运行不依赖Python；paramiko用于通过SSH收集HDFS结果。完整任务清单见 [CURRENT_SCOPE.md](docs/CURRENT_SCOPE.md)。
