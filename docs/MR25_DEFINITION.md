# MR25：首次主动评分用户的持续参与分析

定义版本：v2。入口仍为 CohortActivity.java，任务编号不变。

## 业务问题

某个月首次主动评分的用户，在后续第1、3、6个自然月，还有多少人在提供评价，贡献了多少次评分？比较不同时期进入评价网络的主动贡献者，辅助识别持续参与较弱的用户群组。这个数据集只有信任评分，不能用来判断注册、登录、交易或离开平台。

## 统计口径

设用户 u 在平台 p 作为 SOURCE 首次评分的 UTC 月为 C(u)。用户只属于这个群组；即使此前作为 TARGET 被别人评分，也不会提前进入群组。只作为 TARGET、从未发出评分的人完全排除。

- 群组规模 N(p,c) = 首次主动评分月等于 c 的去重用户数。
- 第 k 月参与人数 A(p,c,k) = 群组用户中，在 c+k 自然月至少主动评分一次的去重用户数。
- 第 k 月参与率 R(p,c,k) = A(p,c,k) / N(p,c)。
- 第 k 月评分贡献 E(p,c,k) = 该群组用户在 c+k 月发出的合法评分事件数之和。

第3月指精确的第三个自然月，不是前三个月累计；也不要求用户在中间月份持续出现。用户可在沉默后再次参与。同一用户同月十条评分，参与人数贡献一，评分次数贡献十。重复评分、自环和零分合法事件均保留；评分正负不影响是否主动参与。

例如：2020-01 首次主动评分的群组有100人。2020-04 中10人共发出35条评分，则第3月参与人数=10，参与率=10%，评分贡献=35；这不表示另外90人已离开平台。

## 可比较条件

采用完整自然月比较。最后一条事件所在月可能不完整，保守标记 cutoff_month，不进入默认图表；比它更晚为 not_observed，人数、比例、次数保存 N/A。更早的实际观察月无人参与时，才保存真实0。

默认 cohort.min.users=30，必须为正数。这是避免极小群组在默认图表中主导比较的展示门槛，不是置信度或显著性保证。小群组原始明细仍保存在 HDFS/HBase。

comparison_eligible = 群组人数达到门槛，并且目标月份严格早于平台截止月。页面展示各群组第1/3/6月的并列柱状图；不符合完整观察期的单元为空缺，不填0。人数比例与评分次数分两张图，悬浮给出分子、分母和贡献次数。第0月必须为100%，由代码校验。

## 经典 MapReduce 实现

1. FirstSeenMapper 只发送 SOURCE 候选首次月，Combiner/Reducer 求 min；内部平台元数据键求 max 截止月。
2. ActivityMapper 用 UserMonthKey(platform,SOURCE,month)，LongWritable 发送1；Combiner 和 Reducer 求和，输出每用户月唯一记录及评分次数。
3. 首次月记录 kind=0、活动月记录 kind=1。JoinKey 排序为平台、用户、kind、月份；分区和分组只看平台、用户。群组记录先到，Reducer 流式计算月偏移，输出人数和评分次数贡献，不缓存整个平台。
4. 平台和群组月份分组。CohortTotals Writable 保存群组人数、按偏移的人数与评分次数直方图；Combiner 可结合求和，Reducer 最后计算比例及比较资格。

没有客户端全量聚合、分子均值平均或按平台单 Reducer 处理全图。前三阶段各4个 Reducer，最后少量群组汇总用2个；Mapper 数由输入切片决定。

## 存储与运行

HDFS 固定结果目录：/bitcoin/results/mr25。HBase 表：analytics_mr25，列族 metrics，新批次完成后切换 current 指针。入口 JAR：/tools/MR/BitcoinMR-1.0-SNAPSHOT-CohortActivity.jar。

users 字段：platform,user_id,first_active_month。

cells 字段：platform,cohort_month,month_offset,activity_month,cohort_size,active_users,activity_rate,observation_status,event_count,comparison_eligible。

```bash
cd /tools/MR
export HADOOP_CLASSPATH="$(hbase classpath)${HADOOP_CLASSPATH:+:$HADOOP_CLASSPATH}"
hadoop jar BitcoinMR-1.0-SNAPSHOT-CohortActivity.jar -Dmapreduce.framework.name=yarn
```

运行参数见scripts/run_additional.sh。HBase 网页 http://192.168.52.3:8002/mr7_echarts.html 中选择 MR25。


## 当前结果与验证

Alpha 3,286名用户、61个群组、2,068条月份统计；OTC 4,814名用户、63个群组、2,094条月份统计。默认可比较群组分别为34、43个。独立验证：

```bash
python tests/verify_temporal.py --jobs mr25 --url http://192.168.52.3:8002 --report target/mr25-verification.json
```
