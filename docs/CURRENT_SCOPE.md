# 当前任务与文件范围

当前保留MR7、MR18、MR22、MR25、MR26、MR27。MR7为3个入口，其余各1个，共8个业务入口。MR22只处理OTC，其余任务均支持Alpha、OTC。

正式Java源码共13个文件：8个业务入口，AnalyticsSupport、DistributedReputation、HdfsToHbase共3个共用运行类，HbaseEchartsServer服务类，以及PipelineAudit只读验收工具。测试及运行脚本只服务上述任务。

固定HDFS输出位于 /bitcoin/results 下各任务目录，当前发布数据存储于对应HBase表。页面 http://192.168.52.3:8002/mr7_echarts.html 支持11种任务/平台组合；MR22锁定OTC。明细统一展示前十条。

编译目录target可重新生成。原始CSV、原始需求方案、Maven/IDEA项目配置、ECharts资源、当前代码及当前任务测试样本予以保留。

## HDFS路径布局

/bitcoin直属只保留results，业务文件统一位于其下：

```text
/bitcoin/
  results/
    input/                  原始CSV
    analytics/              清洗事件、通用快照、MR7专用快照及输入指纹
    runtime-libs/           Hadoop任务所需的客户端依赖
    direct_average/         MR7均分结果
    positive_katz/          MR7正向Katz结果
    signed_katz/            MR7带符号Katz结果
    direct_average-stages/  MR7中间阶段（其余方法同样使用-stages）
    direct_average-hbase/   MR7分布式导入记录（其余方法同样使用-hbase）
    mr18/
    mr22/
    mr25/
    mr26/
    mr27/
    history/                迁移前的历史数据
```

当前代码、脚本、默认路径与独立验证均使用上述目录。历史数据整体迁入history，新的八个业务JAR重新计算并发布。实际运行记录和验证结果保存在本机target/bitcoin-namespace及Linux各入口的*-run.log。

run_distributed.sh在小内存虚拟机上限制同时运行的Map和Reduce各不超过2个；任务总Reducer数仍按业务默认配置，限制并发不改变分区数和结果。

本次路径迁移已完成全流程验收：八个业务JAR全部成功，122个实际Hadoop Job的manifest均标记成功。正向Katz运行52轮、带符号Katz运行40轮，两平台最终L1差值均小于0.01。HDFS全量结果、HBase接口及11个页面视图通过独立校验。验收记录见target/bitcoin-namespace/completion.json及同目录报告。
