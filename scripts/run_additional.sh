#!/usr/bin/env bash
# 只提交 MR18 和 MR25；已有输出由用户提前检查清理，不自动删除。
set -euo pipefail
cd "${MR_HOME:-/tools/MR}"
export HADOOP_CLASSPATH="$(hbase classpath)${HADOOP_CLASSPATH:+:$HADOOP_CLASSPATH}"
export HADOOP_HEAPSIZE="${HADOOP_HEAPSIZE:-512}"
RESULT_ROOT="${RESULT_ROOT:-/bitcoin/results}"
# 对当前小内存虚拟机限制 Java 堆；YARN 仍按集群最小容器大小归一化资源申请。
COMMON=(
  -Dmapreduce.framework.name=yarn
  "-Danalytics.reducers=${REDUCERS:-4}"
  "-Dcohort.summary.reducers=${SUMMARY_REDUCERS:-2}"
  -Dmapreduce.map.memory.mb=512
  -Dmapreduce.reduce.memory.mb=512
  -Dmapreduce.map.java.opts=-Xmx192m
  -Dmapreduce.reduce.java.opts=-Xmx192m
  -Dyarn.app.mapreduce.am.resource.mb=512
  -Dyarn.app.mapreduce.am.command-opts=-Xmx256m
)
hadoop jar BitcoinMR-1.0-SNAPSHOT-ActivityBursts.jar "${COMMON[@]}" "$RESULT_ROOT/mr18"
hadoop jar BitcoinMR-1.0-SNAPSHOT-CohortActivity.jar "${COMMON[@]}" "$RESULT_ROOT/mr25"
echo 'MR18 与 MR25 的计算和 HBase 导入均已成功。'
