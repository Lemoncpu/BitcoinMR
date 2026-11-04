#!/usr/bin/env bash
# 在 Linux 执行；读取集群 XML 配置，不设置 file:///，不自动删除输出。
set -euo pipefail
cd "${MR_HOME:-/tools/MR}"
export HADOOP_CLASSPATH="$(hbase classpath)${HADOOP_CLASSPATH:+:$HADOOP_CLASSPATH}"
RESULT_ROOT="${RESULT_ROOT:-/bitcoin/results}"
INPUT_PATH="${INPUT_PATH:-/bitcoin/results/input}"
REDUCERS="${REDUCERS:-4}"
export HADOOP_HEAPSIZE="${HADOOP_HEAPSIZE:-512}"
COMMON=(
  -Dmapreduce.framework.name=yarn
  "-Danalytics.reducers=$REDUCERS"
  "-Danalytics.input=$INPUT_PATH"
  -Dmapreduce.map.memory.mb=512 -Dmapreduce.reduce.memory.mb=512
  -Dmapreduce.map.java.opts=-Xmx192m -Dmapreduce.reduce.java.opts=-Xmx192m
  -Dyarn.app.mapreduce.am.resource.mb=512 -Dyarn.app.mapreduce.am.command-opts=-Xmx256m
  -Dmapreduce.job.running.map.limit=2 -Dmapreduce.job.running.reduce.limit=2
  -Dmapreduce.job.reduce.slowstart.completedmaps=1.0
  -Dmapreduce.client.completion.pollinterval=500
)
# 小内存单机虚拟机可在 HADOOP 配置中限制任务并发；Reducer 数量不代表物理主机数量。
run_task() {
  local entry="$1"
  shift
  echo "START $entry $(date -u +%FT%TZ)"
  hadoop jar "BitcoinMR-1.0-SNAPSHOT-$entry.jar" "${COMMON[@]}" "$@" > "$entry-run.log" 2>&1
  echo "SUCCESS $entry $(date -u +%FT%TZ)"
}
run_task DirectAverage "$INPUT_PATH" "$RESULT_ROOT/direct_average"
run_task PositiveKatz "$INPUT_PATH" "$RESULT_ROOT/positive_katz"
run_task SignedKatz "$INPUT_PATH" "$RESULT_ROOT/signed_katz"
run_task ActivityHeatmap "$RESULT_ROOT/mr22"
run_task DirectionalAsymmetry "$RESULT_ROOT/mr26"
run_task ActivityBursts "$RESULT_ROOT/mr18"
run_task CohortActivity "$RESULT_ROOT/mr25"
run_task PathConflicts "$RESULT_ROOT/mr27"
echo 'ALL_SUCCESS'
