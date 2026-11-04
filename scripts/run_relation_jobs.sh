#!/usr/bin/env bash
# MR27先经Python验证再选定；固定输出存在时停止，不覆盖或删除。
set -euo pipefail
cd "${MR_HOME:-/tools/MR}"
export HADOOP_CLASSPATH="$(hbase classpath)${HADOOP_CLASSPATH:+:$HADOOP_CLASSPATH}"
export HADOOP_HEAPSIZE="${HADOOP_HEAPSIZE:-512}"
COMMON=(-Dmapreduce.framework.name=yarn -Danalytics.reducers=4
 -Dmapreduce.map.memory.mb=512 -Dmapreduce.reduce.memory.mb=512
 -Dmapreduce.map.java.opts=-Xmx192m -Dmapreduce.reduce.java.opts=-Xmx192m
 -Dyarn.app.mapreduce.am.resource.mb=512 -Dyarn.app.mapreduce.am.command-opts=-Xmx256m)
ROOT="${RESULT_ROOT:-/bitcoin/results}"
for JOB in mr27; do
 if hadoop fs -test -e "$ROOT/$JOB"; then echo "Output exists: $ROOT/$JOB" >&2; exit 1; fi
done
hadoop jar BitcoinMR-1.0-SNAPSHOT-PathConflicts.jar "${COMMON[@]}" "$ROOT/mr27"
