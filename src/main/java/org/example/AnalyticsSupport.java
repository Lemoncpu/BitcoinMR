package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 当前任务共用的基础设施，不包含平台级整图计算。
 * 负责事件清洗、业务快照、复合键二次排序、Job 提交与 manifest 留痕、
 * HBase 依赖分发、分片导入及页面结构化查询。MR7 与 MR26 的关系筛选口径分别保留。
 */
public class AnalyticsSupport {

    /** 可单独提交共享预处理；业务任务也会自动调用并校验输入指纹。 */
    public static void main(String[] arguments) throws Exception {
        Configuration configuration = new Configuration();
        new org.apache.hadoop.util.GenericOptionsParser(configuration, arguments);
        System.out.println("MR7 snapshot: " + prepareReputation(configuration));
    }

    public static final ObjectMapper JSON = new ObjectMapper();

    public static final String BASE = "/bitcoin/results/analytics";

    public static final byte[] FAMILY = Bytes.toBytes("metrics");

    public static final Map<String, Map<String, String>> SCHEMAS = new LinkedHashMap<>();

    //业务的单独容器
    static {
        // 为MR22 热力数据的字段定义建立容器，不同业务维度单独保存。
        Map<String, String> mr22 = new LinkedHashMap<>();
        mr22.put("heatmap", "platform,weekday,hour,event_count");
        SCHEMAS.put("mr22", mr22);
        // 为MR26 双向明细与榜单字段定义建立容器，不同业务维度单独保存。
        Map<String, String> mr26 = new LinkedHashMap<>();
        mr26.put(
                "details",
                "platform,u,v,uv_rating,vu_rating,absolute_difference,normalized_difference,sign_conflict");
        mr26.put(
                "top",
                "platform,rank,u,v,uv_rating,vu_rating,absolute_difference,normalized_difference,sign_conflict");
        SCHEMAS.put("mr26", mr26);
        // MR18 按用户与 UTC 日保存全部窗口、异常候选和用户汇总。
        Map<String, String> mr18 = new LinkedHashMap<>();
        String windowColumns =
                "platform,user_id,window_date,event_count,negative_count,distinct_targets,history_days,baseline_mean,baseline_std,burst_threshold,negative_mean,negative_std,negative_threshold,burst,negative_burst,status";
        mr18.put("windows", windowColumns);
        mr18.put("alerts", windowColumns);
        mr18.put("users", "platform,user_id,alert_windows,negative_alert_windows,max_event_count");
        SCHEMAS.put("mr18", mr18);
        // MR25分母与活跃均只统计SOURCE，排除从未主动评分的被评价者。
        Map<String, String> mr25 = new LinkedHashMap<>();
        mr25.put("users", "platform,user_id,first_active_month");
        mr25.put(
                "cells",
                "platform,cohort_month,month_offset,activity_month,cohort_size,active_users,activity_rate,observation_status,event_count,comparison_eligible");
        SCHEMAS.put("mr25", mr25);
        // MR27：每条负边保留分母，每个中介以独立三元组提供可核查证据。
        Map<String, String> mr27 = new LinkedHashMap<>();
        mr27.put(
                "edges",
                "platform,source,target,direct_rating,mediator_count,support_score,max_path_weight,has_conflict");
        mr27.put(
                "paths",
                "platform,source,target,intermediary,source_rating,intermediary_rating,direct_rating,path_weight");
        SCHEMAS.put("mr27", mr27);
    }

    /**
     * 原始数据字典的结构化事件。
     * source/target/rating/time 分别对应 SOURCE/TARGET/RATING/TIME。
     * time 使用 BigDecimal 保留 OTC 的小数秒；eventId 由文件名与字节偏移构成，
     * 仅用于同时间记录的稳定排序，不参与信誉分数计算。
     */
    public static class Event {

        public String platform;
        public String eventId;
        public int source;
        public int target;
        public int rating;

        public java.math.BigDecimal time;

        public String tsv() {
            return platform + "\t" + source + "\t" + target + "\t" + rating + "\t" + time + "\t"
                    + eventId;
        }
    }

    // event_id 使用文件名与字节偏移量，固定输入下唯一且可复现，同时间以该 ID 确定顺序。
    /**
     * 合法事件清洗，属于 Map-only 阶段。
     * 读取两份原始 CSV，按文件名识别平台，检查字段数、用户编号、评分范围和时间。
     * 合法数据写统一 TSV；非法数据写 rejects，并记录质量 Counter。自环在事件层保留。
     */
    public static class CleanMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> rejects;

        @Override
        protected void setup(Context context) {
            rejects = new MultipleOutputs<>(context);
        }

        @Override
        public void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            context.getCounter("QUALITY", "RAW_EVENTS").increment(1);
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            // 把单行解析放在统一异常边界内，格式错误只拒绝此事件，不污染合法统计。
            try {
                String[] fields = line.toString().split(",", -1);
                if (fields.length != 4) {
                    throw new IllegalArgumentException("field_count");
                }
                Event event = new Event();
                // 原文件名必须能识别平台，避免两个网络的同编号用户混合。
                if (!filename.contains("alpha") && !filename.contains("otc")) {
                    throw new IllegalArgumentException("platform");
                }
                event.platform = filename.contains("alpha") ? "alpha" : "otc";
                event.source = Integer.parseInt(fields[0]);
                event.target = Integer.parseInt(fields[1]);
                event.rating = Integer.parseInt(fields[2]);
                // 用精确小数保存 TIME，不能转 long 丢掉 OTC 的小数秒。
                event.time = new java.math.BigDecimal(fields[3]);
                // 检查 SOURCE/TARGET 非负、RATING 在 -10 到 10 之间、TIME 非负，越界事件进入拒绝分支。
                if (event.source < 0
                        || event.target < 0
                        || Math.abs((long) event.rating) > 10
                        || event.time.signum() < 0) {
                    throw new IllegalArgumentException("range");
                }
                // 以文件名和补齐 20 位的字节偏移生成稳定标识，重复运行选版本的顺序一致。
                event.eventId = filename + ":" + String.format(Locale.ROOT, "%020d", offset.get());
                // 识别自环并计数；是否保留由通用快照和 MR7 视图各自决定。
                if (event.source == event.target) {
                    // 记录合法自环数；Counter 由 Hadoop 汇总成功任务，失败重试不会重复累加到最终统计。
                    context.getCounter("QUALITY", "SELF_LOOP_EVENTS").increment(1);
                }
                context.getCounter("QUALITY", "VALID_EVENTS").increment(1);
                context.getCounter("QUALITY", "VALID_" + event.platform).increment(1);
                // 输出合法六字段事件 TSV；NullWritable 避免附加多余值列。
                context.write(new Text(event.tsv()), NullWritable.get());
            } catch (IllegalArgumentException malformed) {
                context.getCounter("QUALITY", "REJECTED_EVENTS").increment(1);
                rejects.write(
                        "rejects",
                        new Text(
                                filename
                                        + "\t"
                                        + offset
                                        + "\t"
                                        + malformed.getMessage()
                                        + "\t"
                                        + line),
                        NullWritable.get(),
                        "rejects/invalid");
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            rejects.close();
        }
    }

    /**
     * 通用关系快照的二次排序键。
     * 平台、SOURCE、TARGET 决定关系身份；TIME 降序、eventId 降序决定组内先后。
     * 时间用精确十进制比较，不能转成整数秒或依赖字符串字典序。
     */
    public static class EdgeVersionKey implements WritableComparable<EdgeVersionKey> {

        protected String platform = "";
        protected String eventId = "";
        protected int source;
        protected int target;

        // 用精确小数保存 TIME，不能转 long 丢掉 OTC 的小数秒。
        protected java.math.BigDecimal time = java.math.BigDecimal.ZERO;

        public EdgeVersionKey() {}

        public EdgeVersionKey(String[] fields) {
            platform = fields[0];
            // 解析快照第 2 列 SOURCE，明确评分发起者的身份。
            source = Integer.parseInt(fields[1]);
            // 解析快照第 3 列 TARGET，它决定入边统计和贡献路由目标。
            target = Integer.parseInt(fields[2]);
            // 用精确小数保存 TIME，不能转 long 丢掉 OTC 的小数秒。
            time = new java.math.BigDecimal(fields[4]);
            eventId = fields[5];
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, platform);
            out.writeInt(source);
            out.writeInt(target);
            Text.writeString(out, time.toPlainString());
            Text.writeString(out, eventId);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            platform = Text.readString(in);
            source = in.readInt();
            target = in.readInt();
            time = new java.math.BigDecimal(Text.readString(in));
            eventId = Text.readString(in);
        }

        public int compareEdge(EdgeVersionKey other) {
            // 先按平台升序比较，Alpha 和 OTC 的相同用户编号不能成为同一业务键。
            int result = platform.compareTo(other.platform);
            if (result == 0) {
                // 平台相同后按 SOURCE 数值升序比较，保持有向关系的发起者身份。
                result = Integer.compare(source, other.source);
            }
            if (result == 0) {
                // SOURCE 相同后按 TARGET 数值升序比较，反向关系不会和原关系混为一组。
                result = Integer.compare(target, other.target);
            }
            return result;
        }

        @Override
        public int compareTo(EdgeVersionKey other) {
            // 先比较关系身份；只有平台和两个端点都相同才比较版本。
            int result = compareEdge(other);
            if (result == 0) {
                // 反过来比较时间以实现降序，最新 TIME 排在同一关系组的最前面。
                result = other.time.compareTo(time);
            }
            if (result == 0) {
                result = other.eventId.compareTo(eventId);
            }
            return result;
        }
    }

    /**
     * 只按平台、SOURCE、TARGET 分区。
     * 忽略版本时间和 eventId，防止同一条关系的不同历史版本被分散到不同 Reducer。
     */
    public static class EdgePartitioner extends Partitioner<EdgeVersionKey, Text> {

        @Override
        public int getPartition(EdgeVersionKey key, Text value, int partitions) {
            // 哈希只包含平台与 SOURCE/TARGET，不含时间，使同一关系的全部版本进入同一区。
            int hash = 31 * (31 * key.platform.hashCode() + key.source) + key.target;
            return (hash & Integer.MAX_VALUE) % partitions;
        }
    }

    /**
     * 关系身份分组比较器。
     * 虽然完整键还包含版本字段，reduce 分组只比较平台与两个端点，
     * 因此同一关系的所有版本按已排序顺序交给一次 reduce 调用。
     */
    public static class EdgeGroupingComparator extends WritableComparator {

        public EdgeGroupingComparator() {
            super(EdgeVersionKey.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            // 分组只比较平台与两个端点，版本字段不能拆开同一关系。
            return ((EdgeVersionKey) left).compareEdge((EdgeVersionKey) right);
        }
    }

    /**
     * 通用非自环快照输入。
     * 排除 SOURCE=TARGET 的事件，其余事件转换为复合版本键；值保留整条 TSV，
     * 让 Reducer 可以直接输出组内第一条记录，而不保存历史集合。
     */
    public static class SnapshotMapper extends Mapper<LongWritable, Text, EdgeVersionKey, Text> {

        @Override
        public void map(LongWritable offset, Text line, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = line.toString().split("\t", -1);
            // 通用快照剔除自环；MR7 专用快照不执行此过滤。
            if (fields[1].equals(fields[2])) {
                // 记录通用快照剔除的自环数；Counter 由 Hadoop 汇总成功任务，失败重试不会重复累加到最终统计。
                context.getCounter("QUALITY", "FILTERED_SELF_LOOPS").increment(1);
                return;
            }
            context.write(new EdgeVersionKey(fields), line);
        }
    }

    /**
     * 二次排序后的流式去重。
     * 第一条记录就是业务比较规则选中的版本；其余记录只计入被替代事件数。
     * 内存不随某个平台的历史边总数增长。MR7 使用同一归约逻辑，但配置自己的排序键。
     */
    public static class SnapshotReducer extends Reducer<EdgeVersionKey, Text, Text, NullWritable> {

        @Override
        public void reduce(EdgeVersionKey key, Iterable<Text> records, Context context)
                throws IOException, InterruptedException {
            // Shuffle 已将最新版本排在首位，流式消费即可，不保存历史记录集合。
            long count = 0;
            // 记录按版本优先级有序，首条保留，其他版本只计数。
            for (Text record : records) {
                // 版本已按优先级排序，只输出首条，其余仅计为被替代记录。
                if (count++ == 0) {
                    // 只输出当前关系的首条最新版本，其他历史记录不写入快照。
                    context.write(record, NullWritable.get());
                }
            }
            // 记录被新版本替代的历史数；Counter 由 Hadoop 汇总成功任务，失败重试不会重复累加到最终统计。
            context.getCounter("QUALITY", "SUPERSEDED_EVENTS").increment(count - 1);
            context.getCounter("QUALITY", "SNAPSHOT_EDGES").increment(1);
        }
    }

    /**
     * MR7 专用关系版本键，保留原有评分选择规则。
     * 最新 TIME 优先，同时间取较大 RATING，最后用 eventId 稳定顺序；
     * MR7 保留自环，不沿用 MR26 的非自环与同时间最后事件口径。
     */
    public static class ReputationEdgeKey extends EdgeVersionKey {

        private int rating;

        public ReputationEdgeKey() {}

        ReputationEdgeKey(String[] fields) {
            // 先处理通用关系身份与版本字段，再补充 MR7 的评分字段。
            super(fields);
            rating = Integer.parseInt(fields[3]);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            super.write(out);
            out.writeInt(rating);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // 先还原父类字段，之后读取子类评分，不能颠倒字节顺序。
            super.readFields(in);
            rating = in.readInt();
        }

        @Override
        public int compareTo(EdgeVersionKey other) {
            ReputationEdgeKey right = (ReputationEdgeKey) other;
            // 先比较关系身份；只有平台和两个端点都相同才比较版本。
            int order = compareEdge(right);
            if (order == 0) {
                // 反过来比较时间以实现降序，最新 TIME 排在同一关系组的最前面。
                order = right.time.compareTo(time);
            }
            if (order == 0) {
                // 保持 MR7 原规则：TIME 相同时较大 RATING 优先，eventId 只作最后稳定顺序。
                order = Integer.compare(right.rating, rating);
            }
            if (order == 0) {
                order = right.eventId.compareTo(eventId);
            }
            return order;
        }
    }

    /**
     * MR7 关系快照的 Mapper。
     * 保留自环，使用 ReputationEdgeKey 排序；其余输入输出字段仍与统一事件 TSV 一致。
     */
    public static class ReputationSnapshotMapper
            extends Mapper<LongWritable, Text, ReputationEdgeKey, Text> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 发送 MR7 专用版本键，保留自环，同时间优先较大 RATING。
            context.write(new ReputationEdgeKey(row.toString().split("\t", -1)), row);
        }
    }

    /**
     * MR7 按关系身份分组，版本和评分不参与分组。
     * 原始字节必须按 ReputationEdgeKey 解码，因为它比通用键多写入一个 rating 字段。
     */
    public static class ReputationGroupingComparator extends WritableComparator {

        public ReputationGroupingComparator() {
            super(ReputationEdgeKey.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            // 分组只比较平台与两个端点，版本字段不能拆开同一关系。
            return ((ReputationEdgeKey) left).compareEdge((ReputationEdgeKey) right);
        }
    }

    /**
     * 准备 MR7 专用快照，并保持重构前的业务规则。
     *
     * 先复用经过指纹检查的合法事件，再按 TIME、RATING、eventId 降序选择最新关系。
     * 自环在该视图保留；通用快照的筛选口径不应直接替代 MR7。
     * 结果存在 _SUCCESS 时复用，否则提交一个独立的二次排序 Job。
     */
    public static String prepareReputation(Configuration configuration) throws Exception {
        // 先完成通用数据准备与输入指纹检查，供 MR7 专用快照复用合法事件。
        prepare(configuration);
        String base = configuration.get("analytics.base", BASE);
        String snapshot = base + "/reputation-snapshot";
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path(snapshot + "/_SUCCESS"))) {
            return snapshot;
        }
        Job latest =
                job(
                        configuration,
                        "MR7 latest edge secondary sort (TIME then RATING)",
                        AnalyticsSupport.class,
                        ReputationSnapshotMapper.class,
                        SnapshotReducer.class,
                        ReputationEdgeKey.class,
                        Text.class,
                        base + "/events/part-*",
                        snapshot);
        latest.setPartitionerClass(EdgePartitioner.class);
        // 指定 reduce 分组身份，允许排序键中的额外字段只参与组内排序。
        latest.setGroupingComparatorClass(ReputationGroupingComparator.class);
        run(latest);
        return snapshot;
    }

    /**
     * 创建一个尚未提交的标准 Job，集中设置一致的输入输出契约。
     *
     * 输入由 TextInputFormat 读取；最终输出为 Text 与 NullWritable 的 TSV 行。
     * mapKey/mapValue 描述 Shuffle 数据类型，不应与最终输出类型混淆。
     * reducer 为 null 时创建 Map-only Job，否则使用 analytics.reducers（默认 4，必须大于 0）。
     * 额外分区器、比较器、Combiner、MultipleInputs 或命名输出由业务驱动继续注册。
     */
    public static Job job(
            Configuration configuration,
            String name,
            Class<?> driver,
            Class<? extends Mapper> mapper,
            Class<? extends Reducer> reducer,
            Class<?> mapKey,
            Class<?> mapValue,
            String input,
            String output)
            throws IOException {
        Job job = Job.getInstance(configuration, name);
        job.setJarByClass(driver);
        job.setMapperClass(mapper);
        if (reducer != null) {
            // 读取 Reduce 并行度，默认四个；下面检查归约阶段不能配置为零。
            int reducers = configuration.getInt("analytics.reducers", 4);
            if (reducers < 1) {
                throw new IllegalArgumentException("analytics.reducers must be positive");
            }
            job.setReducerClass(reducer);
            job.setNumReduceTasks(reducers);
        } else {
            job.setNumReduceTasks(0);
        }
        job.setMapOutputKeyClass(mapKey);
        job.setMapOutputValueClass(mapValue);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));
        return job;
    }

    /**
     * 提交并同步等待一个物理 Job，成功与失败都记录执行 manifest。
     *
     * 先检查输出不存在，防止覆盖旧结果；失败立即抛异常，调用方不能继续发布后续阶段。
     * manifest 保存真实 job_id、输入输出、执行时间和 Counter，用于追溯和分布式验收。
     * 已有目录检查在提交之前完成，不覆盖旧批次的 manifest。
     */
    public static void run(Job job) throws Exception {
        Path plannedOutput = FileOutputFormat.getOutputPath(job);
        if (plannedOutput.getFileSystem(job.getConfiguration()).exists(plannedOutput)) {
            throw new IOException(
                    "Output already exists; inspect and remove it manually: " + plannedOutput);
        }
        long started = System.currentTimeMillis();
        boolean success = false;
        try {
            success = job.waitForCompletion(true);
            // 物理 Job 失败就停止下游，不能继续发布结果。
            if (!success) {
                throw new IOException("Failed job: " + job.getJobName());
            }
        } finally {
            Map<String, Object> manifest = new LinkedHashMap<>();
            manifest.put("name", job.getJobName());
            manifest.put("job_id", String.valueOf(job.getJobID()));
            manifest.put("success", success);
            manifest.put("elapsed_ms", System.currentTimeMillis() - started);
            manifest.put("input", Arrays.toString(FileInputFormat.getInputPaths(job)));
            manifest.put("output", FileOutputFormat.getOutputPath(job).toString());
            Map<String, Long> counters = new TreeMap<>();
            // 失败 Job 可能无法提供完整计数器，读取失败只影响诊断信息。
            try {
                for (CounterGroup group : job.getCounters()) {
                    for (Counter counter : group) {
                        counters.put(group.getName() + "/" + counter.getName(), counter.getValue());
                    }
                }
            } catch (Exception ignored) {
                // 失败时可能无法获取计数器，只容忍此处读取 Counter 失败，仍保留 manifest。
            }
            manifest.put("counters", counters);
            Path record = new Path(FileOutputFormat.getOutputPath(job), "_job_manifest.json");
            FileSystem fs = record.getFileSystem(job.getConfiguration());
            try (FSDataOutputStream out = fs.create(record, true)) {
                out.write(JSON.writeValueAsBytes(manifest));
            }
        }
    }

    private static String inputVersion(Configuration configuration) throws Exception {
        FileSystem fs = FileSystem.get(configuration);
        Map<String, String> versions = new TreeMap<>();
        for (FileStatus file :
                fs.listStatus(
                        new Path(configuration.get("analytics.input", "/bitcoin/results/input")))) {
            // 只对普通 CSV 文件生成指纹，忽略目录和其他辅助文件。
            if (file.isFile() && file.getPath().getName().endsWith(".csv")) {
                // 按文件路径记录内容校验和，输入变化后缓存不能继续复用。
                versions.put(
                        file.getPath().toString(),
                        String.valueOf(fs.getFileChecksum(file.getPath())));
            }
        }
        if (versions.isEmpty()) {
            throw new IOException("No input CSV files");
        }
        return JSON.writeValueAsString(versions);
    }

    /**
     * 准备可复用的清洗事件与通用关系快照。
     *
     * 用输入文件校验和生成指纹；已有完整快照且指纹一致时才允许复用。
     * 输入发生变化或只留下半成品目录时拒绝继续，避免使用过期关系或混入失败输出。
     * 首次执行先提交 Map-only 清洗，再提交带二次排序的非自环快照 Job；
     * 两个平台都必须至少有一条合法事件，质量门槛未通过则停止。
     */
    public static String prepare(Configuration configuration) throws Exception {
        String root = configuration.get("analytics.base", BASE);
        String events = root + "/events";
        String snapshot = root + "/snapshot";
        FileSystem fs = FileSystem.get(configuration);
        String version = inputVersion(configuration);
        if (fs.exists(new Path(snapshot + "/_SUCCESS"))) {
            try (FSDataInputStream in = fs.open(new Path(root + "/input-version.json"))) {
                if (!version.equals(new String(readAll(in), StandardCharsets.UTF_8))) {
                    throw new IOException("Input changed: regenerate analytics events/snapshot");
                }
            }
            return snapshot;
        }
        if (fs.exists(new Path(events)) || fs.exists(new Path(snapshot))) {
            throw new IOException(
                    "Partial preparation exists; inspect then remove events/snapshot manually");
        }
        Job clean =
                job(
                        configuration,
                        "Analytics ETL: validate events",
                        AnalyticsSupport.class,
                        CleanMapper.class,
                        null,
                        Text.class,
                        NullWritable.class,
                        configuration.get("analytics.input", "/bitcoin/results/input"),
                        events);
        MultipleOutputs.addNamedOutput(
                clean, "rejects", TextOutputFormat.class, Text.class, NullWritable.class);
        run(clean);
        if (clean.getCounters().findCounter("QUALITY", "VALID_alpha").getValue() == 0
                || clean.getCounters().findCounter("QUALITY", "VALID_otc").getValue() == 0) {
            throw new IOException("Quality gate: both platforms must contain valid events");
        }
        Job latest =
                job(
                        configuration,
                        "Analytics ETL: latest directed snapshot",
                        AnalyticsSupport.class,
                        SnapshotMapper.class,
                        SnapshotReducer.class,
                        EdgeVersionKey.class,
                        Text.class,
                        events + "/part-*",
                        snapshot);
        latest.setPartitionerClass(EdgePartitioner.class);
        // 指定 reduce 分组身份，允许排序键中的额外字段只参与组内排序。
        latest.setGroupingComparatorClass(EdgeGroupingComparator.class);
        run(latest);
        try (FSDataOutputStream out = fs.create(new Path(root + "/input-version.json"), true)) {
            out.write(version.getBytes(StandardCharsets.UTF_8));
        }
        return snapshot;
    }

    public static byte[] readAll(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int size;
        while ((size = in.read(buffer)) != -1) {
            // 只写当前有效字节数，不把缓冲区未使用部分复制到结果。
            out.write(buffer, 0, size);
        }
        return out.toByteArray();
    }

    /**
     * 将实际运行时的 HBase 客户端依赖加入 Hadoop DistributedCache。 HADOOP_CLASSPATH 只解决提交客户端的依赖，不能假定所有 YARN 容器天然有
     * HBase。 使用内容哈希命名共享 HDFS 依赖；每个节点按 Job classpath 自动本地化。
     */
    public static void configureHbaseClasspath(Job job) throws Exception {
        Set<String> libraries = new LinkedHashSet<>();
        // 定位运行中真正使用的依赖 JAR，避免硬编码另一版本路径。
        for (Class<?> dependency :
                new Class<?>[] {
                    ConnectionFactory.class,
                    org.apache.htrace.core.Tracer.class,
                    org.apache.yetus.audience.InterfaceAudience.class
                }) {
            java.net.URL location = dependency.getProtectionDomain().getCodeSource().getLocation();
            if (!location.getProtocol().equals("file")) {
                throw new IOException("Unsupported dependency location: " + location);
            }
            java.io.File local = new java.io.File(location.toURI());
            if (!local.isFile()) {
                throw new IOException("Expected dependency JAR: " + local);
            }
            // 按实际规范路径去重依赖，避免同一 JAR 重复上传。
            libraries.add(local.getCanonicalPath());
        }
        FileSystem fs = FileSystem.get(job.getConfiguration());
        for (String library : libraries) {
            java.security.MessageDigest digest = java.security.MessageDigest.getInstance("SHA-256");
            // 分块读取本地依赖计算摘要，不将整个 JAR 留在内存。
            try (InputStream input = new FileInputStream(library)) {
                byte[] bytes = new byte[65536];
                int size;
                while ((size = input.read(bytes)) != -1) {
                    // 只把本次有效字节加入摘要，不将缓冲尾部旧字节算进去。
                    digest.update(bytes, 0, size);
                }
            }
            StringBuilder hash = new StringBuilder();
            for (byte value : digest.digest()) {
                hash.append(String.format(Locale.ROOT, "%02x", value & 255));
            }
            Path remote =
                    new Path(
                            job.getConfiguration()
                                            .get(
                                                    "analytics.runtime.libs",
                                                    "/bitcoin/results/runtime-libs")
                                    + "/"
                                    + hash
                                    + "-"
                                    + new java.io.File(library).getName());
            if (!fs.exists(remote)) {
                Path uploading = new Path(remote + "." + UUID.randomUUID() + ".upload");
                // 将依赖先上传临时路径，再原子改名，避免并发任务读到半个 JAR。
                try {
                    // 先传到独有临时路径，避免其他任务读到只上传了一半的 JAR。
                    fs.copyFromLocalFile(
                            false, false, new Path(new java.io.File(library).toURI()), uploading);
                    if (!fs.rename(uploading, remote) && !fs.exists(remote)) {
                        throw new IOException("Cannot publish runtime dependency: " + remote);
                    }
                } finally {
                    if (fs.exists(uploading)) {
                        // 只删除本进程临时上传文件，不递归清理业务输出或共享缓存。
                        fs.delete(uploading, false);
                    }
                }
            }
            job.addFileToClassPath(remote);
        }
    }

    /**
     * 已有聚合任务的分片 HBase 导入。
     * 根据 HDFS 文件的父目录识别数据集，再按注册 Schema 拆分字段。
     * 业务主键前加入本批次 generation；任务重试写同一行键，可幂等覆盖。
     * cleanup 关闭 BufferedMutator，确认缓冲写成功后 Map 才能被视为成功。
     */
    public static class HbaseSinkMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        private Connection connection;

        private BufferedMutator writer;

        private String jobId;
        private String generation;
        private String dataset;

        private String[] columns;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration configuration = HBaseConfiguration.create(context.getConfiguration());
            jobId = configuration.get("sink.job");
            generation = configuration.get("sink.generation");
            dataset = ((FileSplit) context.getInputSplit()).getPath().getParent().getName();
            columns = SCHEMAS.get(jobId).get(dataset).split(",");
            connection = ConnectionFactory.createConnection(configuration);
            writer = connection.getBufferedMutator(TableName.valueOf("analytics_" + jobId));
        }

        @Override
        public void map(LongWritable offset, Text line, Context context) throws IOException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            writer.mutate(resultPut(jobId, generation, dataset, columns, line.toString()));
            context.getCounter("HBASE_SINK", "ROWS").increment(1);
        }

        @Override
        protected void cleanup(Context context) throws IOException {
            // 先关闭缓冲写入器，finally 再关闭连接，保证异常时也释放底层资源。
            try {
                // 仅关闭实际创建的写入器，兼容 setup 中途失败。
                if (writer != null) {
                    writer.close();
                }
            } finally {
                // 仅释放已经创建的连接，避免清理阶段再次空指针。
                if (connection != null) {
                    connection.close();
                }
            }
        }
    }

    /**
     * 把一个已完成任务的所有数据集分片导入 HBase，再发布批次。
     *
     * 先逐个检查数据集的 _SUCCESS，创建本次 generation 并注册 Schema。
     * HbaseSinkMapper 负责实际明细写入，驱动只读取总行数 Counter，不收集全部记录。
     * 只有导入 Job 成功、缓冲写全部确认后，才原子更新 current 的 generation 和 Schema。
     * 各任务表独立切换；不承诺多个任务之间的跨表事务。
     */
    public static void publish(Configuration configuration, String jobId, String root)
            throws Exception {
        Configuration sinkConfiguration = HBaseConfiguration.create(configuration);
        FileSystem fs = FileSystem.get(sinkConfiguration);
        Map<String, String> schemas = SCHEMAS.get(jobId);
        String generation = UUID.randomUUID().toString();
        TableName tableName = TableName.valueOf("analytics_" + jobId);
        sinkConfiguration.set("sink.job", jobId);
        sinkConfiguration.set("sink.generation", generation);
        for (String dataset : schemas.keySet()) {
            if (!fs.exists(new Path(root + "/" + dataset + "/_SUCCESS"))) {
                throw new IOException("Incomplete dataset: " + dataset);
            }
        }
        try (Connection connection = ConnectionFactory.createConnection(sinkConfiguration)) {
            try (Admin admin = connection.getAdmin()) {
                // 仅在表不存在时建表，已有历史批次不删除重建。
                if (!admin.tableExists(tableName)) {
                    admin.createTable(
                            TableDescriptorBuilder.newBuilder(tableName)
                                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
                                    .build());
                }
            }
            String firstDataset = schemas.keySet().iterator().next();
            Job sink =
                    job(
                            sinkConfiguration,
                            jobId + " distributed HBase sink",
                            AnalyticsSupport.class,
                            HbaseSinkMapper.class,
                            null,
                            Text.class,
                            NullWritable.class,
                            root + "/" + firstDataset + "/part-*",
                            root + "/hbase-import");
            for (String dataset : schemas.keySet()) {
                // 首个数据集已注册，此处只追加其余输入，防止同一份数据读取两遍。
                if (!dataset.equals(firstDataset)) {
                    FileInputFormat.addInputPath(sink, new Path(root + "/" + dataset + "/part-*"));
                }
            }
            // 通过分布式缓存分发客户端依赖，容器不能只依赖提交端环境变量。
            configureHbaseClasspath(sink);
            run(sink);
            long count = sink.getCounters().findCounter("HBASE_SINK", "ROWS").getValue();
            try (Table table = connection.getTable(tableName)) {
                Put current = new Put(Bytes.toBytes("current"));
                current.addColumn(FAMILY, Bytes.toBytes("generation"), Bytes.toBytes(generation));
                current.addColumn(
                        FAMILY, Bytes.toBytes("schemas"), JSON.writeValueAsBytes(schemas));
                current.addColumn(FAMILY, Bytes.toBytes("hdfs_output"), Bytes.toBytes(root));
                current.addColumn(
                        FAMILY, Bytes.toBytes("row_count"), Bytes.toBytes(Long.toString(count)));
                table.put(current);
            }
            System.out.println(
                    "Published HBase "
                            + tableName
                            + ", rows="
                            + count
                            + ", generation="
                            + generation);
        }
    }

    /** 两种导入路径共用相同的列校验和业务行键规则。 */
    static Put resultPut(
            String jobId, String generation, String dataset, String[] columns, String line)
            throws IOException {
        String[] fields = line.split("\t", -1);
        if (fields.length != columns.length) {
            throw new IOException("Schema mismatch: " + dataset + " " + line);
        }
        String rowKey =
                generation
                        + "#"
                        + fields[0]
                        + "#"
                        + dataset
                        + "#"
                        + datasetKey(jobId, dataset, fields);
        Put put = new Put(Bytes.toBytes(rowKey));
        for (int column = 0; column < columns.length; column++) {
            put.addColumn(FAMILY, Bytes.toBytes(columns[column]), Bytes.toBytes(fields[column]));
        }
        return put;
    }

    /** 按 Schema 选择完整业务身份，避免多日期或多偏移单元写到同一个 HBase 行。 */
    public static String datasetKey(String jobId, String dataset, String[] fields) {
        if (jobId.equals("mr27") && dataset.equals("paths")) {
            return fields[1] + "#" + fields[2] + "#" + fields[3];
        }
        boolean composite =
                jobId.equals("mr22")
                        || (jobId.equals("mr26") && dataset.equals("details"))
                        || (jobId.equals("mr18") && !dataset.equals("users"))
                        || (jobId.equals("mr25") && dataset.equals("cells"))
                        || (jobId.equals("mr27") && dataset.equals("edges"));
        return composite ? fields[1] + "#" + fields[2] : fields[1];
    }

    /**
     * 小中型结果的流式 HDFS→HBase 导入，不额外启动 YARN Job。
     * 逐行读各结果文件，不合并单文件、不收集全量列表；确认 flush 成功后发布 current。
     * 计算仍完全由上游 MapReduce 执行，导入阶段仅做格式校验和存储。
     */
    public static void publishStreaming(Configuration configuration, String jobId, String root)
            throws Exception {
        Map<String, String> schemas = SCHEMAS.get(jobId);
        if (schemas == null) {
            throw new IllegalArgumentException("Unknown job: " + jobId);
        }
        FileSystem fs = FileSystem.get(configuration);
        // 所有数据集都完整后才开始任何写入，避免将半成品作为输入。
        for (String dataset : schemas.keySet()) {
            if (!fs.exists(new Path(root + "/" + dataset + "/_SUCCESS"))) {
                throw new IOException("Incomplete dataset: " + dataset);
            }
        }
        String generation = UUID.randomUUID().toString();
        TableName tableName = TableName.valueOf("analytics_" + jobId);
        Map<String, Long> datasetCounts = new LinkedHashMap<>();
        long rowCount = 0;
        // 普通客户端只负责导入，不在这里重新聚合或计算业务指标。
        try (Connection connection =
                ConnectionFactory.createConnection(HBaseConfiguration.create(configuration))) {
            try (Admin admin = connection.getAdmin()) {
                // 仅在表不存在时建表，已有历史批次不删除重建。
                if (!admin.tableExists(tableName)) {
                    admin.createTable(
                            TableDescriptorBuilder.newBuilder(tableName)
                                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
                                    .build());
                }
            }
            try (BufferedMutator writer = connection.getBufferedMutator(tableName)) {
                for (Map.Entry<String, String> schema : schemas.entrySet()) {
                    String dataset = schema.getKey();
                    String[] columns = schema.getValue().split(",");
                    FileStatus[] parts = fs.globStatus(new Path(root + "/" + dataset + "/part-*"));
                    if (parts == null) {
                        throw new IOException("Missing part files: " + dataset);
                    }
                    long count = 0;
                    for (FileStatus part : parts) {
                        try (BufferedReader reader =
                                new BufferedReader(
                                        new InputStreamReader(
                                                fs.open(part.getPath()), StandardCharsets.UTF_8))) {
                            String line;
                            // 逐行读取到文件末尾，避免整体加载大文件。
                            while ((line = reader.readLine()) != null) {
                                // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
                                writer.mutate(resultPut(jobId, generation, dataset, columns, line));
                                count++;
                            }
                        }
                    }
                    datasetCounts.put(dataset, count);
                    rowCount += count;
                }
                // 未得到写入确认就不能发布，关闭也会继续检查异步错误。
                writer.flush();
            }
            try (Table table = connection.getTable(tableName)) {
                Put current = new Put(Bytes.toBytes("current"));
                current.addColumn(FAMILY, Bytes.toBytes("generation"), Bytes.toBytes(generation));
                current.addColumn(
                        FAMILY, Bytes.toBytes("schemas"), JSON.writeValueAsBytes(schemas));
                current.addColumn(FAMILY, Bytes.toBytes("hdfs_output"), Bytes.toBytes(root));
                current.addColumn(
                        FAMILY, Bytes.toBytes("row_count"), Bytes.toBytes(Long.toString(rowCount)));
                table.put(current);
            }
        }
        Map<String, Object> report = new LinkedHashMap<>();
        report.put("kind", "streaming-hbase-import");
        report.put("table", tableName.toString());
        report.put("generation", generation);
        report.put("datasets", datasetCounts);
        report.put("row_count", rowCount);
        try (FSDataOutputStream out = fs.create(new Path(root + "/_hbase_import.json"), false)) {
            out.write(JSON.writeValueAsBytes(report));
        }
        System.out.println(
                "Published " + tableName + " rows=" + rowCount + " datasets=" + datasetCounts);
    }

    /**
     * 为 ECharts 提供当前任务、当前平台的结构化 HBase 数据。
     *
     * 先读取 current 的 generation，再按 generation/platform/dataset 前缀扫描。
     * 同一次请求固定使用同一批次，不混入其他历史 generation；返回列名来自注册 Schema。
     * HBase 中保留 UTF-8 字段文本，前端按字段含义显式转为数值。
     */
    public static Map<String, Object> dashboard(
            Connection connection, String jobId, String platform) throws Exception {
        // 只允许已注册任务，防止查询未知数据集或任意 HBase 表。
        if (!SCHEMAS.containsKey(jobId)) {
            throw new IllegalArgumentException("Unknown job");
        }
        // MR22仅保留OTC；即使直接调用Java查询接口，也不读取历史Alpha批次。
        if (jobId.equals("mr22") && !platform.equals("otc")) {
            throw new IllegalArgumentException("MR22 supports OTC only");
        }
        Map<String, Object> response = new LinkedHashMap<>();
        Map<String, Object> datasets = new LinkedHashMap<>();
        try (Table table = connection.getTable(TableName.valueOf("analytics_" + jobId))) {
            Result current = table.get(new Get(Bytes.toBytes("current")));
            byte[] value = current.getValue(FAMILY, Bytes.toBytes("generation"));
            // 必需 HBase 列缺失时不能填零，交给调用方报告错误。
            if (value == null) {
                throw new IOException("No published generation");
            }
            String generation = Bytes.toString(value);
            for (Map.Entry<String, String> schema : SCHEMAS.get(jobId).entrySet()) {
                List<Map<String, String>> rows = new ArrayList<>();
                // 限定到本次 generation、平台与数据集前缀，不扫描其他历史批次。
                Scan scan =
                        new Scan()
                                .setRowPrefixFilter(
                                        Bytes.toBytes(
                                                generation
                                                        + "#"
                                                        + platform
                                                        + "#"
                                                        + schema.getKey()
                                                        + "#"));
                // 只读取 metrics 列族，减少无关字段和网络传输。
                scan.addFamily(FAMILY);
                scan.setCaching(1000);
                try (ResultScanner scanner = table.getScanner(scan)) {
                    for (Result stored : scanner) {
                        Map<String, String> row = new LinkedHashMap<>();
                        for (String column : schema.getValue().split(",")) {
                            byte[] field = stored.getValue(FAMILY, Bytes.toBytes(column));
                            if (field == null) {
                                throw new IOException("Missing column: " + column);
                            }
                            row.put(column, Bytes.toString(field));
                        }
                        rows.add(row);
                    }
                }
                datasets.put(schema.getKey(), rows);
            }
            response.put("source", "hbase-api");
            response.put("job", jobId);
            response.put("run_id", generation);
            response.put("datasets", datasets);
            response.put("schemas", SCHEMAS.get(jobId));
        }
        return response;
    }
}
