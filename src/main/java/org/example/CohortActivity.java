package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.util.*;

/**
 * MR25：首次主动评分用户的持续参与分析，不解释为注册、交易或平台留存。
 * 四个 Job：SOURCE 首次评分月 → 用户月评分计数 → 带标记用户 Join → 群组月参与及评分贡献。
 * 分母和分子统一为主动评分用户；仅被评价的 TARGET 不进入群组。
 * 末次观察月份之后的单元是 not_observed，不输出虚假的零留存。
 */
public class CohortActivity {
    // -1 不是合法原始用户 ID，只作为每平台观察上界的内部元数据键。
    private static final int BOUND_USER = -1;

    static int monthIndex(String time) {
        // 仅在月份分桶时取非负时间整秒，原始小数时间由清洗层保留。
        ZonedDateTime utc =
                Instant.ofEpochSecond(new BigDecimal(time).longValue()).atZone(ZoneOffset.UTC);
        return utc.getYear() * 12 + utc.getMonthValue() - 1;
    }

    static int parseMonth(String month) {
        YearMonth parsed = YearMonth.parse(month);
        return parsed.getYear() * 12 + parsed.getMonthValue() - 1;
    }

    static String formatMonth(int month) {
        return YearMonth.of(Math.floorDiv(month, 12), Math.floorMod(month, 12) + 1).toString();
    }

    /** 按SOURCE发送首次主动评分候选月，并发送平台截止月候选。 */
    public static class FirstSeenMapper
            extends Mapper<LongWritable, Text, DistributedReputation.NodeKey, IntWritable> {
        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            int month = monthIndex(fields[4]);
            // 只有 SOURCE 才是主动评分者；TARGET 被动收到评分不构成参与。
            context.write(
                    new DistributedReputation.NodeKey(fields[0], Integer.parseInt(fields[1])),
                    new IntWritable(month));
            context.write(
                    new DistributedReputation.NodeKey(fields[0], BOUND_USER),
                    new IntWritable(month));
        }
    }

    /** 同型局部聚合：用户月份取min，平台截止月取max。 */
    public static class FirstSeenCombiner
            extends Reducer<
                    DistributedReputation.NodeKey,
                    IntWritable,
                    DistributedReputation.NodeKey,
                    IntWritable> {
        @Override
        public void reduce(
                DistributedReputation.NodeKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int selected = key.userId == BOUND_USER ? Integer.MIN_VALUE : Integer.MAX_VALUE;
            for (IntWritable value : values) {
                // 普通节点求 min，元数据求 max；两者都可结合并允许多次 Combiner。
                selected =
                        key.userId == BOUND_USER
                                ? Math.max(selected, value.get())
                                : Math.min(selected, value.get());
            }
            // 普通节点求最早月份，-1 元数据节点求最晚月份，两者都可重复局部合并。
            context.write(key, new IntWritable(selected));
        }
    }

    /** 输出唯一用户首次月；平台截止月写入独立bounds数据集。 */
    public static class FirstSeenReducer
            extends Reducer<DistributedReputation.NodeKey, IntWritable, Text, NullWritable> {
        private MultipleOutputs<Text, NullWritable> outputs;

        @Override
        protected void setup(Context context) {
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(
                DistributedReputation.NodeKey key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int selected = key.userId == BOUND_USER ? Integer.MIN_VALUE : Integer.MAX_VALUE;
            for (IntWritable value : values) {
                selected =
                        key.userId == BOUND_USER
                                ? Math.max(selected, value.get())
                                : Math.min(selected, value.get());
            }
            if (key.userId == BOUND_USER) {
                outputs.write(
                        "bounds",
                        new Text(key.platform + "\t" + selected),
                        NullWritable.get(),
                        "bounds/part");
            } else {
                context.write(
                        new Text(key.platform + "\t" + key.userId + "\t" + formatMonth(selected)),
                        NullWritable.get());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }

    /** 月活动键：一个 SOURCE 在一个月最多贡献一次活跃用户。 */
    public static class UserMonthKey implements WritableComparable<UserMonthKey> {
        String platform = "";
        int userId;
        int month;

        public UserMonthKey() {}

        UserMonthKey(String platform, int userId, int month) {
            this.platform = platform;
            this.userId = userId;
            this.month = month;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, platform);
            out.writeInt(userId);
            out.writeInt(month);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            platform = Text.readString(in);
            userId = in.readInt();
            month = in.readInt();
        }

        @Override
        public int compareTo(UserMonthKey other) {
            int order = platform.compareTo(other.platform);
            if (order == 0) {
                order = Integer.compare(userId, other.userId);
            }
            return order != 0 ? order : Integer.compare(month, other.month);
        }

        @Override
        public int hashCode() {
            return Objects.hash(platform, userId, month);
        }
    }

    /** 用户月计数：重复事件保留为评分贡献，活跃人数在下游每用户月只加一。 */
    public static class ActivityMapper
            extends Mapper<LongWritable, Text, UserMonthKey, LongWritable> {
        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            String[] fields = row.toString().split("\t", -1);
            // 一条合法原始事件贡献一次评分，不能用最新边快照代替事件流。
            context.write(
                    new UserMonthKey(fields[0], Integer.parseInt(fields[1]), monthIndex(fields[4])),
                    new LongWritable(1));
        }
    }

    /** 局部累计每用户月评分次数，不提前计算参与比例。 */
    public static class ActivityCombiner
            extends Reducer<UserMonthKey, LongWritable, UserMonthKey, LongWritable> {
        @Override
        public void reduce(UserMonthKey key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long eventCount = 0;
            for (LongWritable value : values) {
                eventCount += value.get();
            }
            context.write(key, new LongWritable(eventCount));
        }
    }

    /** 输出每用户月唯一记录及评分总次数。 */
    public static class ActivityReducer
            extends Reducer<UserMonthKey, LongWritable, Text, NullWritable> {
        @Override
        public void reduce(UserMonthKey key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long eventCount = 0;
            for (LongWritable value : values) {
                eventCount += value.get();
            }
            // 每用户月只输出一行，同时保留该月真实评分次数。
            context.write(
                    new Text(
                            key.platform
                                    + "\t"
                                    + key.userId
                                    + "\t"
                                    + formatMonth(key.month)
                                    + "\t"
                                    + eventCount),
                    NullWritable.get());
        }
    }

    /** Join 复合键将 cohort 放在活动记录之前，避免 Reducer 缓存用户全部活动等待 cohort。 */
    public static class JoinKey implements WritableComparable<JoinKey> {
        String platform = "";
        int userId;
        byte kind;
        int month;

        public JoinKey() {}

        JoinKey(String platform, int userId, byte kind, int month) {
            this.platform = platform;
            this.userId = userId;
            this.kind = kind;
            this.month = month;
        }

        int compareUser(JoinKey other) {
            int order = platform.compareTo(other.platform);
            return order != 0 ? order : Integer.compare(userId, other.userId);
        }

        @Override
        public int compareTo(JoinKey other) {
            int order = compareUser(other);
            if (order == 0) {
                order = Byte.compare(kind, other.kind);
            }
            return order != 0 ? order : Integer.compare(month, other.month);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, platform);
            out.writeInt(userId);
            out.writeByte(kind);
            out.writeInt(month);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            platform = Text.readString(in);
            userId = in.readInt();
            kind = in.readByte();
            month = in.readInt();
        }
    }

    /** 值也保留来源标记与月份，不能依赖组内被框架复用的 Key。 */
    public static class MonthRecord implements Writable {
        byte kind;
        int month;
        long eventCount;

        public MonthRecord() {}

        MonthRecord(byte kind, int month, long eventCount) {
            this.kind = kind;
            this.month = month;
            this.eventCount = eventCount;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(kind);
            out.writeInt(month);
            out.writeLong(eventCount);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            kind = in.readByte();
            month = in.readInt();
            eventCount = in.readLong();
        }
    }

    /** 首次评分月连接侧，kind=0，排序后先于活动月份到达。 */
    public static class CohortMapper extends Mapper<LongWritable, Text, JoinKey, MonthRecord> {
        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            int month = parseMonth(fields[2]);
            // 来源标记0保证首次月份先到，同一用户后续活动可以流式连接。
            context.write(
                    new JoinKey(fields[0], Integer.parseInt(fields[1]), (byte) 0, month),
                    new MonthRecord((byte) 0, month, 0));
        }
    }

    /** 活动月连接侧，kind=1，保留月份和评分次数。 */
    public static class ActiveMonthMapper extends Mapper<LongWritable, Text, JoinKey, MonthRecord> {
        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            int month = parseMonth(fields[2]);
            context.write(
                    new JoinKey(fields[0], Integer.parseInt(fields[1]), (byte) 1, month),
                    new MonthRecord((byte) 1, month, Long.parseLong(fields[3])));
        }
    }

    /** 只按平台和用户分区，来源标记和月份不参与分区。 */
    public static class JoinPartitioner extends Partitioner<JoinKey, MonthRecord> {
        @Override
        public int getPartition(JoinKey key, MonthRecord value, int partitions) {
            return (Objects.hash(key.platform, key.userId) & Integer.MAX_VALUE) % partitions;
        }
    }

    /** 按平台和用户归组，组内仍保持kind和月份顺序。 */
    public static class JoinGrouping extends WritableComparator {
        public JoinGrouping() {
            super(JoinKey.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            return ((JoinKey) left).compareUser((JoinKey) right);
        }
    }

    /** 流式连接首次月和活动月，输出群组人数及月份偏移贡献。 */
    public static class JoinReducer extends Reducer<JoinKey, MonthRecord, Text, NullWritable> {
        @Override
        public void reduce(JoinKey key, Iterable<MonthRecord> values, Context context)
                throws IOException, InterruptedException {
            // 复制身份，避免遍历 values 时被二次排序 Key 复用影响。
            String platform = key.platform;
            Integer cohort = null;
            int previousActiveMonth = Integer.MIN_VALUE;
            for (MonthRecord value : values) {
                // 首次主动评分记录排在活动记录之前；必须唯一，不能被活动月份覆盖。
                if (value.kind == 0) {
                    if (cohort != null) {
                        throw new IOException("Duplicate first-seen row");
                    }
                    // 记住先到的唯一首次月份，之后每个活动月份都据此计算偏移。
                    cohort = value.month;
                    // -1 是内部 cohort 分母贡献，每名首次主动评分用户恰好一次。
                    context.write(
                            new Text(platform + "\t" + formatMonth(cohort) + "\t-1\t1\t0"),
                            NullWritable.get());
                } else if (value.kind == 1) {
                    // 仅主动评分者进入分母；每用户月贡献一人和该月全部评分次数。
                    if (cohort == null
                            || value.month < cohort
                            || value.month <= previousActiveMonth) {
                        throw new IOException("Missing cohort or invalid/distinct activity order");
                    }
                    // 活动已按用户月份去重，这条记录为该偏移贡献一名活跃用户。
                    context.write(
                            new Text(
                                    platform
                                            + "\t"
                                            + formatMonth(cohort)
                                            + "\t"
                                            + (value.month - cohort)
                                            + "\t1\t"
                                            + value.eventCount),
                            NullWritable.get());
                    previousActiveMonth = value.month;
                } else {
                    throw new IOException("Invalid cohort Join source tag");
                }
            }
        }
    }

    /** 可结合直方图：只保存 cohort 大小和月份偏移计数，不保存用户集合。 */
    public static class CohortTotals implements Writable {
        long users;
        // 为按月份偏移保存的主动活跃人数建立容器，按键排序以稳定输出。
        TreeMap<Integer, Long> active = new TreeMap<>();
        // 评分贡献与人数分别聚合，不能把评分次数误当人数。
        TreeMap<Integer, Long> events = new TreeMap<>();

        public CohortTotals() {}

        void add(int offset, long count, long eventCount) {
            // 区分分母贡献、合法活动偏移与不可接受的负偏移。
            if (offset == -1) {
                // 汇总首次主动评分用户分母，每名用户在 Join 阶段只贡献一次。
                users += count;
            } else if (offset >= 0) {
                // 区分分母贡献、合法活动偏移与不可接受的负偏移。
                // 相同月份偏移只加人数，不保存用户列表，空间只与月份数量有关。
                active.merge(offset, count, Long::sum);
                events.merge(offset, eventCount, Long::sum);
            } else {
                throw new IllegalArgumentException("Invalid cohort offset");
            }
        }

        void add(CohortTotals other) {
            // 汇总首次主动评分用户分母，每名用户在 Join 阶段只贡献一次。
            users += other.users;
            // 只遍历稀疏月份桶，桶数与历史月份有关，不与用户数量成正比。
            for (Map.Entry<Integer, Long> entry : other.active.entrySet()) {
                // 相同月份偏移只加人数，不保存用户列表，空间只与月份数量有关。
                active.merge(entry.getKey(), entry.getValue(), Long::sum);
                events.merge(entry.getKey(), other.events.get(entry.getKey()), Long::sum);
            }
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(users);
            out.writeInt(active.size());
            for (Map.Entry<Integer, Long> entry : active.entrySet()) {
                out.writeInt(entry.getKey());
                out.writeLong(entry.getValue());
                out.writeLong(events.get(entry.getKey()));
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            users = in.readLong();
            active.clear();
            events.clear();
            int size = in.readInt();
            if (size < 0) {
                throw new IOException("Invalid histogram size");
            }
            for (int index = 0; index < size; index++) {
                int monthOffset = in.readInt();
                active.put(monthOffset, in.readLong());
                events.put(monthOffset, in.readLong());
            }
        }
    }

    /** 按平台和首次评分月发送群组人数、活跃人数及事件数贡献。 */
    public static class CellMapper extends Mapper<LongWritable, Text, Text, CohortTotals> {
        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            CohortTotals count = new CohortTotals();
            count.add(
                    Integer.parseInt(fields[2]),
                    Long.parseLong(fields[3]),
                    Long.parseLong(fields[4]));
            // 平台与 cohort 月份共同分组，绝不只按平台汇总所有用户。
            context.write(new Text(fields[0] + "\t" + fields[1]), count);
        }
    }

    /** 合并群组人数及月份直方图；不平均局部比例。 */
    public static class CellCombiner extends Reducer<Text, CohortTotals, Text, CohortTotals> {
        @Override
        public void reduce(Text key, Iterable<CohortTotals> values, Context context)
                throws IOException, InterruptedException {
            CohortTotals total = new CohortTotals();
            for (CohortTotals value : values) {
                total.add(value);
            }
            // 把局部 cohort 大小与偏移计数直方图结合，不能平均局部比例。
            context.write(key, total);
        }
    }

    /** 计算各月参与率与评分贡献，区分完整月、截止月和未观察月。 */
    public static class CellReducer extends Reducer<Text, CohortTotals, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<CohortTotals> values, Context context)
                throws IOException, InterruptedException {
            CohortTotals total = new CohortTotals();
            for (CohortTotals value : values) {
                total.add(value);
            }
            String[] identity = key.toString().split("\t");
            int cohort = parseMonth(identity[1]);
            int lastObserved =
                    context.getConfiguration()
                            .getInt("cohort.last." + identity[0], Integer.MIN_VALUE);
            if (total.users == 0 || lastObserved < cohort) {
                throw new IOException("Missing cohort size or observation bound");
            }
            // 统一 SOURCE 口径后，第零月所有群组成员都必须主动评分。
            if (total.active.getOrDefault(0, 0L) != total.users) {
                throw new IOException("First active month must contain every cohort user");
            }
            int observedOffsets = lastObserved - cohort;
            // 至少展示头一年；截止月之后的单元保留 N/A，不能用零冒充未观察结果。
            int displayOffsets = Math.max(12, observedOffsets);
            // 逐个实际或待展示偏移输出格子，零活动与未观察两种状态分开。
            for (int offset = 0; offset <= displayOffsets; offset++) {
                // 仅到平台最后观察月为止才可解释活动比例；之后属于右删失。
                boolean observed = offset <= observedOffsets;
                long activeUsers = total.active.getOrDefault(offset, 0L);
                // 活跃人数不能大于 cohort 总人数，也不能出现在未观察月份。
                if (activeUsers > total.users
                        || total.events.getOrDefault(offset, 0L) < activeUsers
                        || (!observed && activeUsers != 0)) {
                    throw new IOException(
                            "Cohort numerator exceeds denominator or observation bound");
                }
                String status =
                        !observed
                                ? "not_observed"
                                : (offset == observedOffsets ? "cutoff_month" : "observed");
                // 已观察月保留真实计数和比例，未观察月写 N/A，不能伪造零。
                String count = observed ? Long.toString(activeUsers) : "N/A";
                // 已观察月保留真实计数和比例，未观察月写 N/A，不能伪造零。
                String rate =
                        observed ? Double.toString((double) activeUsers / total.users) : "N/A";
                // 用全 cohort 规模作分母，未观察的未来月份保留 N/A 而非零。
                context.write(
                        new Text(
                                key
                                        + "\t"
                                        + offset
                                        + "\t"
                                        + formatMonth(cohort + offset)
                                        + "\t"
                                        + total.users
                                        + "\t"
                                        + count
                                        + "\t"
                                        + rate
                                        + "\t"
                                        + status
                                        + "\t"
                                        + (observed
                                                ? Long.toString(
                                                        total.events.getOrDefault(offset, 0L))
                                                : "N/A")
                                        + "\t"
                                        + (offset < observedOffsets
                                                && total.users
                                                        >= context.getConfiguration()
                                                                .getInt("cohort.min.users", 30))),
                        NullWritable.get());
            }
        }
    }

    /** 驱动只读取每平台一个观察上界，不拉取用户明细。 */
    static Map<String, Integer> readBounds(Configuration configuration, String pattern)
            throws IOException {
        // 为每平台仅一条的观察截止月份建立容器，按键排序以稳定输出。
        Map<String, Integer> bounds = new TreeMap<>();
        FileSystem fs = FileSystem.get(configuration);
        FileStatus[] files = fs.globStatus(new Path(pattern));
        if (files == null) {
            throw new IOException("Missing observation bounds");
        }
        for (FileStatus file : files) {
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    fs.open(file.getPath()), StandardCharsets.UTF_8))) {
                String line;
                // 逐行读取到文件末尾，避免整体加载大文件。
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split("\t");
                    // 每平台必须且只能有一个观察上界，不能把两个网络的截止时间混用。
                    if (bounds.put(fields[0], Integer.parseInt(fields[1])) != null) {
                        throw new IOException("Duplicate platform observation bound");
                    }
                }
            }
        }
        // 每平台必须且只能有一个观察上界，不能把两个网络的截止时间混用。
        if (!bounds.containsKey("alpha") || !bounds.containsKey("otc")) {
            throw new IOException("Both platforms need observation bounds");
        }
        return bounds;
    }

    public static void main(String[] arguments) throws Exception {
        Configuration configuration = new Configuration();
        String[] paths = new GenericOptionsParser(configuration, arguments).getRemainingArgs();
        // 按允许的参数数量检查或选择路径，不能把配置或单个路径误解释为完整输入输出。
        if (paths.length > 1) {
            throw new IllegalArgumentException("Expected optional output root");
        }
        if (configuration.getInt("cohort.min.users", 30) < 1) {
            throw new IllegalArgumentException("cohort.min.users must be positive");
        }
        String output = paths.length == 0 ? "/bitcoin/results/mr25" : paths[0];
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path(output))) {
            throw new IOException("Output exists; inspect/remove manually: " + output);
        }
        // 先准备合法事件和通用快照，不能假定中间数据已存在。
        AnalyticsSupport.prepare(configuration);
        String events =
                configuration.get("analytics.base", AnalyticsSupport.BASE) + "/events/part-*";
        // Job 1 同时求用户首次主动评分 min 和每平台观察截止 max。
        Job first =
                AnalyticsSupport.job(
                        configuration,
                        "MR25 first source participation and cutoff",
                        CohortActivity.class,
                        FirstSeenMapper.class,
                        FirstSeenReducer.class,
                        DistributedReputation.NodeKey.class,
                        IntWritable.class,
                        events,
                        output + "/users");
        first.setCombinerClass(FirstSeenCombiner.class);
        MultipleOutputs.addNamedOutput(
                first, "bounds", TextOutputFormat.class, Text.class, NullWritable.class);
        AnalyticsSupport.run(first);
        Map<String, Integer> bounds = readBounds(configuration, output + "/users/bounds/part-*");
        for (Map.Entry<String, Integer> entry : bounds.entrySet()) {
            configuration.setInt("cohort.last." + entry.getKey(), entry.getValue());
        }
        // Job 2 按 SOURCE、月份计数，每用户月一行；重复评分增加事件数，不增加人数。
        Job activity =
                AnalyticsSupport.job(
                        configuration,
                        "MR25 source user month event counts",
                        CohortActivity.class,
                        ActivityMapper.class,
                        ActivityReducer.class,
                        UserMonthKey.class,
                        LongWritable.class,
                        events,
                        output + "/stages/activity");
        activity.setCombinerClass(ActivityCombiner.class);
        AnalyticsSupport.run(activity);
        // Job 3 二次排序使 cohort 先到、活动后到，按用户流式 Join。
        Job joined =
                AnalyticsSupport.job(
                        configuration,
                        "MR25 cohort first tagged user join",
                        CohortActivity.class,
                        CohortMapper.class,
                        JoinReducer.class,
                        JoinKey.class,
                        MonthRecord.class,
                        output + "/users/part-*",
                        output + "/stages/joined");
        MultipleInputs.addInputPath(
                joined,
                new Path(output + "/users/part-*"),
                TextInputFormat.class,
                CohortMapper.class);
        MultipleInputs.addInputPath(
                joined,
                new Path(output + "/stages/activity/part-*"),
                TextInputFormat.class,
                ActiveMonthMapper.class);
        joined.setPartitionerClass(JoinPartitioner.class);
        // 指定 reduce 分组身份，允许排序键中的额外字段只参与组内排序。
        joined.setGroupingComparatorClass(JoinGrouping.class);
        AnalyticsSupport.run(joined);
        // Job 4 每 cohort 只有少量月份桶，两个汇总 Reducer 足够，不盲目与前面并行度一致。
        Job cells =
                AnalyticsSupport.job(
                        configuration,
                        "MR25 cohort offset histogram",
                        CohortActivity.class,
                        CellMapper.class,
                        CellReducer.class,
                        Text.class,
                        CohortTotals.class,
                        output + "/stages/joined/part-*",
                        output + "/cells");
        int summaryReducers = configuration.getInt("cohort.summary.reducers", 2);
        if (summaryReducers < 1) {
            throw new IllegalArgumentException("cohort.summary.reducers must be positive");
        }
        cells.setNumReduceTasks(summaryReducers);
        cells.setCombinerClass(CellCombiner.class);
        AnalyticsSupport.run(cells);
        // 将分母与删失规则固化为元数据，不能把未知月份解释为流失。
        Map<String, Object> definition = new LinkedHashMap<>();
        definition.put("definition_version", 2);
        definition.put("first_active", "min UTC month over SOURCE only; passive TARGET excluded");
        definition.put(
                "active",
                "distinct SOURCE user/month; event_count retains every valid rating event");
        definition.put("comparison_min_users", configuration.getInt("cohort.min.users", 30));
        definition.put("comparison_horizons", Arrays.asList(1, 3, 6));
        definition.put(
                "comparison",
                "exact offset month, not cumulative; cohort >= min users; month strictly before"
                        + " cutoff");
        definition.put("cutoff_epoch_month", bounds);
        definition.put("future_cells", "N/A, not_observed; cutoff month may be partial");
        try (FSDataOutputStream out = fs.create(new Path(output + "/_definition.json"), false)) {
            out.write(AnalyticsSupport.JSON.writeValueAsBytes(definition));
        }
        // 按发布开关决定是否写 HBase；关闭时只生成 HDFS 结果。
        if (configuration.getBoolean("analytics.publish", true)) {
            AnalyticsSupport.publishStreaming(configuration, "mr25", output);
        }
    }
}
