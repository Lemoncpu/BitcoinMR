package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.math.BigDecimal;
import java.time.*;
import java.util.*;

/**
 * MR18：用户评价窗口突发分析。固定 UTC 日窗，不是欺诈分类器。
 * 四个物理 Job：日目标聚合 → 日窗口汇总 → 时间二次排序与历史基线 → 异常用户汇总。
 * 历史从用户第一次主动评价日起算，之前日历日中的无事件日按零计入分母。
 * 只使用当前窗口之前的数据；至少七个历史日，且观察值达到 max(5, mean+3*std) 才标记。
 */
public class ActivityBursts {
    // 预先固定规则，不能看过全量结果后再调阈值去追求更多异常。
    public static final int MIN_HISTORY_DAYS = 7;
    public static final int MIN_EVENTS = 5;

    /** 平台、SOURCE、UTC 日、TARGET 的复合键；用于精确统计日内不同目标。 */
    public static class TargetDayKey implements WritableComparable<TargetDayKey> {
        String platform = "";
        int source;
        long day;
        int target;

        public TargetDayKey() {}

        TargetDayKey(String platform, int source, long day, int target) {
            // 原始 SOURCE 与 TARGET 始终保留方向，不能交换。
            this.platform = platform;
            this.source = source;
            this.day = day;
            this.target = target;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, platform);
            out.writeInt(source);
            out.writeLong(day);
            out.writeInt(target);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            platform = Text.readString(in);
            source = in.readInt();
            day = in.readLong();
            target = in.readInt();
        }

        @Override
        public int compareTo(TargetDayKey other) {
            int order = platform.compareTo(other.platform);
            if (order == 0) {
                order = Integer.compare(source, other.source);
            }
            if (order == 0) {
                order = Long.compare(day, other.day);
            }
            return order != 0 ? order : Integer.compare(target, other.target);
        }

        @Override
        public int hashCode() {
            // 完整身份求哈希，相同日目标一定同分区，不同用户可分散。
            return Objects.hash(platform, source, day, target);
        }
    }

    /** 可结合计数，不携带目标集合，避免把一个用户整天的目标放进内存 Set。 */
    public static class EventCounts implements Writable {
        long events;
        long negative;

        public EventCounts() {}

        EventCounts(long events, long negative) {
            this.events = events;
            this.negative = negative;
        }

        void add(EventCounts other) {
            events += other.events;
            // 将当前局部值累加到RATING 小于零的事件次数，保留可结合统计。
            negative += other.negative;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(events);
            out.writeLong(negative);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            events = in.readLong();
            negative = in.readLong();
        }
    }

    /** 按平台、SOURCE、UTC日期、TARGET发送评分与负评次数。 */
    public static class EventMapper extends Mapper<LongWritable, Text, TargetDayKey, EventCounts> {
        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 输入为已校验的六字段事件：platform SOURCE TARGET RATING TIME eventId。
            String[] fields = row.toString().split("\t", -1);
            // 仅在分日时舍弃非负小数秒；UTC 避免操作系统时区改变日边界。
            long day =
                    Instant.ofEpochSecond(new BigDecimal(fields[4]).longValue())
                            .atZone(ZoneOffset.UTC)
                            .toLocalDate()
                            .toEpochDay();
            int rating = Integer.parseInt(fields[3]);
            // 同一目标的多次评价仍计多次事件，但下个阶段只计一个独立目标。
            context.write(
                    new TargetDayKey(
                            fields[0],
                            Integer.parseInt(fields[1]),
                            day,
                            Integer.parseInt(fields[2])),
                    new EventCounts(1, rating < 0 ? 1 : 0));
        }
    }

    /** 局部累计同一用户日目标的评分和负评次数。 */
    public static class EventCombiner
            extends Reducer<TargetDayKey, EventCounts, TargetDayKey, EventCounts> {
        @Override
        public void reduce(TargetDayKey key, Iterable<EventCounts> values, Context context)
                throws IOException, InterruptedException {
            EventCounts total = new EventCounts();
            for (EventCounts value : values) {
                total.add(value);
            }
            context.write(key, total);
        }
    }

    /** 每个用户日目标输出一行，保留事件次数并精确去重目标。 */
    public static class TargetReducer
            extends Reducer<TargetDayKey, EventCounts, Text, NullWritable> {
        @Override
        public void reduce(TargetDayKey key, Iterable<EventCounts> values, Context context)
                throws IOException, InterruptedException {
            EventCounts total = new EventCounts();
            for (EventCounts value : values) {
                total.add(value);
            }
            // 每组只输出一个目标，因此后续可用行数精确统计 distinct_targets。
            context.write(
                    new Text(
                            key.platform
                                    + "\t"
                                    + key.source
                                    + "\t"
                                    + LocalDate.ofEpochDay(key.day)
                                    + "\t"
                                    + key.target
                                    + "\t"
                                    + total.events
                                    + "\t"
                                    + total.negative),
                    NullWritable.get());
        }
    }

    /** 日窗口键：默认按完整日期分组；基线阶段用自定义分组只看平台与用户。 */
    public static class UserDayKey implements WritableComparable<UserDayKey> {
        String platform = "";
        int source;
        long day;

        public UserDayKey() {}

        UserDayKey(String platform, int source, long day) {
            this.platform = platform;
            this.source = source;
            this.day = day;
        }

        int compareUser(UserDayKey other) {
            int order = platform.compareTo(other.platform);
            return order != 0 ? order : Integer.compare(source, other.source);
        }

        @Override
        public int compareTo(UserDayKey other) {
            int order = compareUser(other);
            return order != 0 ? order : Long.compare(day, other.day);
        }

        @Override
        public int hashCode() {
            return Objects.hash(platform, source, day);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, platform);
            out.writeInt(source);
            out.writeLong(day);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            platform = Text.readString(in);
            source = in.readInt();
            day = in.readLong();
        }
    }

    /** 日计数的代数状态，day 用于校验 Combiner 不跨窗口合并。 */
    public static class DailyCounts implements Writable {
        long day = Long.MIN_VALUE;
        long events;
        long negative;
        long targets;

        public DailyCounts() {}

        DailyCounts(long day, long events, long negative, long targets) {
            this.day = day;
            this.events = events;
            this.negative = negative;
            this.targets = targets;
        }

        void add(DailyCounts other) {
            if (day != Long.MIN_VALUE && day != other.day) {
                throw new IllegalArgumentException("Cannot combine different UTC days");
            }
            day = other.day;
            events += other.events;
            // 将当前局部值累加到RATING 小于零的事件次数，保留可结合统计。
            negative += other.negative;
            // 将当前局部值累加到日内独立 TARGET 数，保留可结合统计。
            targets += other.targets;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(day);
            out.writeLong(events);
            out.writeLong(negative);
            out.writeLong(targets);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            day = in.readLong();
            events = in.readLong();
            negative = in.readLong();
            targets = in.readLong();
        }
    }

    /** 将唯一目标行转为用户每日统计贡献，不同目标数加一。 */
    public static class DailyMapper extends Mapper<LongWritable, Text, UserDayKey, DailyCounts> {
        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            long day = LocalDate.parse(fields[2]).toEpochDay();
            context.write(
                    new UserDayKey(fields[0], Integer.parseInt(fields[1]), day),
                    new DailyCounts(day, Long.parseLong(fields[4]), Long.parseLong(fields[5]), 1));
        }
    }

    /** 局部合并同一用户日期的事件、负评及目标计数。 */
    public static class DailyCombiner
            extends Reducer<UserDayKey, DailyCounts, UserDayKey, DailyCounts> {
        @Override
        public void reduce(UserDayKey key, Iterable<DailyCounts> values, Context context)
                throws IOException, InterruptedException {
            DailyCounts total = new DailyCounts();
            for (DailyCounts value : values) {
                total.add(value);
            }
            context.write(key, total);
        }
    }

    /** 输出用户当天完整的评分活动统计。 */
    public static class DailyReducer extends Reducer<UserDayKey, DailyCounts, Text, NullWritable> {
        @Override
        public void reduce(UserDayKey key, Iterable<DailyCounts> values, Context context)
                throws IOException, InterruptedException {
            DailyCounts total = new DailyCounts();
            for (DailyCounts value : values) {
                total.add(value);
            }
            context.write(
                    new Text(
                            key.platform
                                    + "\t"
                                    + key.source
                                    + "\t"
                                    + LocalDate.ofEpochDay(key.day)
                                    + "\t"
                                    + total.events
                                    + "\t"
                                    + total.negative
                                    + "\t"
                                    + total.targets),
                    NullWritable.get());
        }
    }

    /** 只按用户分区，不把日期放入哈希，否则同一用户的历史无法按序处理。 */
    public static class UserPartitioner extends Partitioner<UserDayKey, DailyCounts> {
        @Override
        public int getPartition(UserDayKey key, DailyCounts value, int partitions) {
            return (Objects.hash(key.platform, key.source) & Integer.MAX_VALUE) % partitions;
        }
    }

    /** 按用户归组，日期只用于组内升序排序。 */
    public static class UserGrouping extends WritableComparator {
        public UserGrouping() {
            super(UserDayKey.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            // 日期只参与组内升序，不拆开同一用户的历史。
            return ((UserDayKey) left).compareUser((UserDayKey) right);
        }
    }

    /** 将每日汇总重组为按用户日期二次排序的输入。 */
    public static class BaselineMapper extends Mapper<LongWritable, Text, UserDayKey, DailyCounts> {
        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            long day = LocalDate.parse(fields[2]).toEpochDay();
            // 日期同时写入值；分组迭代时不能依赖可能被框架复用的 key.day。
            context.write(
                    new UserDayKey(fields[0], Integer.parseInt(fields[1]), day),
                    new DailyCounts(
                            day,
                            Long.parseLong(fields[3]),
                            Long.parseLong(fields[4]),
                            Long.parseLong(fields[5])));
        }
    }

    /** 按日期流式维护历史基线，先判断当天再更新历史。 */
    public static class BaselineReducer
            extends Reducer<UserDayKey, DailyCounts, Text, NullWritable> {
        private MultipleOutputs<Text, NullWritable> outputs;

        @Override
        protected void setup(Context context) {
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(UserDayKey key, Iterable<DailyCounts> values, Context context)
                throws IOException, InterruptedException {
            String platform = key.platform;
            int source = key.source;
            long firstDay = Long.MIN_VALUE;
            long previousDay = Long.MIN_VALUE;
            double eventSum = 0;
            double eventSquares = 0;
            double negativeSum = 0;
            double negativeSquares = 0;
            for (DailyCounts value : values) {
                // 输入必须是唯一日窗并严格按日期升序，重复或乱序就拒绝计算基线。
                if (value.day <= previousDay) {
                    throw new IOException("Daily rows must be unique and ascending");
                }
                if (firstDay == Long.MIN_VALUE) {
                    firstDay = value.day;
                }
                // 缺失的日历日相当于事件数零，增加分母但不需要生成海量补零行。
                long historyDays = value.day - firstDay;
                // 使用当前窗之前的累计值除以历史日数，当前窗尚未加入基线。
                double mean = historyDays == 0 ? 0 : eventSum / historyDays;
                // 以日计数平方和计算总体方差，max(0,...) 仅消除浮点舍入产生的小负数。
                double variance =
                        historyDays == 0
                                ? 0
                                : Math.max(0, eventSquares / historyDays - mean * mean);
                // 总体方差开平方得到历史标准差，供固定的三倍标准差阈值使用。
                double std = Math.sqrt(variance);
                // 使用当前窗之前的累计值除以历史日数，当前窗尚未加入基线。
                double negativeMean = historyDays == 0 ? 0 : negativeSum / historyDays;
                // 以日计数平方和计算总体方差，max(0,...) 仅消除浮点舍入产生的小负数。
                double negativeVariance =
                        historyDays == 0
                                ? 0
                                : Math.max(
                                        0,
                                        negativeSquares / historyDays
                                                - negativeMean * negativeMean);
                // 总体方差开平方得到历史标准差，供固定的三倍标准差阈值使用。
                double negativeStd = Math.sqrt(negativeVariance);
                // 标准差为零时也要求至少五条，避免一条事件就被标为异常。
                double eventThreshold = Math.max(MIN_EVENTS, mean + 3 * std);
                double negativeThreshold = Math.max(MIN_EVENTS, negativeMean + 3 * negativeStd);
                boolean enoughHistory = historyDays >= MIN_HISTORY_DAYS;
                boolean burst = enoughHistory && value.events >= eventThreshold;
                boolean negativeBurst = enoughHistory && value.negative >= negativeThreshold;
                String status =
                        !enoughHistory
                                ? "insufficient_history"
                                : (burst || negativeBurst ? "candidate" : "normal");
                String result =
                        platform
                                + "\t"
                                + source
                                + "\t"
                                + LocalDate.ofEpochDay(value.day)
                                + "\t"
                                + value.events
                                + "\t"
                                + value.negative
                                + "\t"
                                + value.targets
                                + "\t"
                                + historyDays
                                + "\t"
                                + mean
                                + "\t"
                                + std
                                + "\t"
                                + eventThreshold
                                + "\t"
                                + negativeMean
                                + "\t"
                                + negativeStd
                                + "\t"
                                + negativeThreshold
                                + "\t"
                                + (burst ? 1 : 0)
                                + "\t"
                                + (negativeBurst ? 1 : 0)
                                + "\t"
                                + status;
                context.write(new Text(result), NullWritable.get());
                if (burst || negativeBurst) {
                    outputs.write("alerts", new Text(result), NullWritable.get(), "alerts/part");
                    context.getCounter("MR18", "ALERT_WINDOWS").increment(1);
                }
                // 历史不足窗口也保留明细并计数，不能把没有基线当作正常。
                if (!enoughHistory) {
                    context.getCounter("MR18", "INSUFFICIENT_HISTORY_WINDOWS").increment(1);
                }
                // 当前窗输出之后才更新基线，严禁将当前窗或未来窗算进自身历史。
                eventSum += value.events;
                eventSquares += (double) value.events * value.events;
                // 当前窗输出之后才把观察值加入历史，防止自身信息泄漏。
                negativeSum += value.negative;
                negativeSquares += (double) value.negative * value.negative;
                previousDay = value.day;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }

    /** sum/sum/max 的可结合用户摘要，不平均局部比率。 */
    public static class AlertTotals implements Writable {
        long alerts;
        long negativeAlerts;
        long maximumEvents;

        public AlertTotals() {}

        AlertTotals(long alerts, long negativeAlerts, long maximumEvents) {
            this.alerts = alerts;
            this.negativeAlerts = negativeAlerts;
            this.maximumEvents = maximumEvents;
        }

        void add(AlertTotals other) {
            alerts += other.alerts;
            negativeAlerts += other.negativeAlerts;
            maximumEvents = Math.max(maximumEvents, other.maximumEvents);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeLong(alerts);
            out.writeLong(negativeAlerts);
            out.writeLong(maximumEvents);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            alerts = in.readLong();
            negativeAlerts = in.readLong();
            maximumEvents = in.readLong();
        }
    }

    /** 只读取待核查日期，按用户发送次数及峰值贡献。 */
    public static class AlertMapper extends Mapper<LongWritable, Text, Text, AlertTotals> {
        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            // 用户摘要只聚合候选日，不能把 normal 或历史不足窗计入告警数。
            context.write(
                    new Text(fields[0] + "\t" + fields[1]),
                    new AlertTotals(1, Long.parseLong(fields[14]), Long.parseLong(fields[3])));
        }
    }

    /** 局部累计待核查次数，评分峰值取max。 */
    public static class AlertCombiner extends Reducer<Text, AlertTotals, Text, AlertTotals> {
        @Override
        public void reduce(Text key, Iterable<AlertTotals> values, Context context)
                throws IOException, InterruptedException {
            AlertTotals total = new AlertTotals();
            for (AlertTotals value : values) {
                total.add(value);
            }
            context.write(key, total);
        }
    }

    /** 输出每名用户的待核查次数、负评突增次数和相关评分峰值。 */
    public static class AlertReducer extends Reducer<Text, AlertTotals, Text, NullWritable> {
        @Override
        public void reduce(Text key, Iterable<AlertTotals> values, Context context)
                throws IOException, InterruptedException {
            AlertTotals total = new AlertTotals();
            for (AlertTotals value : values) {
                total.add(value);
            }
            // 输出用户候选窗口数及峰值，不把它解释为欺诈概率。
            context.write(
                    new Text(
                            key
                                    + "\t"
                                    + total.alerts
                                    + "\t"
                                    + total.negativeAlerts
                                    + "\t"
                                    + total.maximumEvents),
                    NullWritable.get());
        }
    }

    /** 独立入口；所有聚合和基线计算都通过真实 MapReduce 完成。 */
    public static void main(String[] arguments) throws Exception {
        Configuration configuration = new Configuration();
        String[] paths = new GenericOptionsParser(configuration, arguments).getRemainingArgs();
        String output = paths.length == 0 ? "/bitcoin/results/mr18" : paths[0];
        // 按允许的参数数量检查或选择路径，不能把配置或单个路径误解释为完整输入输出。
        if (paths.length > 1) {
            throw new IllegalArgumentException("Expected optional output root");
        }
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path(output))) {
            throw new IOException("Output exists; inspect/remove manually: " + output);
        }
        // 先准备合法事件和通用快照，不能假定中间数据已存在。
        AnalyticsSupport.prepare(configuration);
        String events =
                configuration.get("analytics.base", AnalyticsSupport.BASE) + "/events/part-*";
        // Job 1 以日+目标分组，精确去重目标但保留重复事件次数。
        Job targets =
                AnalyticsSupport.job(
                        configuration,
                        "MR18 daily target counts",
                        ActivityBursts.class,
                        EventMapper.class,
                        TargetReducer.class,
                        TargetDayKey.class,
                        EventCounts.class,
                        events,
                        output + "/stages/targets");
        targets.setCombinerClass(EventCombiner.class);
        AnalyticsSupport.run(targets);
        // Job 2 汇总日事件数、负评数和独立目标数。
        Job daily =
                AnalyticsSupport.job(
                        configuration,
                        "MR18 daily window totals",
                        ActivityBursts.class,
                        DailyMapper.class,
                        DailyReducer.class,
                        UserDayKey.class,
                        DailyCounts.class,
                        output + "/stages/targets/part-*",
                        output + "/stages/daily");
        daily.setCombinerClass(DailyCombiner.class);
        AnalyticsSupport.run(daily);
        // Job 3 按用户分组、日期升序，常量历史状态即可避免未来信息泄漏。
        Job baseline =
                AnalyticsSupport.job(
                        configuration,
                        "MR18 chronological historical baseline",
                        ActivityBursts.class,
                        BaselineMapper.class,
                        BaselineReducer.class,
                        UserDayKey.class,
                        DailyCounts.class,
                        output + "/stages/daily/part-*",
                        output + "/windows");
        baseline.setPartitionerClass(UserPartitioner.class);
        // 指定 reduce 分组身份，允许排序键中的额外字段只参与组内排序。
        baseline.setGroupingComparatorClass(UserGrouping.class);
        MultipleOutputs.addNamedOutput(
                baseline, "alerts", TextOutputFormat.class, Text.class, NullWritable.class);
        AnalyticsSupport.run(baseline);
        // 仅整理已提交的命名输出目录，不在客户端做任何异常判断或计数。
        Path nestedAlerts = new Path(output + "/windows/alerts");
        Path alerts = new Path(output + "/alerts");
        if (fs.exists(nestedAlerts)) {
            if (!fs.rename(nestedAlerts, alerts)) {
                throw new IOException("Cannot publish alert dataset");
            }
        } else {
            fs.mkdirs(alerts);
            fs.create(new Path(alerts, "part-r-00000"), false).close();
        }
        fs.create(new Path(alerts, "_SUCCESS"), false).close();
        Job users =
                AnalyticsSupport.job(
                        configuration,
                        "MR18 candidate user summary",
                        ActivityBursts.class,
                        AlertMapper.class,
                        AlertReducer.class,
                        Text.class,
                        AlertTotals.class,
                        output + "/alerts/part-*",
                        output + "/users");
        users.setCombinerClass(AlertCombiner.class);
        AnalyticsSupport.run(users);
        if (configuration.getBoolean("analytics.publish", true)) {
            AnalyticsSupport.publishStreaming(configuration, "mr18", output);
        }
    }
}
