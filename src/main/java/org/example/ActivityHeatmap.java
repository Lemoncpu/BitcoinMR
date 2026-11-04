package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.time.*;

/**
 * MR22：仅对OTC按 UTC 星期与小时统计合法评价事件数量。
 * 使用全部合法历史事件，包括重复评价与自环；活动量与最新关系快照的边数不是同一口径。
 * Map 分桶、Combiner 局部求和、Reducer 汇总后分片写入 HBase。
 */
public class ActivityHeatmap {

    // 每条合法事件只映射到一个 UTC 星期/小时桶；此处不对事件去重。
    /**
     * 把精确事件时间映射为 UTC 时间格。
     * 输出键为平台、星期（周一=1）、小时（0～23），值为计数 1。
     * 只在选择时间格时舍去非负时间的小数秒，不改变上游快照对时间的精确比较。
     */
    public static class HourMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            String[] fields = row.toString().split("\t");
            // Alpha原始时间集中于两个小时，不再纳入本任务；在Map端过滤，不能只隐藏页面。
            if (!fields[0].equals("otc")) {
                context.getCounter("MR22", "EXCLUDED_NON_OTC_EVENTS").increment(1);
                return;
            }
            context.getCounter("MR22", "OTC_EVENTS").increment(1);
            // 只在时间分桶时取非负整秒，并显式使用 UTC，不受机器本地时区影响。
            ZonedDateTime time =
                    Instant.ofEpochSecond(new java.math.BigDecimal(fields[4]).longValue())
                            .atZone(ZoneOffset.UTC);
            context.write(
                    new Text(
                            fields[0]
                                    + "\t"
                                    + time.getDayOfWeek().getValue()
                                    + "\t"
                                    + time.getHour()),
                    new LongWritable(1));
        }
    }

    // 中间值只是计数，求和可结合，因此可在 Map 端先合并，减少 shuffle。
    /**
     * 局部合并相同时间格的事件计数。
     * 求和允许零次或多次执行 Combiner，不影响最终数量；减少相同时间格的网络记录。
     */
    public static class SumCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            // 流式消费本键的所有值，只提取字段，不持有 Hadoop 可能复用的对象。
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(key, new LongWritable(sum));
        }
    }

    /**
     * 对一个平台时间格的局部计数求和，输出结构化 TSV。
     * 没有事件的时间格不伪造数据，页面展示时再补零。
     */
    public static class HourReducer extends Reducer<Text, LongWritable, Text, NullWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            long sum = 0;
            // 流式消费本键的所有值，只提取字段，不持有 Hadoop 可能复用的对象。
            for (LongWritable value : values) {
                sum += value.get();
            }
            context.write(new Text(key + "\t" + sum), NullWritable.get());
        }
    }

    // 准备数据仅首次运行；业务计算是独立 YARN Job，成功后通过 HBase API 发布。
    public static void main(String[] arguments) throws Exception {
        Configuration configuration = new Configuration();
        String[] paths = new GenericOptionsParser(configuration, arguments).getRemainingArgs();
        if (paths.length > 1) {
            throw new IllegalArgumentException("Expected optional output root");
        }
        // 先准备合法事件和通用快照，不能假定中间数据已存在。
        AnalyticsSupport.prepare(configuration);
        String base = configuration.get("analytics.base", AnalyticsSupport.BASE);
        String output = paths.length > 0 ? paths[0] : "/bitcoin/results/mr22";
        Job histogram =
                AnalyticsSupport.job(
                        configuration,
                        "MR22 OTC only UTC weekday/hour activity",
                        ActivityHeatmap.class,
                        HourMapper.class,
                        HourReducer.class,
                        Text.class,
                        LongWritable.class,
                        base + "/events/part-*",
                        output + "/heatmap");
        histogram.setCombinerClass(SumCombiner.class);
        AnalyticsSupport.run(histogram);
        // 按发布开关决定是否写 HBase；关闭时只生成 HDFS 结果。
        if (configuration.getBoolean("analytics.publish", true)) {
            AnalyticsSupport.publishStreaming(configuration, "mr22", output);
        }
    }
}
