package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;

/**
 * MR27：A→B、B→C为正，但A→C为负的可解释反例。
 * 负边锚定的三段 Join 等价于先枚举全部正向二跳再与负边半连接，但避免输出无关正路径。
 * 保存每条直接负边（包括零中介）以保留真实分母；不执行MR5推荐或MR16通用三环分析。
 */
public class PathConflicts {

    /**
     * SOURCE/TARGET 是本阶段连接身份；kind和neighbor只参与组内二次排序。
     */
    public static class JoinKey implements WritableComparable<JoinKey> {

        String platform = "";
        int source;
        int target;
        byte kind;
        int neighbor;

        public JoinKey() {}

        JoinKey(String platform, int source, int target, int kind, int neighbor) {
            this.platform = platform;
            this.source = source;
            this.target = target;
            this.kind = (byte) kind;
            this.neighbor = neighbor;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, platform);
            out.writeInt(source);
            out.writeInt(target);
            out.writeByte(kind);
            out.writeInt(neighbor);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // 覆盖上次复用对象的平台，避免Hadoop重用Writable时残留字段。
            platform = Text.readString(in);
            source = in.readInt();
            target = in.readInt();
            kind = in.readByte();
            neighbor = in.readInt();
        }

        int compareIdentity(JoinKey other) {
            int order = platform.compareTo(other.platform);
            if (order == 0) {
                order = Integer.compare(source, other.source);
            }
            return order != 0 ? order : Integer.compare(target, other.target);
        }

        @Override
        public int compareTo(JoinKey other) {
            int order = compareIdentity(other);
            if (order == 0) {
                order = Byte.compare(kind, other.kind);
            }
            return order != 0 ? order : Integer.compare(neighbor, other.neighbor);
        }

        @Override
        public int hashCode() {
            return Objects.hash(platform, source, target);
        }
    }

    /**
     * 值中重复来源和邻居，不能依赖遍历时被Hadoop覆盖的排序键。
     */
    public static class TaggedRating implements Writable {

        byte kind;
        int neighbor;
        int rating;
        int directRating;

        public TaggedRating() {}

        TaggedRating(int kind, int neighbor, int rating, int directRating) {
            this.kind = (byte) kind;
            this.neighbor = neighbor;
            this.rating = rating;
            this.directRating = directRating;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(kind);
            out.writeInt(neighbor);
            out.writeInt(rating);
            // 直接负评分独立保存，不能被正向路径分数替换。
            out.writeInt(directRating);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            kind = in.readByte();
            neighbor = in.readInt();
            rating = in.readInt();
            directRating = in.readInt();
        }
    }

    /** 只按当前连接身份分区，忽略kind和neighbor。 */
    public static class IdentityPartitioner extends Partitioner<JoinKey, TaggedRating> {

        @Override
        public int getPartition(JoinKey key, TaggedRating value, int partitions) {
            // 同一连接身份相遇；记录来源和邻居绝不能参与分区。
            return (key.hashCode() & Integer.MAX_VALUE) % partitions;
        }
    }

    /** 按平台及当前两端身份归组，保留组内来源和邻居排序。 */
    public static class IdentityGrouping extends WritableComparator {

        public IdentityGrouping() {
            super(JoinKey.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            return ((JoinKey) left).compareIdentity((JoinKey) right);
        }
    }

    static void configureJoin(Job job) {
        job.setPartitionerClass(IdentityPartitioner.class);
        job.setGroupingComparatorClass(IdentityGrouping.class);
        // 自然排序保留kind、neighbor，使小侧记录先到、活动侧可流式处理。
    }

    /** 按SOURCE发送负边(kind=0)与正边(kind=1)，让负边先到。 */
    public static class SourceMapper extends Mapper<LongWritable, Text, JoinKey, TaggedRating> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按本阶段固定列序拆分记录；原始平台和SOURCE/TARGET身份不能串位。
            String[] fields = row.toString().split("\t", -1);
            int source = Integer.parseInt(fields[1]);
            int target = Integer.parseInt(fields[2]);
            int rating = Integer.parseInt(fields[3]);
            if (source == target || rating == 0) {
                return;
            }
            // 负边kind=0先到；正边kind=1后到。按共同SOURCE连接，不按平台聚合整图。
            int kind = rating < 0 ? 0 : 1;
            context.write(
                    new JoinKey(fields[0], source, 0, kind, target),
                    new TaggedRating(kind, target, rating, 0));
        }
    }

    /** 连接同一SOURCE的正负出边，生成待验证的A→B→C候选。 */
    public static class SourceReducer extends Reducer<JoinKey, TaggedRating, Text, NullWritable> {

        @Override
        public void reduce(JoinKey key, Iterable<TaggedRating> values, Context context)
                throws IOException, InterruptedException {
            String platform = key.platform;
            int source = key.source;
            List<int[]> negativeTargets = new ArrayList<>();
            for (TaggedRating value : values) {
                if (value.kind == 0) {
                    // 仅保存一个SOURCE的负向出邻接小侧，绝不缓存整个平台。
                    negativeTargets.add(new int[] {value.neighbor, value.rating});
                } else {
                    for (int[] negative : negativeTargets) {
                        if (value.neighbor == negative[0]) {
                            continue;
                        }
                        context.write(
                                new Text(
                                        platform
                                                + "\t"
                                                + source
                                                + "\t"
                                                + value.neighbor
                                                + "\t"
                                                + negative[0]
                                                + "\t"
                                                + value.rating
                                                + "\t"
                                                + negative[1]),
                                NullWritable.get());
                        context.getCounter("MR27", "ANCHORED_WEDGES").increment(1);
                    }
                }
            }
        }
    }

    /** 按B、C发送真实正边，作为半连接先到的基准侧。 */
    public static class PositiveEdgeMapper
            extends Mapper<LongWritable, Text, JoinKey, TaggedRating> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按本阶段固定列序拆分记录；原始平台和SOURCE/TARGET身份不能串位。
            String[] fields = row.toString().split("\t");
            int rating = Integer.parseInt(fields[3]);
            if (rating > 0) {
                // B→C边先于待验证(A,B,C)记录到达，缺失边不能按正边补齐。
                context.write(
                        new JoinKey(
                                fields[0],
                                Integer.parseInt(fields[1]),
                                Integer.parseInt(fields[2]),
                                0,
                                0),
                        new TaggedRating(0, 0, rating, 0));
            }
        }
    }

    /** 将候选路径按B、C重新分组，值中保留A和两条已知边评分。 */
    public static class WedgeMapper extends Mapper<LongWritable, Text, JoinKey, TaggedRating> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按本阶段固定列序拆分记录；原始平台和SOURCE/TARGET身份不能串位。
            String[] fields = row.toString().split("\t");
            int source = Integer.parseInt(fields[1]);
            int middle = Integer.parseInt(fields[2]);
            int target = Integer.parseInt(fields[3]);
            context.write(
                    new JoinKey(fields[0], middle, target, 1, source),
                    new TaggedRating(
                            1, source, Integer.parseInt(fields[4]), Integer.parseInt(fields[5])));
        }
    }

    /** 仅在真实B→C正边存在时输出唯一冲突三元组。 */
    public static class PathReducer extends Reducer<JoinKey, TaggedRating, Text, NullWritable> {

        @Override
        public void reduce(JoinKey key, Iterable<TaggedRating> values, Context context)
                throws IOException, InterruptedException {
            String platform = key.platform;
            int middle = key.source;
            int target = key.target;
            Integer positiveRating = null;
            int previousSource = -1;
            for (TaggedRating value : values) {
                if (value.kind == 0) {
                    if (positiveRating != null) {
                        throw new IOException("Duplicate positive snapshot edge");
                    }
                    positiveRating = value.rating;
                } else {
                    if (value.neighbor == previousSource) {
                        throw new IOException("Duplicate conflict triple");
                    }
                    previousSource = value.neighbor;
                    // 半连接：没有真实B→C正边就丢弃。
                    if (positiveRating == null) {
                        continue;
                    }
                    if (value.directRating >= 0 || value.rating <= 0) {
                        throw new IOException("Invalid path signs");
                    }
                    double weight = value.rating * positiveRating / 100.0;
                    context.write(
                            new Text(
                                    platform
                                            + "\t"
                                            + value.neighbor
                                            + "\t"
                                            + target
                                            + "\t"
                                            + middle
                                            + "\t"
                                            + value.rating
                                            + "\t"
                                            + positiveRating
                                            + "\t"
                                            + value.directRating
                                            + "\t"
                                            + weight),
                            NullWritable.get());
                    context.getCounter("MR27", "CONFIRMED_PATHS").increment(1);
                }
            }
        }
    }

    /** 发送全部直接负边作为基准，保留零中介负边。 */
    public static class NegativeEdgeMapper
            extends Mapper<LongWritable, Text, JoinKey, TaggedRating> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按本阶段固定列序拆分记录；原始平台和SOURCE/TARGET身份不能串位。
            String[] fields = row.toString().split("\t");
            int rating = Integer.parseInt(fields[3]);
            if (rating < 0) {
                // 全部直接负边先到，零中介的负边也必须输出，不能丢掉分母。
                context.write(
                        new JoinKey(
                                fields[0],
                                Integer.parseInt(fields[1]),
                                Integer.parseInt(fields[2]),
                                0,
                                0),
                        new TaggedRating(0, 0, rating, 0));
            }
        }
    }

    /** 按A、C发送已确认路径，携带中介和整数评分乘积。 */
    public static class ConfirmedPathMapper
            extends Mapper<LongWritable, Text, JoinKey, TaggedRating> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按本阶段固定列序拆分记录；原始平台和SOURCE/TARGET身份不能串位。
            String[] fields = row.toString().split("\t");
            // 两段乘积保留为整数；最终除100，避免不同合并次序的浮点误差。
            int numerator = Integer.parseInt(fields[4]) * Integer.parseInt(fields[5]);
            context.write(
                    new JoinKey(
                            fields[0],
                            Integer.parseInt(fields[1]),
                            Integer.parseInt(fields[2]),
                            1,
                            Integer.parseInt(fields[3])),
                    new TaggedRating(
                            1,
                            Integer.parseInt(fields[3]),
                            numerator,
                            Integer.parseInt(fields[6])));
        }
    }

    /** 按直接负边汇总不同中介数、路径支持分与最大单路径分值。 */
    public static class EdgeReducer extends Reducer<JoinKey, TaggedRating, Text, NullWritable> {

        @Override
        public void reduce(JoinKey key, Iterable<TaggedRating> values, Context context)
                throws IOException, InterruptedException {
            String platform = key.platform;
            int source = key.source;
            int target = key.target;
            Integer directRating = null;
            long mediators = 0;
            long supportNumerator = 0;
            int maximumNumerator = 0;
            int previousMiddle = -1;
            for (TaggedRating value : values) {
                if (value.kind == 0) {
                    if (directRating != null) {
                        throw new IOException("Duplicate direct negative edge");
                    }
                    directRating = value.rating;
                } else {
                    if (directRating == null
                            || value.directRating != directRating
                            || value.neighbor == previousMiddle) {
                        throw new IOException("Invalid or duplicate negative path join");
                    }
                    previousMiddle = value.neighbor;
                    mediators++;
                    supportNumerator += value.rating;
                    maximumNumerator = Math.max(maximumNumerator, value.rating);
                }
            }
            if (directRating == null) {
                throw new IOException("Missing direct negative edge");
            }
            context.write(
                    new Text(
                            platform
                                    + "\t"
                                    + source
                                    + "\t"
                                    + target
                                    + "\t"
                                    + directRating
                                    + "\t"
                                    + mediators
                                    + "\t"
                                    + supportNumerator / 100.0
                                    + "\t"
                                    + maximumNumerator / 100.0
                                    + "\t"
                                    + (mediators > 0)),
                    NullWritable.get());
        }
    }

    public static void main(String[] arguments) throws Exception {
        Configuration configuration = new Configuration();
        String[] paths = new GenericOptionsParser(configuration, arguments).getRemainingArgs();
        if (paths.length > 1) {
            throw new IllegalArgumentException("Expected optional output root");
        }
        String root = paths.length == 0 ? "/bitcoin/results/mr27" : paths[0];
        if (FileSystem.get(configuration).exists(new Path(root))) {
            throw new IOException("Output exists: " + root);
        }
        // 校验输入指纹后复用最新非自环快照；不提交任何禁选业务任务。
        String snapshot = AnalyticsSupport.prepare(configuration) + "/part-*";
        Job wedges =
                AnalyticsSupport.job(
                        configuration,
                        "MR27 negative anchored source join",
                        PathConflicts.class,
                        SourceMapper.class,
                        SourceReducer.class,
                        JoinKey.class,
                        TaggedRating.class,
                        snapshot,
                        root + "/stages/wedges");
        configureJoin(wedges);
        // 等待负边锚定连接完成；该阶段只产生待验证B→C的路径候选。
        AnalyticsSupport.run(wedges);
        Job confirmed =
                AnalyticsSupport.job(
                        configuration,
                        "MR27 positive edge semi join",
                        PathConflicts.class,
                        PositiveEdgeMapper.class,
                        PathReducer.class,
                        JoinKey.class,
                        TaggedRating.class,
                        snapshot,
                        root + "/paths");
        MultipleInputs.addInputPath(
                confirmed, new Path(snapshot), TextInputFormat.class, PositiveEdgeMapper.class);
        MultipleInputs.addInputPath(
                confirmed,
                new Path(root + "/stages/wedges/part-*"),
                TextInputFormat.class,
                WedgeMapper.class);
        configureJoin(confirmed);
        // 等待真实正向末边半连接完成，此时输出才是有效冲突三元组。
        AnalyticsSupport.run(confirmed);
        Job edges =
                AnalyticsSupport.job(
                        configuration,
                        "MR27 all negative edges and mediator summary",
                        PathConflicts.class,
                        NegativeEdgeMapper.class,
                        EdgeReducer.class,
                        JoinKey.class,
                        TaggedRating.class,
                        snapshot,
                        root + "/edges");
        MultipleInputs.addInputPath(
                edges, new Path(snapshot), TextInputFormat.class, NegativeEdgeMapper.class);
        MultipleInputs.addInputPath(
                edges,
                new Path(root + "/paths/part-*"),
                TextInputFormat.class,
                ConfirmedPathMapper.class);
        configureJoin(edges);
        // 保留全部负边并汇总中介数，不能仅从非空路径推算覆盖率分母。
        AnalyticsSupport.run(edges);
        if (configuration.getBoolean("analytics.publish", true)) {
            AnalyticsSupport.publishStreaming(configuration, "mr27", root);
        }
    }
}
