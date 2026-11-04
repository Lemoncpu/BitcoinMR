package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.util.*;

/**
 * MR26：识别双向评分强度不一致的具体用户对。
 * 流水线为非自环最新关系 → 双向 Self Join → 局部候选与全局 Top10 → HBase。
 * 单向关系不当作另一侧零评分；结果是复核线索，不直接判定用户风险。
 */
public class DirectionalAsymmetry {

    /**
     * 无序用户对复合键：平台、较小用户 ID、较大用户 ID。
     * A→B 和 B→A 规范成同一键，平台仍隔离两个数据集；排序按两个编号数值升序。
     */
    public static class PairKey implements WritableComparable<PairKey> {

        String platform = "";
        int lowerUserId;
        int higherUserId;

        public PairKey() {}

        PairKey(String platform, int source, int target) {
            this.platform = platform;
            lowerUserId = Math.min(source, target);
            higherUserId = Math.max(source, target);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, platform);
            out.writeInt(lowerUserId);
            out.writeInt(higherUserId);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            platform = Text.readString(in);
            lowerUserId = in.readInt();
            higherUserId = in.readInt();
        }

        @Override
        public int compareTo(PairKey other) {
            // 先按平台升序比较，Alpha 和 OTC 的相同用户编号不能成为同一业务键。
            int result = platform.compareTo(other.platform);
            if (result == 0) {
                result = Integer.compare(lowerUserId, other.lowerUserId);
            }
            return result != 0 ? result : Integer.compare(higherUserId, other.higherUserId);
        }

        @Override
        public int hashCode() {
            return 31 * (31 * platform.hashCode() + lowerUserId) + higherUserId;
        }

        @Override
        public String toString() {
            return platform + "\t" + lowerUserId + "\t" + higherUserId;
        }
    }

    /**
     * Join 输入的方向标记与评分 Writable。
     * direction=1 表示较小编号到较大编号，direction=2 表示相反方向，
     * rating 保存原始 RATING；缺失方向不能用 rating=0 代替。
     */
    public static class DirectionRating implements Writable {

        byte direction;
        int rating;

        public DirectionRating() {}

        DirectionRating(boolean forward, int rating) {
            // 正向编码为 1、反向编码为 2，Join 才能分别读取两侧评分。
            direction = (byte) (forward ? 1 : 2);
            this.rating = rating;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(direction);
            out.writeInt(rating);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            direction = in.readByte();
            rating = in.readInt();
        }
    }

    /**
     * 按完整无序用户对哈希分区，使正向与反向记录在同一 Reducer 相遇。
     */
    public static class PairPartitioner extends Partitioner<PairKey, DirectionRating> {

        @Override
        public int getPartition(PairKey pair, DirectionRating rating, int partitions) {
            return (pair.hashCode() & Integer.MAX_VALUE) % partitions;
        }
    }

    /**
     * 将快照关系规范到无序用户对，并附上原始方向标记。
     * SOURCE/TARGET 的大小只影响方向标记，不交换原评分的业务含义。
     */
    public static class PairMapper extends Mapper<LongWritable, Text, PairKey, DirectionRating> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            String[] fields = row.toString().split("\t");
            // 解析快照第 2 列 SOURCE，明确评分发起者的身份。
            int source = Integer.parseInt(fields[1]);
            // 解析快照第 3 列 TARGET，它决定入边统计和贡献路由目标。
            int target = Integer.parseInt(fields[2]);
            context.write(
                    new PairKey(fields[0], source, target),
                    new DirectionRating(source < target, Integer.parseInt(fields[3])));
        }
    }

    /**
     * Reduce-side Self Join，每组只需保存两个方向的评分。
     * 只有两侧都存在才计算绝对差、归一化差与正负冲突；重复方向视为快照错误。
     * 两边均为零时归一化差取零，避免除零。
     */
    public static class PairReducer extends Reducer<PairKey, DirectionRating, Text, NullWritable> {

        @Override
        public void reduce(PairKey pair, Iterable<DirectionRating> values, Context context)
                throws IOException, InterruptedException {
            Integer forward = null;
            Integer reverse = null;
            // 流式消费本键的所有值，只提取字段，不持有 Hadoop 可能复用的对象。
            for (DirectionRating value : values) {
                // 标记 1 表示较小编号到较大编号，记录正向评分。
                if (value.direction == 1) {
                    // 正向快照只能有一条，重复说明上游唯一性契约被破坏。
                    if (forward != null) {
                        throw new IOException("Duplicate forward edge");
                    }
                    forward = value.rating;
                } else if (value.direction == 2) {
                    // 反向快照只能有一条，不能任意覆盖重复评分。
                    if (reverse != null) {
                        throw new IOException("Duplicate reverse edge");
                    }
                    reverse = value.rating;
                } else {
                    throw new IOException("Invalid direction tag");
                }
            }
            // 缺少一个方向就排除，不能把缺失方向当作零评分。
            if (forward == null || reverse == null) {
                context.getCounter("BUSINESS", "EXCLUDED_SINGLE_DIRECTION").increment(1);
                return;
            }
            int difference = Math.abs(forward - reverse);
            double denominator = Math.abs(forward) + Math.abs(reverse);
            boolean conflict = forward * reverse < 0;
            context.getCounter("BUSINESS", "RECIPROCAL_PAIRS").increment(1);
            if (conflict) {
                context.getCounter("BUSINESS", "SIGN_CONFLICTS").increment(1);
            }
            // 输出两侧评分、绝对差、归一化差和冲突标记，只包含真正双向关系。
            context.write(
                    new Text(
                            pair
                                    + "\t"
                                    + forward
                                    + "\t"
                                    + reverse
                                    + "\t"
                                    + difference
                                    + "\t"
                                    + (denominator == 0 ? 0 : difference / denominator)
                                    + "\t"
                                    + (conflict ? 1 : 0)),
                    NullWritable.get());
        }
    }

    private static final Comparator<String> BEST_FIRST =
            (left, right) -> {
                String[] a = left.split("\t");
                String[] b = right.split("\t");
                int order = Integer.compare(Integer.parseInt(b[5]), Integer.parseInt(a[5]));
                if (order == 0) {
                    order = Integer.compare(Integer.parseInt(a[1]), Integer.parseInt(b[1]));
                }
                if (order == 0) {
                    order = Integer.compare(Integer.parseInt(a[2]), Integer.parseInt(b[2]));
                }
                return order;
            };

    private static void retain(PriorityQueue<String> heap, String row) {
        heap.offer(row);
        if (heap.size() > 10) {
            // 删除堆顶最差候选，将容量限制为十条，避免全量内存排序。
            heap.poll();
        }
    }

    // 对双向明细做局部候选堆筛选，后续 Job 再归约为每平台全局 Top10。
    /**
     * 经典局部 Top-N：每个平台只保留最多 10 条候选。
     * 堆顶是当前候选中最差的一条，超出容量立即移除；cleanup 才发送局部候选，
     * 避免把全部双向明细汇集到全局 Reducer。
     */
    public static class TopMapper extends Mapper<LongWritable, Text, Text, Text> {

        private final Map<String, PriorityQueue<String>> heaps = new HashMap<>();

        @Override
        public void map(LongWritable offset, Text value, Context context) {
            String row = value.toString();
            String platform = row.split("\t", 2)[0];
            retain(
                    heaps.computeIfAbsent(
                            platform, key -> new PriorityQueue<>(BEST_FIRST.reversed())),
                    row);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 逐个平台发送局部 Top10，而不是全量明细。
            for (Map.Entry<String, PriorityQueue<String>> entry : heaps.entrySet()) {
                // 输出已保留的局部候选，数量受 Top10 容量约束。
                for (String row : entry.getValue()) {
                    // 仅发送本 Mapper 的局部候选，每平台最多十条，不传全部明细。
                    context.write(new Text(entry.getKey()), new Text(row));
                }
            }
        }
    }

    /**
     * 全局 Top10 归并，只消费各 Mapper 的少量候选。
     * 仍保持容量为 10 的堆，最后对最多 10 条记录排序输出，
     * 这一步不是对全部输入明细做内存排序。
     */
    public static class TopReducer extends Reducer<Text, Text, Text, NullWritable> {

        @Override
        public void reduce(Text platform, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            PriorityQueue<String> heap = new PriorityQueue<>(BEST_FIRST.reversed());
            // 流式消费本键的所有值，只提取字段，不持有 Hadoop 可能复用的对象。
            for (Text value : values) {
                retain(heap, value.toString());
            }
            List<String> ordered = new ArrayList<>(heap);
            // 只对最终最多十条候选排序，其他明细从未被收集到此列表。
            ordered.sort(BEST_FIRST);
            int rank = 0;
            for (String row : ordered) {
                String[] fields = row.split("\t", 2);
                // 按最终候选顺序生成平台全局 Top10，序号是榜单位置。
                context.write(
                        new Text(fields[0] + "\t" + (++rank) + "\t" + fields[1]),
                        NullWritable.get());
            }
        }
    }

    public static void main(String[] arguments) throws Exception {
        Configuration configuration = new Configuration();
        String[] paths = new GenericOptionsParser(configuration, arguments).getRemainingArgs();
        // 先准备合法事件和通用快照，不能假定中间数据已存在。
        String snapshot = AnalyticsSupport.prepare(configuration);
        String output = paths.length > 0 ? paths[0] : "/bitcoin/results/mr26";
        Job details =
                AnalyticsSupport.job(
                        configuration,
                        "MR26 reciprocal score asymmetry join",
                        DirectionalAsymmetry.class,
                        PairMapper.class,
                        PairReducer.class,
                        PairKey.class,
                        DirectionRating.class,
                        snapshot + "/part-*",
                        output + "/details");
        details.setPartitionerClass(PairPartitioner.class);
        AnalyticsSupport.run(details);
        Job top =
                AnalyticsSupport.job(
                        configuration,
                        "MR26 distributed asymmetry Top10",
                        DirectionalAsymmetry.class,
                        TopMapper.class,
                        TopReducer.class,
                        Text.class,
                        Text.class,
                        output + "/details/part-*",
                        output + "/top");
        AnalyticsSupport.run(top);
        // 按发布开关决定是否写 HBase；关闭时只生成 HDFS 结果。
        if (configuration.getBoolean("analytics.publish", true)) {
            AnalyticsSupport.publish(configuration, "mr26", output);
        }
    }
}
