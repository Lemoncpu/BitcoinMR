package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * MR7 的共享分布式流水线与 Job 驱动。
 *
 * 阶段顺序：关系快照 → 节点统计与结构状态 → Katz 多轮传播（均分跳过）
 * → 分区排序 → 全局名次偏移与分数 Join → HBase 分片导入。
 * 所有核心分组都包含平台和具体节点，驱动只收集分区计数与误差标量，不加载整张图。
 */
public class DistributedReputation {

    /** 用户指定的全平台 L1 停止阈值；导入校验使用同一常量。 */
    public static final double KATZ_CONVERGENCE_THRESHOLD = 0.01;

    /**
     * 节点业务复合键，隔离不同平台上编号相同的用户。
     * platform 对应 alpha/otc，userId 对应 SOURCE 或 TARGET。
     * 比较顺序为平台升序、用户编号升序；序列化顺序固定，供 Shuffle 排序和分组使用。
     */
    public static class NodeKey implements WritableComparable<NodeKey> {

        String platform = "";
        int userId;

        public NodeKey() {}

        NodeKey(String platform, int userId) {
            this.platform = platform;
            this.userId = userId;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, platform);
            out.writeInt(userId);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            platform = Text.readString(in);
            userId = in.readInt();
        }

        @Override
        public int compareTo(NodeKey other) {
            // 先按平台升序比较，Alpha 和 OTC 的相同用户编号不能成为同一业务键。
            int order = platform.compareTo(other.platform);
            return order != 0 ? order : Integer.compare(userId, other.userId);
        }

        @Override
        public int hashCode() {
            return 31 * platform.hashCode() + userId;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof NodeKey && compareTo((NodeKey) other) == 0;
        }

        @Override
        public String toString() {
            return platform + "\t" + userId;
        }
    }

    /**
     * 节点哈希分区器。
     * 使用平台和用户编号的完整哈希，而不是只对平台分区；同一节点的结构状态、
     * 入边统计和传播贡献必然进入同一个 Reducer，同平台的不同节点可以分散并行处理。
     */
    public static class NodePartitioner extends Partitioner<NodeKey, NodeMessage> {

        @Override
        public int getPartition(NodeKey key, NodeMessage value, int count) {
            return (key.hashCode() & Integer.MAX_VALUE) % count;
        }
    }

    /**
     * 带业务来源标记的 Writable：避免把结构记录与数值贡献混合求和。
     *
     * INCOMING：incomingCount=评价次数，ratingSum=评分总和；
     * EDGE：adjacency 保存一条 TARGET:RATING 出边；
     * STATE：incomingCount=入度，score=上一轮分数，ratingSum=出边归一化分母，adjacency=出边表；
     * CONTRIBUTION：score=源节点传播来的贡献；PRESENCE：只表示端点存在。
     * 只有 INCOMING 与 CONTRIBUTION 的数值可以结合；STATE 和 EDGE 必须保留原始记录。
     */
    public static class NodeMessage implements Writable {

        // INCOMING 表示入边统计，负载是评分总和与评价次数。
        static final byte INCOMING = 1;

        // EDGE 表示源节点的一条出边，不能与数值贡献混合求和。
        static final byte EDGE = 2;

        // STATE 表示唯一节点结构，携带上一轮分数与邻接信息。
        static final byte STATE = 3;

        // CONTRIBUTION 表示沿边传播的数值，可以先由 Combiner 求和。
        static final byte CONTRIBUTION = 4;

        // PRESENCE 只说明节点出现过，不增加入度或得分。
        static final byte PRESENCE = 5;
        byte tag;
        long incomingCount;
        double ratingSum;
        double score;
        String adjacency = "";

        public NodeMessage() {}

        static NodeMessage incoming(long count, double ratingSum) {
            return create(INCOMING, count, ratingSum, 0, "");
        }

        static NodeMessage edge(String adjacency) {
            return create(EDGE, 0, 0, 0, adjacency);
        }

        static NodeMessage presence() {
            return create(PRESENCE, 0, 0, 0, "");
        }

        static NodeMessage contribution(double score) {
            return create(CONTRIBUTION, 0, 0, score, "");
        }

        static NodeMessage state(
                long incomingCount, double denominator, double previousScore, String adjacency) {
            return create(STATE, incomingCount, denominator, previousScore, adjacency);
        }

        private static NodeMessage create(
                byte tag, long count, double ratingSum, double score, String adjacency) {
            NodeMessage value = new NodeMessage();
            // 填入来源标记，后续不能仅凭字段是否为零猜测消息类型。
            value.tag = tag;
            value.incomingCount = count;
            value.ratingSum = ratingSum;
            value.score = score;
            value.adjacency = adjacency;
            return value;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeByte(tag);
            out.writeLong(incomingCount);
            out.writeDouble(ratingSum);
            out.writeDouble(score);
            Text.writeString(out, adjacency);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            // 先读消息来源标记，后续同一负载槽位必须按这个类型解释。
            tag = in.readByte();
            incomingCount = in.readLong();
            ratingSum = in.readDouble();
            score = in.readDouble();
            adjacency = Text.readString(in);
        }
    }

    /**
     * 把最新关系投影到节点统计和出边结构。
     * 输入 TSV：platform、SOURCE、TARGET、RATING、TIME、eventId。
     * 按 TARGET 输出入边次数和评分；Katz 还按 SOURCE 输出出边。
     * 直接均分不需要出边结构，因此跳过 EDGE 消息，减少 Shuffle 数据。
     */
    public static class SnapshotNodeMapper
            extends Mapper<LongWritable, Text, NodeKey, NodeMessage> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            String[] fields = row.toString().split("\t");
            // 解析快照第 2 列 SOURCE，明确评分发起者的身份。
            int source = Integer.parseInt(fields[1]);
            // 解析快照第 3 列 TARGET，它决定入边统计和贡献路由目标。
            int target = Integer.parseInt(fields[2]);
            double rating = Double.parseDouble(fields[3]);
            // 发送入边评分与次数；若来自 Combiner，次数可能已经是局部累计值。
            context.write(new NodeKey(fields[0], target), NodeMessage.incoming(1, rating));
            // 只有 Katz 需要源节点出边，均分不发送 EDGE，减少 Shuffle。
            if (!context.getConfiguration().get("reputation.method").equals("direct_average")) {
                // 以 SOURCE 为键发送 TARGET:RATING，供源节点计算出边分母和结构。
                context.write(
                        new NodeKey(fields[0], source), NodeMessage.edge(target + ":" + rating));
            }
        }
    }

    /**
     * 外连接的节点存在性输入。
     * 从清洗事件分别输出 SOURCE 与 TARGET，保证仅发出评分、没有入边以及只有自环的用户
     * 也能进入结果；PRESENCE 本身不增加入度，不改变评分。
     */
    public static class PresenceMapper extends Mapper<LongWritable, Text, NodeKey, NodeMessage> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            String[] fields = row.toString().split("\t");
            context.write(
                    new NodeKey(fields[0], Integer.parseInt(fields[1])), NodeMessage.presence());
            context.write(
                    new NodeKey(fields[0], Integer.parseInt(fields[2])), NodeMessage.presence());
        }
    }

    /**
     * Map 端可重复执行的局部聚合。
     * 分别累加入边次数/评分总和与贡献值，合并重复的存在性标记；
     * 出边和节点结构状态逐条透传。不能平均局部均分，也不能合并两个 STATE。
     */
    public static class MessageCombiner
            extends Reducer<NodeKey, NodeMessage, NodeKey, NodeMessage> {

        @Override
        public void reduce(NodeKey node, Iterable<NodeMessage> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            double sum = 0;
            double contribution = 0;
            boolean hasIncoming = false;
            boolean hasContribution = false;
            boolean present = false;
            // 流式消费本键的所有值，只提取字段，不持有 Hadoop 可能复用的对象。
            for (NodeMessage value : values) {
                // 入边统计单独累计评分与次数，不把它当作传播贡献。
                if (value.tag == NodeMessage.INCOMING) {
                    hasIncoming = true;
                    // 累计评价次数；Combiner 合并后的单条次数可能已经大于一。
                    count += value.incomingCount;
                    // 累加评分总和，不能用局部均分直接求平均替代总和与次数。
                    sum += value.ratingSum;
                } else if (value.tag == NodeMessage.CONTRIBUTION) {
                    // 只有贡献消息的 score 槽位参与本轮贡献求和。
                    hasContribution = true;
                    contribution += value.score;
                } else if (value.tag == NodeMessage.PRESENCE) {
                    // 存在标记只保留节点，不增加入度或评分。
                    present = true;
                } else {
                    // 输出合并的统计或贡献；结构消息原样透传，不能当成数值相加。
                    context.write(node, value);
                }
            }
            // 只有本组实际收到入边统计才输出小计，避免凭空增加零记录。
            if (hasIncoming) {
                // 发送入边评分与次数；若来自 Combiner，次数可能已经是局部累计值。
                context.write(node, NodeMessage.incoming(count, sum));
            }
            if (hasContribution) {
                // 以 TARGET 为键发送 0.8×RATING/分母×旧分数，由目标 Reducer 汇总来源贡献。
                context.write(node, NodeMessage.contribution(contribution));
            }
            if (present) {
                context.write(node, NodeMessage.presence());
            }
        }
    }

    /**
     * 为每个节点生成初始状态，不按平台构造全量图。
     * 输出 TSV：平台、用户 ID、入度、得分、出边归一化分母、邻接表。
     * 均分=sum(RATING)/入度，无入边输出 N/A；Katz 初始得分为 1。
     * 正向方法的分母仅累加正评分，带符号方法累加所有出边评分的绝对值。
     */
    public static class InitReducer extends Reducer<NodeKey, NodeMessage, Text, NullWritable> {

        @Override
        public void reduce(NodeKey node, Iterable<NodeMessage> values, Context context)
                throws IOException, InterruptedException {
            long count = 0;
            double sum = 0;
            double denominator = 0;
            StringJoiner edges = new StringJoiner(",");
            String method = context.getConfiguration().get("reputation.method");
            // 流式消费本键的所有值，只提取字段，不持有 Hadoop 可能复用的对象。
            for (NodeMessage value : values) {
                // 入边统计单独累计评分与次数，不把它当作传播贡献。
                if (value.tag == NodeMessage.INCOMING) {
                    // 累计评价次数；Combiner 合并后的单条次数可能已经大于一。
                    count += value.incomingCount;
                    // 累加评分总和，不能用局部均分直接求平均替代总和与次数。
                    sum += value.ratingSum;
                }
                // 出边用于源节点结构与分母，不计入本用户收到的评分。
                if (value.tag == NodeMessage.EDGE) {
                    edges.add(value.adjacency);
                    // 取出当前出边评分，参与正向或绝对值分母求和。
                    double rating = Double.parseDouble(value.adjacency.split(":")[1]);
                    // 正向方法仅累加正评分，带符号方法累加绝对值，不能把负边塞进正向分母。
                    denominator +=
                            method.equals("positive_katz") ? Math.max(0, rating) : Math.abs(rating);
                }
            }
            // 状态字段：平台、节点、入度、得分、归一化分母、邻接表。仅保存单节点出边。
            String score =
                    method.equals("direct_average")
                            ? (count == 0 ? "N/A" : Double.toString(sum / count))
                            : "1.0";
            context.write(
                    new Text(
                            node + "\t" + count + "\t" + score + "\t" + denominator + "\t" + edges),
                    NullWritable.get());
        }
    }

    /**
     * Katz 单轮的贡献分发。
     * 读取上一轮 HDFS 状态，向本节点发送唯一 STATE，向每个 TARGET 发送贡献。
     * 正向方法跳过非正边；带符号方法保留负贡献；分母为零时不传播。
     * 所有贡献都使用上一轮分数，不能在 Map 内直接更新目标节点。
     */
    public static class PropagateMapper extends Mapper<LongWritable, Text, NodeKey, NodeMessage> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            double previous = Double.parseDouble(fields[3]);
            double denominator = Double.parseDouble(fields[4]);
            // 将旧分数、入度、出边分母和邻接表发回自身，保留唯一结构侧。
            context.write(
                    new NodeKey(fields[0], Integer.parseInt(fields[1])),
                    NodeMessage.state(Long.parseLong(fields[2]), denominator, previous, fields[5]));
            // 根据算法名决定是否过滤非正边，保证两种 Katz 口径明确。
            boolean positive =
                    context.getConfiguration().get("reputation.method").equals("positive_katz");
            // 有可传播出边且分母大于零才计算贡献，避免除零。
            if (denominator > 0 && !fields[5].isEmpty()) {
                // 仅遍历本节点的出边，贡献按目标重新分组，不在此遍历整张图。
                for (String edge : fields[5].split(",")) {
                    // 按约定分隔符提取一条出边的 TARGET 与 RATING，不改变原始业务值。
                    String[] relation = edge.split(":");
                    // 从 TARGET:RATING 的第二项读取评分，下一步计算沿该边传播的贡献。
                    double rating = Double.parseDouble(relation[1]);
                    // 正向方法跳过零边和负边；带符号方法保留负贡献。
                    if (positive && rating <= 0) {
                        // 本条非正边不参与正向 Katz，继续处理下一条出边。
                        continue;
                    }
                    // 以 TARGET 为键发送 0.8×RATING/分母×旧分数，由目标 Reducer 汇总来源贡献。
                    context.write(
                            new NodeKey(fields[0], Integer.parseInt(relation[0])),
                            NodeMessage.contribution(.8 * rating / denominator * previous));
                }
            }
        }
    }

    /**
     * 按目标节点执行结构状态与数值贡献的 Reduce-side Join。
     * 必须恰好有一条 STATE；缺失或重复会使任务失败，避免静默丢节点。
     * 下一轮分数=1+全部贡献之和；输出完整新状态供下一轮读取。
     * 同时累计分区内各平台的 L1 差，cleanup 只写误差小计，不回传全量节点。
     */
    public static class PropagateReducer extends Reducer<NodeKey, NodeMessage, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> outputs;

        // 为本 Reducer 按平台累计的 L1建立容器，不同业务维度单独保存。
        private final Map<String, Double> partitionErrors = new HashMap<>();

        @Override
        protected void setup(Context context) {
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(NodeKey node, Iterable<NodeMessage> values, Context context)
                throws IOException, InterruptedException {
            // 为旧入度设置占位值，稍后必须从唯一 STATE 恢复；本轮不重新统计图的入度。
            long incoming = 0;
            // 为上一轮分数预留槽位，读取 STATE 后覆盖它，不能把此占位零当作实际旧分数。
            double previous = 0;
            // 先设置占位分母，稍后从 STATE 还原已计算的出边权重和。
            double denominator = 0;
            double contributions = 0;
            String adjacency = null;
            // 流式消费本键的所有值，只提取字段，不持有 Hadoop 可能复用的对象。
            for (NodeMessage value : values) {
                // 读取唯一结构消息，恢复旧分数、入度、分母和邻接表。
                if (value.tag == NodeMessage.STATE) {
                    // 结构消息已经存在，再遇 STATE 就是重复，不能任意覆盖。
                    if (adjacency != null) {
                        throw new IOException("Duplicate STATE: " + node);
                    }
                    adjacency = value.adjacency;
                    // 从 STATE 恢复固定图上的入度，传播过程不改变关系数量。
                    incoming = value.incomingCount;
                    // 从 STATE 取出真实旧分数，用它和新分数计算本节点变化量。
                    previous = value.score;
                    // STATE 的 ratingSum 槽位存放出边归一化分母，此处按结构消息含义还原。
                    denominator = value.ratingSum;
                } else if (value.tag == NodeMessage.CONTRIBUTION) {
                    // 只有贡献消息的 score 槽位参与本轮贡献求和。
                    // 累加一个来源节点的带符号贡献，不把结构状态计入贡献。
                    contributions += value.score;
                } else {
                    // 本阶段只接受 STATE 与 CONTRIBUTION，出现其他标记说明消息路由错误，直接失败。
                    throw new IOException("Unexpected Join tag");
                }
            }
            // 缺少结构时拒绝更新，不能只依据贡献或候选凭空补出节点。
            if (adjacency == null) {
                throw new IOException("Missing STATE: " + node);
            }
            double next = 1 + contributions;
            // 计算本节点新旧分数绝对差，供分区 L1 与全局停止判断使用。
            double delta = Math.abs(next - previous);
            if (!Double.isFinite(next)) {
                // 新分数为 NaN 或无穷大时失败，不能把它写入状态再交给排名。
                throw new IOException("Non-finite Katz state");
            }
            context.write(
                    new Text(
                            node
                                    + "\t"
                                    + incoming
                                    + "\t"
                                    + next
                                    + "\t"
                                    + denominator
                                    + "\t"
                                    + adjacency),
                    NullWritable.get());
            // 只按平台累加当前 Reducer 的误差小计，驱动不会收回所有节点。
            partitionErrors.merge(node.platform, delta, Double::sum);
            context.getCounter("REPUTATION", "UPDATED_NODES").increment(1);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // 每个平台只输出本分区一个误差小计，驱动最多归并平台数乘 Reducer 数条记录。
            for (Map.Entry<String, Double> error : partitionErrors.entrySet()) {
                // 每平台仅写本分区的 L1 小计，驱动只需归并少量标量。
                outputs.write(
                        "errors",
                        new Text(error.getKey() + "\t" + error.getValue()),
                        NullWritable.get(),
                        "errors/part");
            }
            outputs.close();
        }
    }

    /**
     * 全局排名的复合排序键。
     * 按平台升序、得分降序、用户 ID 升序排序；用户 ID 只稳定展示次序，
     * 不打破同分用户共享名次的业务规则。
     */
    public static class ScoreKey implements WritableComparable<ScoreKey> {

        String platform = "";
        double score;
        int userId;

        public ScoreKey() {}

        ScoreKey(String platform, double score, int userId) {
            this.platform = platform;
            this.score = score;
            this.userId = userId;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, platform);
            out.writeDouble(score);
            out.writeInt(userId);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            platform = Text.readString(in);
            score = in.readDouble();
            userId = in.readInt();
        }

        @Override
        public int compareTo(ScoreKey other) {
            // 先按平台升序比较，Alpha 和 OTC 的相同用户编号不能成为同一业务键。
            int order = platform.compareTo(other.platform);
            if (order == 0) {
                order = Double.compare(other.score, score);
            }
            return order != 0 ? order : Integer.compare(userId, other.userId);
        }
    }

    /**
     * 显式排序比较器，使用 ScoreKey 的业务比较顺序。
     * 排序发生在 Hadoop Shuffle，Reducer 不收集全量用户再调用集合排序。
     */
    public static class ScoreSortComparator extends WritableComparator {

        public ScoreSortComparator() {
            super(ScoreKey.class, true);
        }

        @Override
        public int compare(WritableComparable left, WritableComparable right) {
            return ((ScoreKey) left).compareTo((ScoreKey) right);
        }
    }

    /**
     * 排名阶段按完整排序键分组，每个用户独立进入 reduce。
     * 相邻同分用户之间的竞赛名次由 RankReducer 维护；不能按整个平台合并一个巨型分组。
     */
    public static class ScoreGroupingComparator extends ScoreSortComparator {}

    /**
     * 按分数划分连续区间，而不是对用户 ID 做哈希。
     * 高分区间的分区编号更小，相同分数必然在同一区间，保证分区名次可通过人数偏移拼接。
     * 范围划分保证顺序，不能保证每个区间人数完全均衡。
     */
    public static class ScorePartitioner extends Partitioner<ScoreKey, Text> {

        static int bucket(double score, int partitions) {
            double position = .5 - Math.atan(score / 4) / Math.PI;
            return Math.min(partitions - 1, Math.max(0, (int) (position * partitions)));
        }

        @Override
        public int getPartition(ScoreKey key, Text value, int partitions) {
            // 按得分范围分区，同分不会跨分区，后续才可用人数修正名次。
            return bucket(key.score, partitions);
        }
    }

    /**
     * 筛选入榜用户并构造排序键。
     * 只有入度至少 5 且具有数值得分的用户进入排名；其他节点仍保留在最终分数表中。
     */
    public static class RankMapper extends Mapper<LongWritable, Text, ScoreKey, Text> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            // 仅对入度至少五且分数有效的用户排名，其他用户在最终左连接中保留。
            if (Long.parseLong(fields[2]) >= 5 && !fields[3].equals("N/A")) {
                context.write(
                        new ScoreKey(
                                fields[0],
                                Double.parseDouble(fields[3]),
                                Integer.parseInt(fields[1])),
                        new Text(
                                fields[0] + "\t" + fields[1] + "\t" + fields[2] + "\t"
                                        + fields[3]));
            }
        }
    }

    /**
     * 流式计算当前分区的竞赛排名与每平台人数。
     * 同分保持前一个名次，分数改变时取当前位置，得到 1、1、3 的规则。
     * 同时写出 platform、partitionId、count 小表，供下阶段修正全局名次。
     */
    public static class RankReducer extends Reducer<ScoreKey, Text, Text, NullWritable> {

        String platform = "";
        long position = 0;
        long rank = 0;
        double previous = Double.NaN;

        private MultipleOutputs<Text, NullWritable> outputs;

        @Override
        protected void setup(Context context) {
            outputs = new MultipleOutputs<>(context);
        }

        private void count(Context context) throws IOException, InterruptedException {
            // 已经处理过某个平台才写分区人数，避免初始化时输出空平台记录。
            if (!platform.isEmpty()) {
                // 记录平台、分区号和人数，计算全局名次偏移时只累加同平台高分区人数。
                outputs.write(
                        "counts",
                        new Text(
                                platform
                                        + "\t"
                                        + context.getTaskAttemptID().getTaskID().getId()
                                        + "\t"
                                        + position),
                        NullWritable.get(),
                        "counts/part");
            }
        }

        @Override
        public void reduce(ScoreKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 切换平台时先保存人数，再重置名次状态，不能混算两个网络。
            if (!platform.equals(key.platform)) {
                count(context);
                platform = key.platform;
                // 切换平台后清空分区人数，不能把另一平台的用户加到名次中。
                position = 0;
                previous = Double.NaN;
            }
            position++;
            // 首位或分数变化时更新名次，同分沿用之前的名次。
            if (position == 1 || Double.compare(previous, key.score) != 0) {
                // 分数变化时取当前位置作为名次；同分不更新，因此得到 1、1、3。
                rank = position;
            }
            previous = key.score;
            // 流式消费本键的所有值，只提取字段，不持有 Hadoop 可能复用的对象。
            for (Text value : values) {
                context.write(
                        new Text(
                                value
                                        + "\t"
                                        + rank
                                        + "\t"
                                        + context.getTaskAttemptID().getTaskID().getId()),
                        NullWritable.get());
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            count(context);
            outputs.close();
        }
    }

    /**
     * 分区人数小表与局部名次的广播 Join。
     * setup 只加载每平台每分区的计数，内存与 Reducer 数量成正比。
     * 全局名次=当前局部名次+所有更高分区的人数，然后按节点发送 RANK 记录。
     */
    public static class RankedMapper extends Mapper<LongWritable, Text, NodeKey, Text> {

        Map<String, long[]> counts = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException {
            int reducers = context.getConfiguration().getInt("analytics.reducers", 4);
            // 逐行读取到文件末尾，避免整体加载大文件。
            for (String line :
                    readLines(
                            context.getConfiguration(),
                            context.getConfiguration().get("rank.counts"))) {
                String[] fields = line.split("\t");
                // 按平台建人数数组，并将该排序分区人数填入对应位置；只广播小表，不加载用户。
                counts.computeIfAbsent(fields[0], key -> new long[reducers])[
                                Integer.parseInt(fields[1])] =
                        Long.parseLong(fields[2]);
            }
        }

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            String[] fields = row.toString().split("\t");
            long rank = Long.parseLong(fields[4]);
            int partition = Integer.parseInt(fields[5]);
            // 只累计高分区的人数，当前分区和低分区不能加进偏移。
            for (int index = 0; index < partition; index++) {
                rank += counts.get(fields[0])[index];
            }
            context.write(
                    new NodeKey(fields[0], Integer.parseInt(fields[1])), new Text("RANK\t" + rank));
        }
    }

    /**
     * 最终分数侧的 Join 输入。
     * 按节点发送 SCORE 标记，保留全部节点的入度与原始得分，避免未入榜用户被内连接丢弃。
     */
    public static class FinalStateMapper extends Mapper<LongWritable, Text, NodeKey, Text> {

        @Override
        public void map(LongWritable offset, Text row, Context context)
                throws IOException, InterruptedException {
            // 按制表符拆分本阶段中间记录，-1 保留末尾空邻接字段，孤立节点不会少一列。
            String[] fields = row.toString().split("\t", -1);
            // 发送所有节点的原始分数与入度，保证未入榜用户也能出现在最终表。
            context.write(
                    new NodeKey(fields[0], Integer.parseInt(fields[1])),
                    new Text("SCORE\t" + fields[2] + "\t" + fields[3]));
        }
    }

    /**
     * 按节点合并 SCORE 与可选的 RANK，执行左外连接。
     * 没有名次时输出 null，无入边均分保留 N/A；每个平台每个输出文件写一次 CSV 表头。
     * MultipleOutputs 与主输出一起由 Hadoop OutputCommitter 提交。
     */
    public static class FinalReducer extends Reducer<NodeKey, Text, Text, NullWritable> {

        private MultipleOutputs<Text, NullWritable> outputs;

        Set<String> headers = new HashSet<>();

        @Override
        protected void setup(Context context) {
            outputs = new MultipleOutputs<>(context);
        }

        @Override
        public void reduce(NodeKey node, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            // 默认保留未入榜标记；只有收到 RANK 消息才替换，左连接不会丢掉其他用户。
            String rank = "null";
            String incoming = null;
            String score = null;
            // 流式消费本键的所有值，只提取字段，不持有 Hadoop 可能复用的对象。
            for (Text value : values) {
                String[] fields = value.toString().split("\t");
                if (fields[0].equals("RANK")) {
                    rank = fields[1];
                } else {
                    // 同一用户分数侧只能有一条 SCORE，重复会使结果不唯一。
                    if (incoming != null) {
                        throw new IOException("Duplicate SCORE");
                    }
                    incoming = fields[1];
                    score = fields[2];
                }
            }
            // 只有名次没有分数说明连接输入缺失，不能输出半条记录。
            if (incoming == null) {
                throw new IOException("Missing SCORE");
            }
            String path = node.platform + "/scores";
            // 首次加入平台返回 true，本 Reducer 对该平台只写一次 CSV 表头。
            if (headers.add(node.platform)) {
                outputs.write(
                        "csv",
                        new Text("platform,user_id,in_degree,score,rank"),
                        NullWritable.get(),
                        path);
            }
            outputs.write(
                    "csv",
                    new Text(
                            node.platform
                                    + ","
                                    + node.userId
                                    + ","
                                    + incoming
                                    + ","
                                    + score
                                    + ","
                                    + rank),
                    NullWritable.get(),
                    path);
            context.write(
                    new Text(node + "\t" + incoming + "\t" + score + "\t" + rank),
                    NullWritable.get());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outputs.close();
        }
    }

    static List<String> readLines(Configuration configuration, String pattern) throws IOException {
        List<String> lines = new ArrayList<>();
        Path path = new Path(pattern);
        FileSystem fs = path.getFileSystem(configuration);
        FileStatus[] files = fs.globStatus(path);
        if (files == null) {
            return lines;
        }
        // 取文件路径作为稳定排序依据，只排序文件元信息，不排序用户全量结果。
        Arrays.sort(files, Comparator.comparing(file -> file.getPath().toString()));
        for (FileStatus file : files) {
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    fs.open(file.getPath()), StandardCharsets.UTF_8))) {
                // 预留本次读取的一行文本，循环中每次只替换这一行，不缓存整个大文件。
                String line;
                // 逐行读取到文件末尾，避免整体加载大文件。
                while ((line = reader.readLine()) != null) {
                    lines.add(line);
                }
            }
        }
        return lines;
    }

    /**
     * MR7 的 Job 编排入口，只有驱动负责启动下一轮，Reducer 不启动子任务。
     *
     * 1. 解析 -D 配置与可选 input/output，检查固定输出和中间目录是否已存在。
     * 2. 校验输入指纹并准备 MR7 最新关系，按节点初始化均分或 Katz 状态。
     * 3. Katz 每轮提交独立传播 Job，等待成功后合并各 Reducer 的平台误差小计。
     * 4. 两个平台 L1 均小于 0.01 才停止；超出最大轮数则抛异常，不发布结果。
     * 5. 通过范围分区和 Shuffle 排序生成全局排名，保存原始分数及收敛历史。
     * 6. analytics.publish=true 时提交分片导入，成功后才切换 HBase 当前批次。
     *
     * @param method 仅由独立入口传入 direct_average、positive_katz 或 signed_katz
     * @param arguments Hadoop 通用参数以及可选的输入、输出路径
     * @throws Exception 任一阶段失败立即中止；程序不自动删除用户已有输出
     */
    public static void run(String method, String[] arguments) throws Exception {
        Configuration configuration = new Configuration();
        String[] paths = new GenericOptionsParser(configuration, arguments).getRemainingArgs();
        // 按允许的参数数量检查或选择路径，不能把配置或单个路径误解释为完整输入输出。
        if (paths.length != 0 && paths.length != 2) {
            throw new IllegalArgumentException("Expected optional <input> <output>");
        }
        // 按允许的参数数量检查或选择路径，不能把配置或单个路径误解释为完整输入输出。
        if (paths.length == 2) {
            configuration.set("analytics.input", paths[0]);
        }
        configuration.set("reputation.method", method);
        String output = paths.length == 2 ? paths[1] : "/bitcoin/results/" + method;
        String stages = output + "-stages";
        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path(output))
                || fs.exists(new Path(stages))
                || fs.exists(new Path(output + "-hbase"))) {
            throw new IOException(
                    "Output/stages/HBase import directory exists; inspect and remove before rerun: "
                            + output
                            + ", "
                            + stages);
        }
        // 校验指纹并准备 MR7 专用最新关系，完整且匹配的缓存才允许复用。
        String snapshot = AnalyticsSupport.prepareReputation(configuration);
        String base = configuration.get("analytics.base", AnalyticsSupport.BASE);
        Job initial =
                AnalyticsSupport.job(
                        configuration,
                        "MR7 " + method + " node aggregation",
                        DistributedReputation.class,
                        SnapshotNodeMapper.class,
                        InitReducer.class,
                        NodeKey.class,
                        NodeMessage.class,
                        snapshot + "/part-*",
                        stages + "/initial");
        // MultipleInputs 使用不同 Mapper 标记关系与节点存在记录，Reduce-side Join 合并。
        // 为关系快照和节点存在数据注册不同 Mapper，随后按节点做 Join。
        configurationForInputs(initial, snapshot + "/part-*", base + "/events/part-*");
        initial.setCombinerClass(MessageCombiner.class);
        initial.setPartitionerClass(NodePartitioner.class);
        AnalyticsSupport.run(initial);
        String state = stages + "/initial";
        // 为按平台分别保存的迭代历史建立容器，不同业务维度单独保存。
        Map<String, List<Double>> history = new LinkedHashMap<>();
        // 为此平台建立独立历史列表，两个平台的误差记录不能混在一起。
        history.put("alpha", new ArrayList<>());
        // 为此平台建立独立历史列表，两个平台的误差记录不能混在一起。
        history.put("otc", new ArrayList<>());
        // 仅两种 Katz 执行传播或检查迭代历史，直接均分跳过此分支。
        if (!method.equals("direct_average")) {
            boolean converged = false;
            int maximum = configuration.getInt("katz.max.iterations", 200);
            for (int iteration = 1; iteration <= maximum; iteration++) {
                String next =
                        stages + "/iteration-" + String.format(Locale.ROOT, "%03d", iteration);
                Job propagation =
                        AnalyticsSupport.job(
                                configuration,
                                "MR7 " + method + " propagation " + iteration,
                                DistributedReputation.class,
                                PropagateMapper.class,
                                PropagateReducer.class,
                                NodeKey.class,
                                NodeMessage.class,
                                state + "/part-*",
                                next);
                propagation.setCombinerClass(MessageCombiner.class);
                propagation.setPartitionerClass(NodePartitioner.class);
                MultipleOutputs.addNamedOutput(
                        propagation,
                        "errors",
                        TextOutputFormat.class,
                        Text.class,
                        NullWritable.class);
                AnalyticsSupport.run(propagation);
                // 每个 Reducer 只提交每平台一个局部误差；驱动只归并 O(R×平台数) 个标量。
                // 不使用整数 Counter 截断小数，也不把节点数据收回客户端。
                // 为本轮各平台全局 L1建立容器，不同业务维度单独保存。
                Map<String, Double> deltas = new HashMap<>();
                // 逐行读取到文件末尾，避免整体加载大文件。
                for (String line : readLines(configuration, next + "/errors/part-*")) {
                    String[] fields = line.split("\t");
                    // 按平台汇总所有分区误差标量，得到本轮全局 L1。
                    deltas.merge(fields[0], Double.parseDouble(fields[1]), Double::sum);
                }
                // 先假定本轮达到条件，后续任一平台超阈值都会将它改回 false。
                converged = true;
                // 分别检查 Alpha 和 OTC 的全局误差；任一平台未达标，本轮都不能停止。
                for (String platform : history.keySet()) {
                    Double delta = deltas.get(platform);
                    // 误差缺失或无效时拒绝判断收敛，防止残缺状态被误认为成功。
                    if (delta == null || !Double.isFinite(delta)) {
                        throw new IOException("Missing/invalid L1");
                    }
                    history.get(platform).add(delta);
                    // 该平台 L1 仍不小于 0.01，本轮不能停止。
                    if (delta >= KATZ_CONVERGENCE_THRESHOLD) {
                        // 标记本轮仍未达到停止条件，不能发布最终结果。
                        converged = false;
                    }
                }
                state = next;
                System.out.println(
                        "KATZ method=" + method + " iteration=" + iteration + " deltas=" + deltas);
                if (converged) {
                    break;
                }
            }
            if (!converged) {
                // 超过轮数仍未收敛，停止计算且不发布新批次。
                throw new IOException("Katz not converged; no publication");
            }
        }
        // 进入分布式范围排序与名次修正流水线，此阶段不再改变分数。
        rankAndJoin(configuration, state, output, stages);
        // 依照平台或轮次写出历史记录，不对原始误差做缩放。
        for (Map.Entry<String, List<Double>> entry : history.entrySet()) {
            // 只有执行过迭代才保存历史，直接均分没有 Katz 轮次。
            if (!entry.getValue().isEmpty()) {
                try (FSDataOutputStream stream =
                                fs.create(
                                        new Path(
                                                output
                                                        + "/"
                                                        + entry.getKey()
                                                        + "/iterations-r-00000"),
                                        false);
                        Writer writer = new OutputStreamWriter(stream, StandardCharsets.UTF_8)) {
                    // 先写约定历史表头。
                    writer.write("iteration,l1_delta\n");
                    int iteration = 0;
                    // 输出已保留的局部候选，数量受 Top10 容量约束。
                    for (double delta : entry.getValue()) {
                        // 按真实轮次写原始 L1，供收敛曲线读取，不对数值截断。
                        writer.write((++iteration) + "," + delta + "\n");
                    }
                }
            }
        }
        // 按发布开关决定是否写 HBase；关闭时只生成 HDFS 结果。
        if (configuration.getBoolean("analytics.publish", true)) {
            HdfsToHbase.importMethod(configuration, method, new Path(output));
        }
    }

    /**
     * 排名子流水线：全量数据仍在 HDFS，驱动只广播少量分区计数。
     *
     * 第一个 Job 按 ScoreKey 排序，在各分区生成局部竞赛名次和人数小表；
     * 第二个 Job 将人数偏移加到局部名次，再按节点连接所有分数，保留未入榜用户。
     * 同分不会跨范围分区，因此不需要把全部用户拉回驱动处理并列名次。
     *
     * @param configuration 当前算法配置；包括 Reducer 数量和运行模式
     * @param state 已成功提交的最终状态目录
     * @param output 固定的最终业务输出目录，必须不存在
     * @param stages 当前方法的中间目录，排名结果写入其中的 rank 子目录
     */
    public static void rankAndJoin(
            Configuration configuration, String state, String output, String stages)
            throws Exception {
        String method = configuration.get("reputation.method", "ranking-verification");
        Job ranking =
                AnalyticsSupport.job(
                        configuration,
                        "MR7 " + method + " distributed range sorting",
                        DistributedReputation.class,
                        RankMapper.class,
                        RankReducer.class,
                        ScoreKey.class,
                        Text.class,
                        state + "/part-*",
                        stages + "/rank");
        ranking.setPartitionerClass(ScorePartitioner.class);
        ranking.setSortComparatorClass(ScoreSortComparator.class);
        // 指定 reduce 分组身份，允许排序键中的额外字段只参与组内排序。
        ranking.setGroupingComparatorClass(ScoreGroupingComparator.class);
        MultipleOutputs.addNamedOutput(
                ranking, "counts", TextOutputFormat.class, Text.class, NullWritable.class);
        AnalyticsSupport.run(ranking);
        configuration.set("rank.counts", stages + "/rank/counts/part-*");
        Job finish =
                AnalyticsSupport.job(
                        configuration,
                        "MR7 " + method + " rank offset broadcast and score join",
                        DistributedReputation.class,
                        FinalStateMapper.class,
                        FinalReducer.class,
                        NodeKey.class,
                        Text.class,
                        state + "/part-*",
                        output);
        MultipleInputs.addInputPath(
                finish, new Path(state + "/part-*"), TextInputFormat.class, FinalStateMapper.class);
        MultipleInputs.addInputPath(
                finish,
                new Path(stages + "/rank/part-*"),
                TextInputFormat.class,
                RankedMapper.class);
        MultipleOutputs.addNamedOutput(
                finish, "csv", TextOutputFormat.class, Text.class, NullWritable.class);
        AnalyticsSupport.run(finish);
    }

    private static void configurationForInputs(Job job, String snapshot, String events) {
        MultipleInputs.addInputPath(
                job, new Path(snapshot), TextInputFormat.class, SnapshotNodeMapper.class);
        MultipleInputs.addInputPath(
                job, new Path(events), TextInputFormat.class, PresenceMapper.class);
    }
}
