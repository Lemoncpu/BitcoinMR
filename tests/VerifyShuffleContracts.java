package org.example;

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.*;

import java.io.*;

/**
 * 验证业务键的序列化契约以及排序/分组/分区的一致性。
 */
public class VerifyShuffleContracts {

    static void require(boolean condition, String message) {
        if (!condition) {
            throw new AssertionError(message);
        }
    }

    static <T extends Writable> T roundTrip(T source, T target) throws IOException {
        DataOutputBuffer bytes = new DataOutputBuffer();
        source.write(bytes);
        DataInputBuffer input = new DataInputBuffer();
        input.reset(bytes.getData(), bytes.getLength());
        target.readFields(input);
        return target;
    }

    public static void main(String[] arguments) throws Exception {
        AnalyticsSupport.EdgeVersionKey older =
                new AnalyticsSupport.EdgeVersionKey(
                        new String[] {"otc", "12", "34", "1", "1609459200.01", "a"});
        AnalyticsSupport.EdgeVersionKey newer =
                new AnalyticsSupport.EdgeVersionKey(
                        new String[] {"otc", "12", "34", "-1", "1609459200.02", "b"});
        require(newer.compareTo(older) < 0, "fractional timestamp must sort descending");
        require(
                roundTrip(newer, new AnalyticsSupport.EdgeVersionKey()).compareTo(newer) == 0,
                "edge roundtrip");
        AnalyticsSupport.EdgeGroupingComparator grouping =
                new AnalyticsSupport.EdgeGroupingComparator();
        require(grouping.compare(older, newer) == 0, "versions must group together");
        AnalyticsSupport.EdgePartitioner partitioner = new AnalyticsSupport.EdgePartitioner();
        for (int count : new int[] {1, 2, 4, 7}) {
            require(
                    partitioner.getPartition(older, new Text(), count)
                            == partitioner.getPartition(newer, new Text(), count),
                    "group crosses partitions");
        }
        AnalyticsSupport.ReputationEdgeKey higherRating =
                new AnalyticsSupport.ReputationEdgeKey(
                        new String[] {"alpha", "1", "2", "7", "1.01", "a"});
        AnalyticsSupport.ReputationEdgeKey laterLowerRating =
                new AnalyticsSupport.ReputationEdgeKey(
                        new String[] {"alpha", "1", "2", "-4", "1.01", "z"});
        require(
                higherRating.compareTo(laterLowerRating) < 0,
                "MR7 must preserve higher RATING at same TIME");
        require(
                roundTrip(higherRating, new AnalyticsSupport.ReputationEdgeKey())
                                .compareTo(higherRating)
                        == 0,
                "reputation edge roundtrip");
        DistributedReputation.NodeKey node = new DistributedReputation.NodeKey("alpha", 1001);
        require(
                roundTrip(node, new DistributedReputation.NodeKey()).compareTo(node) == 0,
                "node roundtrip");
        DistributedReputation.NodeMessage state =
                DistributedReputation.NodeMessage.state(9, 12.0, -.25, "2:1.0,3:-2.0");
        DistributedReputation.NodeMessage copy =
                roundTrip(state, new DistributedReputation.NodeMessage());
        require(
                copy.tag == DistributedReputation.NodeMessage.STATE
                        && copy.incomingCount == 9
                        && copy.score == -.25
                        && copy.ratingSum == 12
                        && copy.adjacency.equals(state.adjacency),
                "join state roundtrip");
        for (int partitions : new int[] {1, 2, 4, 7}) {
            int previous = -1;
            for (int step = 10000; step >= -10000; step--) {
                int bucket =
                        DistributedReputation.ScorePartitioner.bucket(step / 100.0, partitions);
                require(bucket >= previous, "score range order");
                previous = bucket;
            }
        }
        DistributedReputation.ScoreKey score = new DistributedReputation.ScoreKey("alpha", 1.25, 3);
        require(
                roundTrip(score, new DistributedReputation.ScoreKey()).compareTo(score) == 0,
                "score roundtrip");
        DirectionalAsymmetry.PairKey pair = new DirectionalAsymmetry.PairKey("otc", 99, 2);
        require(
                roundTrip(pair, new DirectionalAsymmetry.PairKey())
                                .compareTo(new DirectionalAsymmetry.PairKey("otc", 2, 99))
                        == 0,
                "canonical pair roundtrip");
        DirectionalAsymmetry.DirectionRating rating =
                roundTrip(
                        new DirectionalAsymmetry.DirectionRating(false, -10),
                        new DirectionalAsymmetry.DirectionRating());
        require(rating.direction == 2 && rating.rating == -10, "direction tag roundtrip");
        verifyMessageFactories();
        verifyHbaseRows();
        System.out.println(
                "PASS: Writable roundtrips, exact fractional time, group/partition consistency,"
                    + " descending range order, Join tags, named message factories, HBase row"
                    + " identity");
    }

    /** 检查命名工厂仍使用原有Writable字段协议，尤其是带负数的数值槽。 */
    private static void verifyMessageFactories() throws IOException {
        DistributedReputation.NodeMessage incoming =
                roundTrip(
                        DistributedReputation.NodeMessage.incoming(3, -2.5),
                        new DistributedReputation.NodeMessage());
        require(
                incoming.tag == DistributedReputation.NodeMessage.INCOMING
                        && incoming.incomingCount == 3
                        && incoming.ratingSum == -2.5
                        && incoming.score == 0
                        && incoming.adjacency.isEmpty(),
                "incoming factory protocol");
        DistributedReputation.NodeMessage edge =
                roundTrip(
                        DistributedReputation.NodeMessage.edge("2:-3.0"),
                        new DistributedReputation.NodeMessage());
        require(
                edge.tag == DistributedReputation.NodeMessage.EDGE
                        && edge.adjacency.equals("2:-3.0")
                        && edge.incomingCount == 0
                        && edge.ratingSum == 0
                        && edge.score == 0,
                "edge factory protocol");
        DistributedReputation.NodeMessage presence =
                roundTrip(
                        DistributedReputation.NodeMessage.presence(),
                        new DistributedReputation.NodeMessage());
        require(
                presence.tag == DistributedReputation.NodeMessage.PRESENCE
                        && presence.incomingCount == 0
                        && presence.ratingSum == 0
                        && presence.score == 0
                        && presence.adjacency.isEmpty(),
                "presence factory protocol");
        DistributedReputation.NodeMessage contribution =
                roundTrip(
                        DistributedReputation.NodeMessage.contribution(-0.8),
                        new DistributedReputation.NodeMessage());
        require(
                contribution.tag == DistributedReputation.NodeMessage.CONTRIBUTION
                        && contribution.score == -0.8
                        && contribution.incomingCount == 0
                        && contribution.ratingSum == 0
                        && contribution.adjacency.isEmpty(),
                "contribution factory protocol");
    }

    /** 同一负边的不同中介必须落到不同HBase行，批次/平台隔离及列值保持不变。 */
    private static void verifyHbaseRows() throws IOException {
        String[] columns = AnalyticsSupport.SCHEMAS.get("mr27").get("paths").split(",");
        String line = "alpha\t1\t3\t2\t4\t5\t-6\t0.2";
        Put first = AnalyticsSupport.resultPut("mr27", "test-generation", "paths", columns, line);
        Put second =
                AnalyticsSupport.resultPut(
                        "mr27",
                        "test-generation",
                        "paths",
                        columns,
                        "alpha\t1\t3\t4\t2\t3\t-6\t0.06");
        require(
                Bytes.toString(first.getRow()).equals("test-generation#alpha#paths#1#3#2"),
                "path row identity");
        require(
                !Bytes.equals(first.getRow(), second.getRow()),
                "different mediators must not overwrite");
        String[] values = line.split("\t");
        for (int column = 0; column < columns.length; column++) {
            String stored =
                    Bytes.toString(
                            CellUtil.cloneValue(
                                    first.get(
                                                    AnalyticsSupport.FAMILY,
                                                    Bytes.toBytes(columns[column]))
                                            .get(0)));
            require(stored.equals(values[column]), "HBase column " + columns[column]);
        }
        boolean invalidRejected = false;
        try {
            AnalyticsSupport.resultPut("mr27", "test-generation", "paths", columns, "alpha\t1");
        } catch (IOException expected) {
            invalidRejected = true;
        }
        require(invalidRejected, "invalid schema must be rejected before writing");
    }
}
