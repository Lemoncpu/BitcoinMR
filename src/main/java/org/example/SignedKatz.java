package org.example;

/** MR7独立入口：带符号Katz信誉。计算及Job编排由DistributedReputation负责。 */
public class SignedKatz {
    /** 接收Hadoop通用参数及可选的input、output路径。 */
    public static void main(String[] arguments) throws Exception {
        DistributedReputation.run("signed_katz", arguments);
    }
}
