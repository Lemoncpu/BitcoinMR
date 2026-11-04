package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * 黑盒运行生产排名流水线，参考期望由 Python 独立断言。
 */
public class VerifyRankingPipeline {

    public static void main(String[] arguments) throws Exception {
        // 创建测试运行配置，运行模式由后面的 -D 参数决定。
        Configuration configuration = new Configuration();
        // 先合并 -D 运行参数，再取测试输入、输出与阶段目录三个路径。
        String[] paths = new GenericOptionsParser(configuration, arguments).getRemainingArgs();
        // 调用生产排名子流水线；本类实际提交排序和 Join 两个 Job，期望结果由独立参考校验。
        DistributedReputation.rankAndJoin(configuration, paths[0], paths[1], paths[2]);
    }
}
