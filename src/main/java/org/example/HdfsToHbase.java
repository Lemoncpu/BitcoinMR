package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * MR7 的 HDFS→HBase 分布式导入入口，不依赖 JDBC 或本地数据库。
 * Map-only Job 按 HDFS 切片写入分数和收敛历史；所有切片成功后才更新 current。
 * 旧 generation 保留，但页面只读取 current 指向的批次，避免混入历史数据。
 */
public class HdfsToHbase {

    private static final byte[] FAMILY = Bytes.toBytes("metrics");

    /** 也可独立运行，导入三个已经完成的 HDFS 任务输出。 */
    public static void main(String[] arguments) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        String[] paths =
                new org.apache.hadoop.util.GenericOptionsParser(configuration, arguments)
                        .getRemainingArgs();
        String outputRoot = paths.length > 0 ? paths[0] : "/bitcoin/results";
        // 传入方法名时只导入该方法，否则依次导入三种信誉结果。
        for (String method :
                (paths.length > 1
                        ? Collections.singletonList(paths[1])
                        : Arrays.asList("direct_average", "positive_katz", "signed_katz"))) {
            importMethod(configuration, method, new Path(outputRoot + "/" + method));
        }
    }

    /**
     * HDFS CSV 的分片导入 Mapper。
     * 按输入文件父目录识别平台，按文件名前缀区分 scores 与 iterations；跳过合法表头。
     * 校验字段与有限数值后构造 generation+platform+业务键，BufferedMutator 批量 Put。
     * 任务重试覆盖同一批次同一行键；cleanup 刷新失败会使任务失败，不能提前发布 current。
     */
    public static class ImportMapper
            extends org.apache.hadoop.mapreduce.Mapper<
                    org.apache.hadoop.io.LongWritable,
                    org.apache.hadoop.io.Text,
                    org.apache.hadoop.io.Text,
                    org.apache.hadoop.io.NullWritable> {

        private Connection connection;

        private BufferedMutator writer;

        private String platform;
        private String method;
        private String generation;
        private boolean iterationFile;

        @Override
        protected void setup(Context context) throws IOException {
            Configuration configuration = HBaseConfiguration.create(context.getConfiguration());
            Path path =
                    ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit())
                            .getPath();
            platform = path.getParent().getName();
            iterationFile = path.getName().startsWith("iterations-");
            method = configuration.get("import.method");
            generation = configuration.get("import.generation");
            connection = ConnectionFactory.createConnection(configuration);
            writer = connection.getBufferedMutator(TableName.valueOf("mr7_" + method));
        }

        @Override
        public void map(
                org.apache.hadoop.io.LongWritable offset,
                org.apache.hadoop.io.Text line,
                Context context)
                throws IOException {
            String text = line.toString();
            if (text.equals("platform,user_id,in_degree,score,rank")
                    || text.equals("iteration,l1_delta")) {
                return;
            }
            String[] fields = text.split(",", -1);
            // 预留本行的业务行键或列定义，随后按分数/历史两种记录分别填充。
            String[] columns;
            // 预留本行的业务行键或列定义，随后按分数/历史两种记录分别填充。
            String key;
            try {
                // 历史与分数的列数及行键不同，分别解析和校验。
                if (iterationFile) {
                    if (fields.length != 2
                            || Integer.parseInt(fields[0]) < 1
                            || !Double.isFinite(Double.parseDouble(fields[1]))
                            || Double.parseDouble(fields[1]) < 0) {
                        throw new IllegalArgumentException();
                    }
                    columns = new String[] {"iteration", "l1_delta"};
                    key = generation + "#" + platform + "#iteration#" + fields[0];
                } else {
                    if (fields.length != 5
                            || !fields[0].equals(platform)
                            || Integer.parseInt(fields[1]) < 0
                            || Long.parseLong(fields[2]) < 0) {
                        throw new IllegalArgumentException();
                    }
                    if (!fields[3].equals("N/A")
                            && !Double.isFinite(Double.parseDouble(fields[3]))) {
                        throw new IllegalArgumentException();
                    }
                    // 未入榜可以是 null，实际名次必须为正整数，不能默默接受零或负数。
                    if (!fields[4].equals("null") && Long.parseLong(fields[4]) < 1) {
                        throw new IllegalArgumentException();
                    }
                    columns = new String[] {"platform", "user_id", "in_degree", "score", "rank"};
                    key = generation + "#" + platform + "#score#" + fields[1];
                }
            } catch (RuntimeException invalid) {
                throw new IOException("Invalid HDFS import row: " + text, invalid);
            }
            Put put = new Put(Bytes.toBytes(key));
            for (int column = 0; column < columns.length; column++) {
                // 按当前记录类型的列序写入对应列名，历史与分数使用不同 Schema。
                add(put, columns[column], fields[column]);
            }
            // 补齐平台列，迭代历史原始 CSV 本身不包含平台字段。
            add(put, "platform", platform);
            add(put, "method", method);
            writer.mutate(put);
            // 按当前类型分别统计分数行与历史行；只有该 Map 最终成功，计数才进入全局统计。
            context.getCounter("HBASE_IMPORT", iterationFile ? "ITERATIONS" : "SCORES")
                    .increment(1);
        }

        @Override
        protected void cleanup(Context context) throws IOException {
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
     * 导入 MR7 的一个方法，业务输出和 HBase 导入作业分别留痕。
     *
     * 校验方法白名单、HDFS 成功标记，以及 Katz 最后一轮 L1<0.01。
     * 随后创建或复用结果表，分发 HBase 客户端依赖，提交 Map-only 导入 Job。
     * 只有所有 Map 成功且分数记录非空，才更新 current；失败的 generation 不会成为页面数据源。
     *
     * @param jobConfiguration 提交任务时使用的集群与 HBase 配置
     * @param method 三种 MR7 方法之一，用于选择 mr7_ 前缀的结果表
     * @param outputDirectory 已完成的 HDFS 方法结果目录，包含各平台 CSV
     */
    public static void importMethod(
            Configuration jobConfiguration, String method, Path outputDirectory) throws Exception {
        // 只接受三种已知信誉方法，避免把任意字符串拼成表名。
        if (!Arrays.asList("direct_average", "positive_katz", "signed_katz").contains(method)) {
            throw new IllegalArgumentException("Unknown method");
        }
        Configuration configuration = HBaseConfiguration.create(jobConfiguration);
        String generation = UUID.randomUUID().toString();
        configuration.set("import.method", method);
        configuration.set("import.generation", generation);
        FileSystem fs = outputDirectory.getFileSystem(configuration);
        if (!fs.exists(new Path(outputDirectory, "_SUCCESS"))) {
            throw new IOException("Missing _SUCCESS");
        }
        TableName tableName = TableName.valueOf("mr7_" + method);
        try (Connection connection = ConnectionFactory.createConnection(configuration)) {
            try (Admin admin = connection.getAdmin()) {
                // 仅在表不存在时建表，已有历史批次不删除重建。
                if (!admin.tableExists(tableName)) {
                    admin.createTable(
                            TableDescriptorBuilder.newBuilder(tableName)
                                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(FAMILY))
                                    .build());
                }
            }
            org.apache.hadoop.mapreduce.Job job =
                    AnalyticsSupport.job(
                            configuration,
                            "MR7 " + method + " distributed HBase sink",
                            HdfsToHbase.class,
                            ImportMapper.class,
                            null,
                            org.apache.hadoop.io.Text.class,
                            org.apache.hadoop.io.NullWritable.class,
                            outputDirectory + "/alpha/scores-r-*",
                            outputDirectory + "-hbase");
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(
                    job, new Path(outputDirectory, "otc/scores-r-*"));
            // 仅两种 Katz 执行传播或检查迭代历史，直接均分跳过此分支。
            if (!method.equals("direct_average")) {
                // 逐个平台独立处理，避免两个网络的计数和名次混合。
                for (String platform : Arrays.asList("alpha", "otc")) {
                    // 只读取该平台少量收敛历史用于导入前检查；用户分数仍由 Map 分片导入。
                    List<String[]> history =
                            readCsv(
                                    fs,
                                    new Path(outputDirectory, platform + "/iterations-r-*"),
                                    "iteration,l1_delta");
                    // 历史必须存在且末轮 L1 小于 0.01，才能导入 Katz 结果。
                    if (history.isEmpty()
                            || Double.parseDouble(history.get(history.size() - 1)[1])
                                    >= DistributedReputation.KATZ_CONVERGENCE_THRESHOLD) {
                        // 超过轮数仍未收敛，停止计算且不发布新批次。
                        throw new IOException("Not converged: " + platform);
                    }
                    org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(
                            job, new Path(outputDirectory, platform + "/iterations-r-*"));
                }
            }
            // 通过分布式缓存分发客户端依赖，容器不能只依赖提交端环境变量。
            AnalyticsSupport.configureHbaseClasspath(job);
            AnalyticsSupport.run(job);
            long scores = job.getCounters().findCounter("HBASE_IMPORT", "SCORES").getValue();
            // 用户分数为空时不能替换当前可用批次。
            if (scores == 0) {
                throw new IOException("Empty import");
            }
            long rows =
                    scores + job.getCounters().findCounter("HBASE_IMPORT", "ITERATIONS").getValue();
            // 失败重试只覆盖同 generation 的业务主键；所有切片确认成功才发布 current。
            try (Table table = connection.getTable(tableName)) {
                Put current = new Put(Bytes.toBytes("current"));
                add(current, "generation", generation);
                add(current, "hdfs_output", outputDirectory.toString());
                add(current, "row_count", Long.toString(rows));
                table.put(current);
            }
            System.out.println(
                    "HBase table=" + tableName + ", rows=" + rows + ", generation=" + generation);
        }
    }

    private static void add(Put put, String column, String value) {
        // 所有值均为 UTF-8 文本；N/A/null 保留原始缺失标记，查询端需识别，不能当成数字零。
        put.addColumn(FAMILY, Bytes.toBytes(column), Bytes.toBytes(value));
    }

    private static List<String[]> readCsv(FileSystem hdfs, Path pattern, String header)
            throws IOException {
        FileStatus[] files = hdfs.globStatus(pattern);
        // 必须有实际分区文件，路径缺失不能当作合法空结果。
        if (files == null || files.length == 0) {
            throw new FileNotFoundException(pattern.toString());
        }
        // 按文件名稳定读取历史分区；这里只处理历史小文件，不加载分数全集。
        Arrays.sort(files, Comparator.comparing(file -> file.getPath().toString()));
        List<String[]> rows = new ArrayList<>();
        for (FileStatus file : files) {
            try (BufferedReader reader =
                    new BufferedReader(
                            new InputStreamReader(
                                    hdfs.open(file.getPath()), StandardCharsets.UTF_8))) {
                // 检查 CSV 表头与约定列序一致，避免把评分列和名次列读反。
                if (!header.equals(reader.readLine())) {
                    throw new IOException("Invalid header: " + file.getPath());
                }
                // 预留本次读取的一行文本，循环中每次只替换这一行，不缓存整个大文件。
                String line;
                // 逐行读取到文件末尾，避免整体加载大文件。
                while ((line = reader.readLine()) != null) {
                    // 跳过空白行，避免把空行当成缺列记录。
                    if (!line.trim().isEmpty()) {
                        rows.add(line.split(",", -1));
                    }
                }
            }
        }
        return rows;
    }
}
