package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * 仍在使用的验收工具，不是额外的业务任务。
 * 递归读取实际 Job 的 manifest，并统计 MR7 初始状态中各分区的节点数量。
 * 用于证明同平台节点已分散到多个 Reducer；只读 HDFS，不提交 Job、不修改结果。
 */
public class PipelineAudit {

    static final List<Map<String, Object>> jobs = new ArrayList<>();

    static void walk(FileSystem fs, Path root) throws Exception {
        // 检查真实目录项，只读收集验收记录。
        for (FileStatus status : fs.listStatus(root)) {
            if (status.isDirectory()) {
                walk(fs, status.getPath());
            } else if (status.getPath().getName().equals("_job_manifest.json")) {
                // 仅按约定 manifest 文件名收集真实执行证据。
                try (FSDataInputStream stream = fs.open(status.getPath())) {
                    Map<String, Object> record =
                            AnalyticsSupport.JSON.readValue((InputStream) stream, Map.class);
                    record.put("manifest", status.getPath().toString());
                    jobs.add(record);
                }
            }
        }
    }

    public static void main(String[] arguments) throws Exception {
        Configuration configuration = new Configuration();
        // 为只读验收报告建立容器，不同业务维度单独保存。
        Map<String, Object> report = new LinkedHashMap<>();
        FileSystem fs = FileSystem.get(configuration);
        for (String root : arguments) {
            walk(fs, new Path(root));
        }
        report.put("jobs", jobs);
        Map<String, Object> partitions = new LinkedHashMap<>();
        // 在每个指定根目录下查找 MR7 初始化结果，统计真实分区分布。
        for (String root : arguments) {
            // 三个方法分别验收，不能把不同算法的输出合计成一份节点统计。
            for (String method : Arrays.asList("direct_average", "positive_katz", "signed_katz")) {
                // 定位该方法初始状态目录，仅用于读取和计数，不修改输出。
                Path directory = new Path(root + "/" + method + "-stages/initial");
                if (!fs.exists(directory)) {
                    // 本条非正边不参与正向 Katz，继续处理下一条出边。
                    continue;
                }
                Map<String, Object> files = new LinkedHashMap<>();
                for (FileStatus part : fs.globStatus(new Path(directory, "part-r-*"))) {
                    Map<String, Long> platforms = new TreeMap<>();
                    try (BufferedReader reader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            fs.open(part.getPath()), StandardCharsets.UTF_8))) {
                        // 预留本次读取的一行文本，循环中每次只替换这一行，不缓存整个大文件。
                        String line;
                        // 逐行读取到文件末尾，避免整体加载大文件。
                        while ((line = reader.readLine()) != null) {
                            platforms.merge(line.split("\t", 2)[0], 1L, Long::sum);
                        }
                    }
                    files.put(part.getPath().getName(), platforms);
                }
                partitions.put(method, files);
            }
        }
        report.put("initial_state_partitions", partitions);
        System.out.println(
                AnalyticsSupport.JSON.writerWithDefaultPrettyPrinter().writeValueAsString(report));
    }
}
