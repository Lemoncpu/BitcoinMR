package org.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

/**
 * HBase API -> JSON -> ECharts。读取各算法表 current 指向的批次，避免混入历史结果。 推荐在 Linux 运行，加载 hbase-site.xml；网页与
 * API 由同一服务提供。 通过 HBase Java API 查询结构化结果。默认监听本机 8002。
 */
public class HbaseEchartsServer {

    private static final String[] METHODS = {"direct_average", "positive_katz", "signed_katz"};

    private static final ObjectMapper JSON = new ObjectMapper();

    public static void main(String[] arguments) throws Exception {
        Path projectDirectory =
                Paths.get(arguments.length > 0 ? arguments[0] : System.getProperty("user.dir"))
                        .toAbsolutePath();
        int port = Integer.parseInt(System.getenv().getOrDefault("DASHBOARD_PORT", "8002"));
        Configuration configuration = HBaseConfiguration.create();
        boolean windows = System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("win");
        String quorum = System.getenv("HBASE_ZOOKEEPER");
        if (quorum == null && windows) {
            quorum = "192.168.52.3";
        }
        if (quorum != null) {
            configuration.set("hbase.zookeeper.quorum", quorum);
        }
        if (!Files.isRegularFile(projectDirectory.resolve("mr7_echarts.html"))) {
            throw new FileNotFoundException(
                    "Working directory must contain mr7_echarts.html: " + projectDirectory);
        }
        System.out.println(
                "[启动] ZooKeeper: "
                        + configuration.get("hbase.zookeeper.quorum")
                        + ":"
                        + configuration.getInt("hbase.zookeeper.property.clientPort", 2181));
        if (windows) {
            // ZooKeeper 返回的是集群主机名，光配置 IP 仍不能替代 RegionServer 的主机名解析。
            String serverHost = System.getenv().getOrDefault("HBASE_SERVER_HOSTNAME", "niithnu");
            try {
                System.out.println(
                        "[启动] HBase 主机解析: "
                                + serverHost
                                + " -> "
                                + resolveServerHost(serverHost).getHostAddress());
            } catch (UnknownHostException failure) {
                throw new IOException(
                        "Windows cannot resolve HBase hostname '"
                                + serverHost
                                + "'. Add '192.168.52.3 "
                                + serverHost
                                + "' to C:/Windows/System32/drivers/etc/hosts, then restart this"
                                + " Java process.",
                        failure);
            }
            String firstServer = configuration.get("hbase.zookeeper.quorum").split(",")[0];
            try (Socket socket = new Socket()) {
                socket.connect(
                        new InetSocketAddress(
                                firstServer,
                                configuration.getInt("hbase.zookeeper.property.clientPort", 2181)),
                        3000);
            } catch (IOException failure) {
                throw new IOException(
                        "Cannot reach ZooKeeper at "
                                + firstServer
                                + ":2181; check VM and ZooKeeper service",
                        failure);
            }
        }
        configuration.setInt("zookeeper.session.timeout", 10000);
        configuration.setInt("zookeeper.recovery.retry", 1);
        configuration.setInt("hbase.rpc.timeout", 15000);
        configuration.setInt("hbase.client.operation.timeout", 30000);
        configuration.setInt("hbase.client.retries.number", 2);
        System.out.println("[启动] 正在连接 HBase 并检查结果表，请稍候…");
        final Connection hbase = ConnectionFactory.createConnection(configuration);
        try (Admin admin = hbase.getAdmin()) {
            for (String method : METHODS) {
                // 仅在表不存在时建表，已有历史批次不删除重建。
                if (!admin.tableExists(TableName.valueOf("mr7_" + method))) {
                    throw new IOException("Missing HBase table: mr7_" + method);
                }
            }
        } catch (Exception failure) {
            hbase.close();
            throw new IOException(
                    "HBase startup check failed; verify ZooKeeper, niithnu resolution and HBase"
                            + " tables",
                    failure);
        }
        System.out.println("[启动] HBase 连接及结果表检查成功，正在启动网页服务。");
        // 注册进程退出钩子，服务结束时关闭复用的 HBase 连接，避免留下后台资源。
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    // 进程退出时尽力关闭复用连接，关闭异常只记录堆栈。
                                    try {
                                        hbase.close();
                                    } catch (IOException failure) {
                                        // 保留底层堆栈供控制台排查，不能用空结果掩盖错误。
                                        failure.printStackTrace();
                                    }
                                }));
        if (!Files.isRegularFile(projectDirectory.resolve("mr7_echarts.html"))) {
            throw new FileNotFoundException(
                    "Set project directory to the directory containing mr7_echarts.html");
        }
        // JDK 自带轻量 HTTP 服务器。127.0.0.1 仅允许本机访问，不监听所有网卡。
        // 按指定 IP 与端口创建服务，默认只监听本机。
        HttpServer server =
                HttpServer.create(
                        new InetSocketAddress(
                                System.getenv().getOrDefault("DASHBOARD_BIND", "127.0.0.1"), port),
                        0);
        // 统一路由入口：只接受 GET，按下面的白名单返回 API、HTML 或 ECharts 库。
        server.createContext(
                "/",
                exchange -> {
                    // 接口只允许查询，不接受通过 HTTP 写入数据。
                    if (!exchange.getRequestMethod().equals("GET")) {
                        // 方法不支持返回 405，本接口只读不接收写入。
                        respond(
                                exchange,
                                405,
                                "text/plain",
                                "GET only".getBytes(StandardCharsets.UTF_8));
                        return;
                    }
                    String requestPath = exchange.getRequestURI().getPath();
                    try {
                        if (requestPath.equals("/api/dashboard")) {
                            String platform = "alpha";
                            String jobId = "mr7";
                            String query = exchange.getRequestURI().getRawQuery();
                            if (query != null) {
                                for (String parameter : query.split("&")) {
                                    String[] pair = parameter.split("=", 2);
                                    if (pair[0].equals("platform") && pair.length == 2) {
                                        platform = URLDecoder.decode(pair[1], "UTF-8");
                                    }
                                    if (pair[0].equals("job") && pair.length == 2) {
                                        jobId = URLDecoder.decode(pair[1], "UTF-8");
                                    }
                                }
                            }
                            // 只接受 Alpha 和 OTC 两个平台，其他参数返回错误。
                            if (!platform.equals("alpha") && !platform.equals("otc")) {
                                respond(
                                        exchange,
                                        400,
                                        "text/plain",
                                        "Invalid platform".getBytes(StandardCharsets.UTF_8));
                                return;
                            }
                            // 只允许已注册任务，防止查询未知数据集或任意 HBase 表。
                            if (!jobId.equals("mr7")
                                    && !AnalyticsSupport.SCHEMAS.containsKey(jobId)) {
                                respond(
                                        exchange,
                                        400,
                                        "text/plain",
                                        "Invalid job".getBytes(StandardCharsets.UTF_8));
                                return;
                            }
                            // MR22在计算、API和页面三层均仅允许OTC，拒绝访问旧Alpha结果。
                            if (jobId.equals("mr22") && !platform.equals("otc")) {
                                respond(
                                        exchange,
                                        400,
                                        "text/plain",
                                        "MR22 supports OTC only".getBytes(StandardCharsets.UTF_8));
                                return;
                            }
                            Map<String, Object> payload =
                                    jobId.equals("mr7")
                                            ? queryDashboard(hbase, platform)
                                            : AnalyticsSupport.dashboard(hbase, jobId, platform);
                            respond(
                                    exchange,
                                    200,
                                    "application/json; charset=utf-8",
                                    JSON.writeValueAsBytes(payload));
                            // 页面与离线脚本路径固定，不把任意 URL 路径拼到磁盘路径，避免读取其他项目文件。
                        } else if (requestPath.equals("/")
                                || requestPath.equals("/mr7_echarts.html")) {
                            respond(
                                    exchange,
                                    200,
                                    "text/html; charset=utf-8",
                                    Files.readAllBytes(
                                            projectDirectory.resolve("mr7_echarts.html")));
                        } else if (requestPath.equals("/vendor/echarts.min.js")) {
                            // 只提供指定静态库，不把任意 URL 映射为磁盘路径。
                            respond(
                                    exchange,
                                    200,
                                    "application/javascript; charset=utf-8",
                                    Files.readAllBytes(
                                            projectDirectory.resolve("vendor/echarts.min.js")));
                        } else {
                            respond(
                                    exchange,
                                    404,
                                    "text/plain",
                                    "Not found".getBytes(StandardCharsets.UTF_8));
                        }
                    } catch (Exception failure) {
                        // 向错误输出记录诊断信息，不把异常伪装成正常数据。
                        System.err.println("Dashboard query failed: " + failure.getMessage());
                        respond(
                                exchange,
                                503,
                                "application/json",
                                "{\"error\":\"HBase data unavailable; run HdfsToHbase first\"}"
                                        .getBytes(StandardCharsets.UTF_8));
                    }
                });
        server.setExecutor(Executors.newFixedThreadPool(4));
        // 开始监听 HTTP 端口；只有此时浏览器才能访问页面与查询接口。
        server.start();
        System.out.println(
                "HBase API + ECharts: http://"
                        + System.getenv().getOrDefault("DASHBOARD_BIND", "127.0.0.1")
                        + ":"
                        + port
                        + "/mr7_echarts.html");
    }

    // DNS 在某些 Windows 网络会长时间等待；使用守护线程并限制检查时间，避免启动无反馈。
    private static InetAddress resolveServerHost(String hostname) throws UnknownHostException {
        ExecutorService resolver =
                Executors.newSingleThreadExecutor(
                        task -> {
                            Thread thread = new Thread(task, "hbase-dns-preflight");
                            // 解析线程设为守护线程，避免它阻止已结束的客户端退出。
                            thread.setDaemon(true);
                            return thread;
                        });
        try {
            // 在线程池执行主机名解析，最多等待五秒，避免启动无限挂起。
            return resolver.submit(() -> InetAddress.getByName(hostname)).get(5, TimeUnit.SECONDS);
        } catch (Exception failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            UnknownHostException unavailable =
                    new UnknownHostException("Cannot resolve " + hostname + " within 5 seconds");
            unavailable.initCause(failure);
            throw unavailable;
        } finally {
            // 解析完成或失败都关闭临时线程池，避免客户端被后台线程拖住。
            resolver.shutdownNow();
        }
    }

    private static Map<String, Object> queryDashboard(Connection hbase, String platform)
            throws IOException {
        Map<String, Object> payload = new LinkedHashMap<>();
        Map<String, Object> results = new LinkedHashMap<>();
        // 为按方法保存的收敛历史建立容器，不同业务维度单独保存。
        Map<String, Object> iterations = new LinkedHashMap<>();
        Map<String, Object> generations = new LinkedHashMap<>();
        Map<String, Object> outputPaths = new LinkedHashMap<>();
        Set<String> expectedUsers = null;
        for (String method : METHODS) {
            try (Table table = hbase.getTable(TableName.valueOf("mr7_" + method))) {
                Result current = table.get(new Get(Bytes.toBytes("current")));
                String generation = text(current, "generation");
                generations.put(method, generation);
                outputPaths.put(method, text(current, "hdfs_output"));
                List<Map<String, Object>> scores = new ArrayList<>();
                Set<String> userIds = new HashSet<>();
                // 限定到本次 generation、平台与数据集前缀，不扫描其他历史批次。
                Scan scan =
                        new Scan()
                                .setRowPrefixFilter(
                                        Bytes.toBytes(generation + "#" + platform + "#score#"));
                // 只读取 metrics 列族，减少无关字段和网络传输。
                scan.addFamily(Bytes.toBytes("metrics"));
                scan.setCaching(1000);
                try (ResultScanner scanner = table.getScanner(scan)) {
                    for (Result stored : scanner) {
                        Map<String, Object> row = new LinkedHashMap<>();
                        String userId = text(stored, "user_id");
                        if (!userIds.add(userId)) {
                            throw new IOException("Duplicate user");
                        }
                        row.put("user_id", userId);
                        row.put("in_degree", Integer.parseInt(text(stored, "in_degree")));
                        String score = text(stored, "score");
                        String rank = text(stored, "rank");
                        row.put("score", score.equals("N/A") ? null : Double.valueOf(score));
                        row.put("rank", rank.equals("null") ? null : Integer.valueOf(rank));
                        scores.add(row);
                    }
                }
                if (scores.isEmpty()) {
                    throw new IOException("Empty HBase generation: " + method);
                }
                if (expectedUsers == null) {
                    expectedUsers = userIds;
                } else if (!expectedUsers.equals(userIds)) {
                    // 算法覆盖的用户不一致时不能直接对比，明确报错。
                    throw new IOException("Algorithm users not aligned");
                }
                scores.sort(
                        Comparator.comparingInt(
                                // 将用户 ID 作为数值比较，避免文本顺序把用户 10 排在用户 2 前。
                                row -> Integer.parseInt((String) row.get("user_id"))));
                results.put(method, scores);
                // 仅两种 Katz 执行传播或检查迭代历史，直接均分跳过此分支。
                if (!method.equals("direct_average")) {
                    List<Map<String, Object>> history = new ArrayList<>();
                    // 限定到本次 generation、平台与数据集前缀，不扫描其他历史批次。
                    Scan historyScan =
                            new Scan()
                                    .setRowPrefixFilter(
                                            Bytes.toBytes(
                                                    generation + "#" + platform + "#iteration#"));
                    // 只读取 metrics 列族，减少无关字段和网络传输。
                    historyScan.addFamily(Bytes.toBytes("metrics"));
                    try (ResultScanner scanner = table.getScanner(historyScan)) {
                        for (Result stored : scanner) {
                            Map<String, Object> row = new LinkedHashMap<>();
                            row.put("iteration", Integer.parseInt(text(stored, "iteration")));
                            row.put("l1_delta", Double.parseDouble(text(stored, "l1_delta")));
                            // 按真实迭代或读取次序保存历史行，不修改误差数值。
                            history.add(row);
                        }
                    }
                    // 行键是文本，不能依靠扫描顺序排序数字轮次（1、10、100、2）；必须按整数排序。
                    // 按整数轮次排序历史，避免字符串顺序将第十轮放在第二轮之前。
                    history.sort(Comparator.comparingInt(row -> (Integer) row.get("iteration")));
                    // Katz 必须有真实迭代历史，才能说明结果的收敛来源。
                    if (history.isEmpty()) {
                        throw new IOException("Missing iteration history");
                    }
                    iterations.put(method, history);
                }
            }
        }
        payload.put("source", "hbase-api");
        payload.put("run_id", "current");
        payload.put("generations", generations);
        payload.put("hdfs_base", outputPaths);
        payload.put("results", results);
        payload.put("iterations", iterations);
        return payload;
    }

    private static String text(Result row, String qualifier) throws IOException {
        byte[] value = row.getValue(Bytes.toBytes("metrics"), Bytes.toBytes(qualifier));
        // 必需 HBase 列缺失时不能填零，交给调用方报告错误。
        if (value == null) {
            throw new IOException("Missing HBase column: " + qualifier);
        }
        return Bytes.toString(value);
    }

    private static void respond(HttpExchange exchange, int status, String contentType, byte[] body)
            throws IOException {
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.getResponseHeaders().set("Cache-Control", "no-store");
        exchange.sendResponseHeaders(status, body.length);
        try (OutputStream response = exchange.getResponseBody()) {
            response.write(body);
        }
        exchange.close();
    }
}
