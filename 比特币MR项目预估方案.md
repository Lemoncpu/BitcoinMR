# 基于 Hadoop MapReduce 的比特币信任网络分析与关系推荐系统

> 项目方案 v1.1 · MR7 三种信誉聚合对比版 · 2026-10-23  
> 状态：方案设计阶段；本文中的算法参数、数据库结构为建议配置，尚未经过实际运行验证。

## 1. 项目目标

基于 Stanford SNAP 的 Bitcoin OTC 和 Bitcoin Alpha 信任关系数据，使用 Hadoop HDFS 管理原始数据，以 Java Hadoop MapReduce 完成 **4 个基础统计任务 + 3 个网络关系分析任务**，并将结构化结果分别导入 HBase 与 MySQL，最终使用 ECharts 展示统计图表与局部信任关系图。

项目主线为 **“信任关系统计 → 二跳推荐 → 双向关系分析 → 三种信誉聚合对比”**。重点体现多阶段 MapReduce 图计算、数据建模和结果落库，而非单纯词频或求和统计。

## 2. 数据集

| 数据集 | 节点数 | 评价边记录数 | 官方地址 |
|---|---:|---:|---|
| Bitcoin OTC | 5,881 | 35,592 | https://snap.stanford.edu/data/soc-sign-bitcoin-otc.html |
| Bitcoin Alpha | 3,783 | 24,186 | https://snap.stanford.edu/data/soc-sign-bitcoin-alpha.html |
| 合计 | 不跨平台合并节点 | 59,778 | — |

下载：
- OTC：https://snap.stanford.edu/data/soc-sign-bitcoinotc.csv.gz
- Alpha：https://snap.stanford.edu/data/soc-sign-bitcoinalpha.csv.gz

原始记录格式：`SOURCE,TARGET,RATING,TIME`。

- `SOURCE`：发起评价的用户 ID；
- `TARGET`：接受评价的用户 ID；
- `RATING`：范围为 -10 至 +10 的有符号评价；
- `TIME`：Unix 时间戳。

该网络是**有向、带权、有符号**的信任评价网络，`A → B` 不等于 `B → A`。两个平台用户 ID 不保证对齐，所有主键、RowKey 和结果都必须包含 `platform` 标记。

### 2.1 数据口径约定

1. **事件数据**：保留原始评价记录，供评分分布和月度趋势分析使用。
2. **网络快照**：若同一方向用户对存在多次评价，按时间取最新评价构建当前关系图（相同时间戳的决胜规则需在编码前确认）。
3. 仅将 `RATING > 0` 视为正向信任边；`RATING < 0` 视为负向评价；`RATING = 0` 单列为中性，不参与正向推荐。
4. 不将缺失边等同于负面边；不能把预测推荐评分解释成现实可信度或概率。
5. OTC、Alpha 分开运行图任务，最终只在统计层面做平台比较。

## 3. 系统架构与技术栈

```text
SNAP OTC / Alpha CSV.GZ
         |
         v
数据预处理（解压、字段校验、事件 / 最新关系快照）
         |
         v
HDFS：raw / clean / snapshot
         |
         v
Hadoop MapReduce（MR1 ~ MR7）
         |
         v
HDFS：各任务结构化结果
       /   \
      v     v
   HBase   MySQL
  节点/边明细  聚合指标与图表查询
       \   /
        v v
    ECharts 可视化
```

| 层次 | 建议技术 |
|---|---|
| 编程语言 | Java |
| 文件存储 | Hadoop HDFS |
| 分布式计算 | Hadoop MapReduce |
| NoSQL 存储 | HBase |
| 关系型存储 | MySQL |
| 结果导入 | HBase Java API、JDBC 批量入库 |
| 可视化 | Apache ECharts，必要时加轻量 Spring Boot JSON 接口 |

**写入策略**：MapReduce 优先输出 HDFS 中间结果，再通过可重试的批量导入过程分别落到 HBase 和 MySQL。避免 Reducer 直接双写数据库导致失败重试和数据不一致。

## 4. 七个 MapReduce 业务任务

| 编号 | 任务名称 | 分类 | 主要计算 | 主要图表 |
|---|---|---|---|---|
| MR1 | 信任评分分布统计 | 基础、低 | 各评分出现次数、正负中性比例 | 柱状图 / 环形图 |
| MR2 | 用户入度与出度统计 | 基础、低 | 评价发出数、收到评价数 | 双向条形图 / 散点图 |
| MR3 | 用户信誉度统计 | 基础、低 | 收到评价均分、负面评价比例、信誉排行 | Top-K 条形图 / 分布图 |
| MR4 | 信任网络时间趋势 | 基础、低 | 月度评价数、月均评分、负评比例 | 折线图 |
| MR5 | 二跳信任关系推荐 | 网络、中高 | 正向二跳路径连接、加权候选评分、Top-K | 推荐排行 / 局部关系图 |
| MR6 | 双向信任及冲突分析 | 网络、中高 | 双向正面、双向负面、正负冲突、单向关系 | 分组柱状图 / 环形图 |
| MR7 | 三种用户信誉聚合方法对比 | 网络、高 | Direct Average、Positive Katz、Signed Katz 三种信誉得分及排序比较 | 三方法排名对比 / 排名变化 / 迭代收敛 |

> 注意：上述“7 个任务”是 7 种业务分析，不等于仅运行 7 个 Hadoop Job。MR5 和 MR7 将拆成多个 Job。

### MR1：信任评分分布统计

- **输入**：原始事件记录。
- **Mapper**：输出 `(platform, rating) → 1`；同时可以输出按 `positive/negative/neutral` 分类的计数。
- **Reducer**：按评分求和。
- **输出**：`platform, rating, rating_count`。
- **MySQL**：`rating_distribution`。
- **图表**：-10 到 +10 的评分柱状图、三类评分比例环形图。

### MR2：用户入度与出度统计

- **输入**：最新关系快照。
- **Mapper**：对每条 `A → B` 输出 A 的 `out=1` 和 B 的 `in=1`。
- **Reducer**：按 `(platform,user_id)` 累加入度和出度；也可分别统计正负入/出度。
- **输出**：`platform, user_id, in_degree, out_degree, positive_in, negative_in` 等。
- **MySQL**：`user_degree`。
- **图表**：用户度数 Top 20 条形图、入度与出度散点图。

### MR3：用户信誉度统计

- **输入**：最新关系快照。
- **Mapper**：按被评价用户聚合 `rating、1、is_negative`。
- **Reducer**：统计收到评价次数、评分总和、均分、负评数和负评比例。
- **输出**：`platform,user_id,rating_count,avg_rating,negative_count,negative_ratio`。
- **MySQL**：`user_reputation`。
- **图表**：信誉 Top 20、信誉分分布；排行时建议设置最低评价次数门槛（初始建议为 5，待确认）。

### MR4：信任网络时间趋势

- **输入**：原始事件记录。
- **Mapper**：将时间戳转换为明确时区下的 `YYYY-MM`，输出月份、评分及负评标记。
- **Reducer**：计算评论数、月平均分、负评率。
- **输出**：`platform, month, event_count, avg_rating, negative_ratio`。
- **MySQL**：`monthly_trend`。
- **图表**：月度评价数量和负面评价比例折线图。

### MR5：二跳信任关系推荐（核心）

给定图中两条**正向**边 `A → B` 与 `B → C`，构成候选推荐 `A → C`，其中 B 是中介用户。

可定义路径支持分数：

\[
S(A,C)=\sum_{B:\ w_{AB}>0,\ w_{BC}>0}\frac{w_{AB}\,w_{BC}}{100}.
\]

- 该值是**路径支持分数**，可能大于 1，不是概率；
- 排除 `A = C`；
- 排除 A 已经评价过的用户，包括已有负面评价的用户；
- 同一 `(A,C)` 的多个中介贡献合并；
- 推荐 Top-K（初始建议 K=10，待确认）。

**多 Job 实现建议**：

1. **Job A（二跳连接）**：以中间节点 B 为 Key，汇集指向 B 的正向入边及从 B 发出的正向出边，Reducer 对入、出邻居进行连接，生成 `(A,C,B,路径权重)`；
2. **Job B（候选聚合）**：按 `(A,C)` 求路径总分和不同中介数量；
3. **Job C（过滤与排序）**：与原始直接关系表进行连接或使用可靠的辅助索引，剔除已有边及自环，按 A 筛选 Top-K。

结果字段：`platform,user_id,candidate_id,score,path_count,rank_no`，可选保存中介用户 ID 列表（用于局部关系图）。

- **MySQL**：`trust_recommendations`；
- **图表**：某个用户 Top-K 推荐条形图 + A→B→C 的局部关系图。

**扩展（非必做）**：反向推荐可通过逆向路径分析生成“可能信任你的用户”，但第一版仅需完成正向推荐，保证可实现性。

### MR6：双向信任及冲突分析

- **输入**：最新关系快照。
- **Mapper**：以无序节点对 `(min(A,B),max(A,B))` 为 Key，保留有向评分信息。
- **Reducer**：判断是否存在 A→B、B→A，并区分：单向、双向正面、双向负面、正负冲突、其他包含零评分情形。
- **输出**：用户对方向明细及按类别聚合的网络结构统计。
- **MySQL**：`trust_pair_statistics`，可选 `trust_pair_details`。
- **图表**：不同关系类别的数量柱状图/占比环形图。
- **实现提醒**：若需要全网各类别数量，需追加一次按类别求和的 MR Job 或将明细导入后用 MySQL 聚合。不得误将单向正/负关系混为同一个意义上的“互信”。

### MR7：三种用户信誉聚合方法对比（高难度）

**目的**：在同一份有向、有符号信任网络快照上，对比**局部直接评价**、**仅正向信任传播**和**正负评价共同传播**得到的用户信誉分及排名。MR7 不再单独计算 PageRank。

**方法一：Direct Average（直接平均信誉）**

直接复用 MR3 中用户收到的最新评价均分，作为不考虑网络传播的基线：

$$
S_{\mathrm{direct}}(v)=\frac{\sum_{u\to v}w_{uv}}{|\{u:u\to v\}|}.
$$

无入边用户须按统一规则标为 `N/A`（不将“无人评价”误判为零信誉）；排名时沿用 MR3 的最低被评价次数门槛建议。

**方法二：Positive Katz（正向 Katz 传播）**

只保留正向边，并使用正向出边权重和作为归一化分母；负边不参与 Positive Katz 的传播或归一化：

$$
B_{uv}^{+}=\frac{\max(w_{uv},0)}{\sum_k\max(w_{uk},0)}.
$$

$$
S_{t+1}^{+}(v)=1+0.8\sum_{u\to v}B_{uv}^{+}S_t^{+}(u).
$$

**方法三：Signed Katz（带符号 Katz 传播）**

保留正负评价的符号与强度：

$$
B_{uv}=\frac{w_{uv}}{\sum_k|w_{uk}|}.
$$

$$
\boxed{S_{t+1}^{\pm}(v)=1+0.8\sum_{u\to v}B_{uv}S_t^{\pm}(u)}
$$

其中 $w_{uv}\in[-10,10]$ 表示 $u$ 对 $v$ 的最新评分；$1$ 为统一基础得分；$0.8$ 为两种 Katz 共同使用的传播衰减系数。Positive Katz 的分母为正向出边权重和，Signed Katz 的分母为全部出边权重的绝对值之和；任一方法的分母为零时，该方法对应传播权重设为零（无需 PageRank 式悬挂质量分摊）。两种 Katz 均从 $S_0(v)=1$ 初始化，采用相同最大迭代轮数和收敛阈值，以保证公平比较。

**结果解释**：Direct Average 反映用户直接收到的评价；Positive Katz 体现正向信任在网络中的传播；Signed Katz 同时允许负面评价传播。Signed Katz 的多跳有符号路径可能出现“负负得正”，但它只是该数学模型的传播效应，不等于现实中的负面评价必然提升信誉。

**多 Job 实现建议**：

1. **数据准备 Job**：读取最新关系快照，按源用户 $u$ 分别汇总 $D_u^+=\sum_k\max(w_{uk},0)$ 和 $D_u^{\pm}=\sum_k|w_{uk}|$，分别生成归一化正向边 $B^+$ 和有符号边 $B$；保留所有出现过的节点。
2. **直接均分**：从 MR3 的 `user_reputation.avg_rating` 读取，不重复进行分布式迭代。
3. **迭代 Job**：对 Positive Katz 与 Signed Katz 分别传递 $0.8\,B_{uv}^{(\cdot)}S_t(u)$，按目的用户 $v$ 累加并加基础分 $1$；无入边节点保持基础得分。
4. **收敛检查**：统计 $\sum_v|S_{t+1}(v)-S_t(v)|$，达到预设阈值或最大轮数后停止；保存两种 Katz 的轮次与误差。
5. **对齐与比较 Job**：按 `(platform,user_id)` 连接三组结果，计算各自排名、Top-20 重合率、排名变化和 Spearman 等级相关；可单独列举正负关系影响较大的用户。

**比较口径**：Direct Average 原始分数在 $[-10,10]$，Katz 得分尺度不同，**不得直接比较原始数值高低**。图表优先比较各自排名、分位数或明示的归一化得分。OTC 与 Alpha 分开计算。对评价次数不足的节点，分别标注或过滤，确保用户排行有意义。

- **输出**：`platform,user_id,direct_avg,positive_katz,signed_katz,direct_rank,positive_rank,signed_rank,positive_vs_direct_rank_delta,signed_vs_positive_rank_delta`；另输出两种 Katz 的迭代轮次和 L1 变化量。
- **MySQL**：`user_reputation_comparison`、`katz_iteration_stats`。
- **HBase**：`user_metrics` 增加 `reputation:direct_avg/positive_katz/signed_katz` 与相应排名字段。
- **图表**：三种方法的 Top-20 排名对比、同用户的排名变化、Positive Katz vs Signed Katz 散点图、两种 Katz 的迭代收敛曲线。

> **实现前需确认**：最终最大迭代轮数、L1 收敛阈值、无入边节点在 Direct Average 中的展示方式；上述公式与衰减系数为既定方案，迭代终止细节仍待实测确定。

## 5. 结构化存储设计

### 5.1 HBase 表

| 表 | RowKey | 建议列族及字段 |
|---|---|---|
| `trust_graph` | `platform#source_id` | `out:target_id`（实际需设计 qualifier 编码）、`meta:update_time`；或采用每条边独立 RowKey |
| `user_metrics` | `platform#user_id` | `degree:in/out`、`reputation:avg/negative_ratio`、`reputation:direct_avg/positive_katz/signed_katz`、`rank:direct/positive/signed` |
| `time_metrics` | `platform#YYYY-MM` | `stats:count/avg_rating/negative_ratio` |
| `recommendations` | `platform#user_id#candidate_id` | `rec:score/path_count/rank_no` |
| `pair_relations` | `platform#min_id#max_id` | `pair:forward_rating/reverse_rating/type` |
| `rating_stats` | `platform#rating` | `stats:count` |

HBase 具体列族、RowKey 长度、是否按单边存储须在实现前结合查询方式最终确认。

### 5.2 MySQL 结果表

| 表名 | 关键字段 | 对应任务 |
|---|---|---|
| `rating_distribution` | `platform,rating,rating_count` | MR1 |
| `user_degree` | `platform,user_id,in_degree,out_degree,...` | MR2 |
| `user_reputation` | `platform,user_id,rating_count,avg_rating,negative_ratio` | MR3 |
| `monthly_trend` | `platform,month,event_count,avg_rating,negative_ratio` | MR4 |
| `trust_recommendations` | `platform,user_id,candidate_id,score,path_count,rank_no` | MR5 |
| `trust_pair_statistics` | `platform,relation_type,pair_count` | MR6 |
| `user_reputation_comparison` | `platform,user_id,direct_avg,positive_katz,signed_katz,direct_rank,positive_rank,signed_rank,...` | MR7 |
| `katz_iteration_stats` | `platform,method,iteration,l1_delta` | MR7 |

示例 SQL（推荐任务）：

```sql
CREATE TABLE trust_recommendations (
    platform VARCHAR(16) NOT NULL,
    user_id INT NOT NULL,
    candidate_id INT NOT NULL,
    score DOUBLE NOT NULL,
    path_count INT NOT NULL,
    rank_no INT NOT NULL,
    PRIMARY KEY (platform, user_id, candidate_id),
    INDEX idx_rec_lookup (platform, user_id, rank_no)
);
```

所有结果表建议附加 `run_id` 或 `snapshot_time`（如需保存多次批处理版本，则应将其纳入主键设计）。

## 6. 可视化设计

仅要求展示统计图表，不必开发复杂后台管理系统。建议 ECharts + 简单 JSON 接口：

| 模块 | 图表 | 数据源 |
|---|---|---|
| 评分分析 | 评分柱状图、正负中性占比环形图 | `rating_distribution` |
| 用户连接度 | 入度/出度 Top 20、双变量散点图 | `user_degree` |
| 用户信誉 | 信誉排行榜、负面比例分布 | `user_reputation` |
| 时间分析 | 月度评价数、负评比例折线图 | `monthly_trend` |
| 推荐关系 | 用户 Top-K 推荐条形图、局部路径网络图 | `trust_recommendations` + 中介边信息 |
| 双向结构 | 关系类别分组柱状图/环形图 | `trust_pair_statistics` |
| 信誉聚合对比 | Direct Average / Positive Katz / Signed Katz 排名变化、相关性散点图、Katz 收敛曲线 | `user_reputation_comparison` + `katz_iteration_stats` |

建议增加 `platform` 下拉选项（OTC/Alpha）和推荐查询的 `user_id` 输入框。图表必须来自真实计算结果；示意图或模拟数据应明确标记。

## 7. 开发顺序与阶段交付

### 阶段一：基础链路
- [ ] 下载并校验两个 SNAP 数据集
- [ ] 搭建 HDFS、YARN/MapReduce、HBase、MySQL 环境
- [ ] CSV 清洗、生成事件数据和最新关系快照
- [ ] 实现 MR1–MR4，输出到 HDFS
- [ ] 导入 HBase/MySQL，核对结果一致性
- [ ] 完成四类基础图表

### 阶段二：网络关系挖掘
- [ ] 实现 MR5 的二跳连接、候选聚合、直接边过滤及 Top-K
- [ ] 实现 MR6 双向用户对分析及按类别聚合
- [ ] 完成推荐网络图和双向结构图表

### 阶段三：迭代分析
- [ ] 实现 MR7 的 Positive Katz、Signed Katz 多轮 Job 调度与收敛检查，复用 MR3 的 Direct Average
- [ ] 对齐三种信誉方法的得分与排名，展示排名差异、相关性及两种 Katz 收敛曲线
- [ ] 在 OTC 与 Alpha 上重复运行并作对比

### 阶段四：测试与总结
- [ ] 用小样例图手算并比对 MR5 / MR6 / MR7 结果
- [ ] 检查 HBase 与 MySQL 同一 `run_id` 结果的一致性
- [ ] 记录执行时间、数据量、Mapper/Reducer 数量、失败重试情况
- [ ] 若条件允许，做不同 Reducer 数量的性能对照
- [ ] 编写项目报告、整理演示截图和运行说明

## 8. 项目验收清单

1. 同一套代码能够分别读取并处理 OTC、Alpha 数据；
2. 7 个业务任务有可重复执行的脚本和结构化输出；
3. MR5 至少实现正向二跳信任推荐，剔除已有直接关系；
4. MR6 正确处理有向边及正负冲突；
5. MR7 同时输出 Direct Average、Positive Katz、Signed Katz，对齐三种方法的用户排名，并给出两种 Katz 的迭代终止条件；
6. 每个任务的结果均保存至 HBase 和 MySQL；
7. 可视化页面使用真实 SQL 查询结果，至少覆盖 7 个任务；
8. 提供数据口径、算法公式、HDFS 路径、表结构、测试结果和运行截图；
9. 明确说明此数据量适合图算法流程教学验证，不声称已经证明大规模分布式加速效果。

## 9. 待确认的实施参数

- Hadoop/HBase/MySQL 版本，以及单机伪分布式或多节点集群环境；
- 数据清洗时对于相同用户对、相同时间戳的最终取值规则；
- 评分为 0 的数据处理口径；
- 信誉排行榜最低被评价次数（建议起始值 5）；
- 推荐 Top-K（建议起始值 10），以及是否展示反向推荐扩展；
- Positive/Signed Katz 最大轮数、L1 收敛阈值、是否保留逐轮历史、Direct Average 无入边展示规则；
- HBase 图边的最终 RowKey 结构、MySQL 是否保存多批次历史；
- 可视化仅静态导出 JSON，还是添加轻量 HTTP API。

## 10. 数据来源

- SNAP 官方数据首页：https://snap.stanford.edu/data/
- Bitcoin OTC：https://snap.stanford.edu/data/soc-sign-bitcoin-otc.html
- Bitcoin Alpha：https://snap.stanford.edu/data/soc-sign-bitcoin-alpha.html

---

**项目当前方案总结**：使用 Bitcoin OTC 与 Bitcoin Alpha 两个相互独立的有向有符号信任网络，完成 4 个基础 MapReduce 统计任务、2 个中高难度关系挖掘任务和 1 个高难度三方法信誉聚合对比任务（Direct Average、Positive Katz、Signed Katz）；每项结果结构化写入 HBase 与 MySQL，并通过 ECharts 图表可视化。
