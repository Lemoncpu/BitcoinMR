# MR27：正向信任链与直接负评冲突

查看A正评B、B正评C，而A直接负评C的关系及中介证据。A、B、C互不相同，两段评分均>0，直接评分<0；输入为非自环最新有向快照。

| 物理阶段 | 代码 | 分组、排序与处理 |
| --- | --- | --- |
| SOURCE锚定连接 | SourceMapper / SourceReducer | JoinKey按平台、SOURCE归组，负边kind=0先于正边kind=1，只保留当前SOURCE负向邻接，生成待验证路径 |
| 正向末边半连接 | PositiveEdgeMapper / WedgeMapper / PathReducer | 按平台、B、C归组，真实B→C正边先到；仅输出成功匹配的(A,B,C) |
| 直接负边汇总 | NegativeEdgeMapper / ConfirmedPathMapper / EdgeReducer | 按平台、A、C归组，负边先到，B升序，校验不同中介并汇总评分乘积 |

IdentityPartitioner与IdentityGrouping使用相同连接身份，JoinKey额外排序kind及neighbor。TaggedRating在值中保留来源、用户及评分，不依赖框架复用的键。三个计算阶段均默认4个Reducer。

paths保存全部唯一三元组；edges保留所有直接负边，包括零中介记录。mediator_count为不同B数，support_score=sum(RATING_AB×RATING_BC)/100，max_path_weight为最大单路径分值。

当前结果：Alpha 1,536条负边，其中665条有支持，共2,921条路径；OTC 3,563条负边，其中908条有支持，共3,752条路径。至少3名中介时分别仍有311和375条负边。该结果在实现前经本地Python验证，Hadoop结果与独立参考逐字段一致。

HDFS固定目录 /bitcoin/results/mr27，HBase表analytics_mr27。完整路径行键包含SOURCE、TARGET和intermediary，避免中介相互覆盖。页面显示前十条负边及第一条关系的局部路径图，最多展示20名中介，全部路径保存在HBase。

```bash
cd /tools/MR
bash run_relation_jobs.sh
python tests/verify_relation_jobs.py --url http://192.168.52.3:8002 --report target/relation-verification.json
```

独立Python参考枚举所有正向二跳后检查负边，与Java的负边锚定连接采用不同计算路径。tests/relation-fixtures和relation-zero-fixtures覆盖多中介、方向、重复评价、小数时间、平台隔离与零路径。
