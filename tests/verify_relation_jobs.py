"""MR27独立原始CSV参考；核心验证不调用Java，也不复用选型脚本。"""

import argparse, csv, json, math, collections, urllib.request
from pathlib import Path
from decimal import Decimal

SCHEMAS = {
    "mr27": {
        "edges": "platform,source,target,direct_rating,mediator_count,support_score,max_path_weight,has_conflict",
        "paths": "platform,source,target,intermediary,source_rating,intermediary_rating,direct_rating,path_weight",
    },
}


def reference(directory):
    latest = {}
    for path in sorted(Path(directory).glob("*.csv")):
        platform = "alpha" if "alpha" in path.name else "otc"
        for ordinal, row in enumerate(csv.reader(path.open(encoding="utf-8-sig"))):
            try:
                if len(row) != 4:
                    continue
                source, target, rating = map(int, row[:3])
                timestamp = Decimal(row[3])
                if (
                    not timestamp.is_finite()
                    or min(source, target, timestamp) < 0
                    or abs(rating) > 10
                ):
                    continue
            except (ValueError, ArithmeticError):
                continue
            if source == target:
                continue
            key = (platform, source, target)
            version = (timestamp, path.name, ordinal)
            if key not in latest or version > latest[key][0]:
                latest[key] = (version, rating)
    edges = {key: row[1] for key, row in latest.items()}
    # 与Java负边锚定Join不同：在参考中枚举所有正向二跳，再检查负向直接边。
    outgoing = collections.defaultdict(list)
    for (platform, source, target), rating in edges.items():
        if rating > 0:
            outgoing[platform, source].append((target, rating))
    paths = []
    evidence = collections.defaultdict(list)
    for (platform, source, middle), first_rating in edges.items():
        if first_rating <= 0:
            continue
        for target, second_rating in outgoing[platform, middle]:
            if target == source or edges.get((platform, source, target), 0) >= 0:
                continue
            direct = edges[platform, source, target]
            numerator = first_rating * second_rating
            paths.append(
                dict(
                    platform=platform,
                    source=str(source),
                    target=str(target),
                    intermediary=str(middle),
                    source_rating=str(first_rating),
                    intermediary_rating=str(second_rating),
                    direct_rating=str(direct),
                    path_weight=str(numerator / 100),
                )
            )
            evidence[platform, source, target].append(numerator)
    summaries = []
    for (platform, source, target), rating in edges.items():
        if rating >= 0:
            continue
        values = evidence[platform, source, target]
        summaries.append(
            dict(
                platform=platform,
                source=str(source),
                target=str(target),
                direct_rating=str(rating),
                mediator_count=str(len(values)),
                support_score=str(sum(values) / 100),
                max_path_weight=str(max(values, default=0) / 100),
                has_conflict=str(bool(values)).lower(),
            )
        )
    return {
        "mr27": {"edges": summaries, "paths": paths},
    }


def identity(job, dataset, row):
    fields = {
        "edges": ["source", "target"],
        "paths": ["source", "target", "intermediary"],
    }[dataset]
    return tuple(row[field] for field in ["platform"] + fields)


def compare(job, dataset, expected, actual):
    left = {identity(job, dataset, r): r for r in expected}
    right = {identity(job, dataset, r): r for r in actual}
    assert len(right) == len(actual), "duplicate business keys"
    assert left.keys() == right.keys(), (
        job,
        dataset,
        "keys",
        len(left),
        len(right),
        list(left.keys() - right.keys())[:2],
        list(right.keys() - left.keys())[:2],
    )
    for key, row in left.items():
        assert row.keys() == right[key].keys(), (job, dataset, "columns")
        for column, value in row.items():
            actual_value = right[key][column]
            if (
                column
                in [
                    "support_score",
                    "max_path_weight",
                    "path_weight",
                ]
                and value != "N/A"
            ):
                assert math.isclose(
                    float(value), float(actual_value), abs_tol=1e-10, rel_tol=1e-10
                ), (job, dataset, key, column, value, actual_value)
            else:
                assert str(value) == str(actual_value), (
                    job,
                    dataset,
                    key,
                    column,
                    value,
                    actual_value,
                )


def hand_calculated_reference_check():
    """固定手算答案，防止Java和参考代码共享同一种口径错误。"""
    fixture = reference(Path(__file__).parent / "relation-fixtures")
    paths = [row for row in fixture["mr27"]["paths"] if row["platform"] == "alpha"]
    assert {(row["source"], row["target"], row["intermediary"]) for row in paths} == {
        ("1", "3", "2"),
        ("1", "3", "4"),
    }
    assert math.isclose(sum(float(row["path_weight"]) for row in paths), 0.26)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=".")
    parser.add_argument("--results")
    parser.add_argument("--url")
    parser.add_argument("--jobs", default="mr27")
    parser.add_argument("--report", required=True)
    args = parser.parse_args()
    hand_calculated_reference_check()
    expected = reference(args.input)
    expected = {job: expected[job] for job in args.jobs.split(",")}
    report = {"passed": True, "datasets": {}}
    for job, datasets in expected.items():
        if args.results:
            for dataset, rows in datasets.items():
                directory = Path(args.results) / job / dataset
                assert (directory / "_SUCCESS").exists(), directory
                actual = []
                for part in directory.glob("part-*"):
                    for values in csv.reader(
                        part.open(encoding="utf-8"), delimiter="\t"
                    ):
                        fields = SCHEMAS[job][dataset].split(",")
                        assert len(values) == len(fields), (dataset, values)
                        actual.append(dict(zip(fields, values)))
                compare(job, dataset, rows, actual)
                report["datasets"][job + "/" + dataset] = len(actual)
        if args.url:
            for platform in ["alpha", "otc"]:
                with urllib.request.urlopen(
                    args.url + f"/api/dashboard?job={job}&platform={platform}",
                    timeout=90,
                ) as response:
                    payload = json.load(response)
                assert payload["source"] == "hbase-api"
                Path(f"target/api-payloads/{job}-{platform}.json").write_text(
                    json.dumps(payload), encoding="utf-8"
                )
                for dataset, rows in datasets.items():
                    actual = payload["datasets"][dataset]
                    compare(
                        job,
                        dataset,
                        [r for r in rows if r["platform"] == platform],
                        actual,
                    )
                    report["datasets"][job + "/" + platform + "/" + dataset] = len(
                        actual
                    )
    assert args.results or args.url
    Path(args.report).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
