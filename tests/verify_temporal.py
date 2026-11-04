"""MR18/MR25 独立原始 CSV 参考：不复用 Java 分组、排序或基线代码。"""

import argparse, collections, csv, datetime, json, math, urllib.request
from decimal import Decimal
from pathlib import Path


def month_index(month):
    year, number = map(int, month.split("-"))
    return year * 12 + number - 1


def month_text(index):
    return f"{index // 12:04d}-{index % 12 + 1:02d}"


def reference(directory):
    events = []
    rejected = 0
    for path in sorted(Path(directory).glob("*.csv")):
        platform = "alpha" if "alpha" in path.name else "otc"
        for fields in csv.reader(path.open(encoding="utf-8")):
            try:
                if len(fields) != 4:
                    raise ValueError()
                source, target, rating = map(int, fields[:3])
                timestamp = Decimal(fields[3])
                if min(source, target, timestamp) < 0 or abs(rating) > 10:
                    raise ValueError()
                utc = datetime.datetime.fromtimestamp(
                    int(timestamp), datetime.timezone.utc
                )
            except (ValueError, ArithmeticError, OverflowError):
                rejected += 1
                continue
            events.append(
                (platform, source, target, rating, utc.date(), utc.strftime("%Y-%m"))
            )
    daily = {}
    first = {}
    active = set()
    event_counts = collections.Counter()
    bounds = {}
    for platform, source, target, rating, day, month in events:
        counts = daily.setdefault((platform, source, day), [0, 0, set()])
        counts[0] += 1
        counts[1] += rating < 0
        counts[2].add(target)
        key = platform, source
        first[key] = min(first.get(key, month), month)
        event_counts[platform, source, month] += 1
        active.add((platform, source, month))
        bounds[platform] = max(bounds.get(platform, month), month)
    windows = []
    alerts = []
    by_user = collections.defaultdict(list)
    for (platform, user, day), values in daily.items():
        by_user[platform, user].append((day, values))
    for (platform, user), records in sorted(by_user.items()):
        ordered = sorted(records)
        first_day = ordered[0][0]
        past_events, past_negative = [], []
        for day, (event_count, negative_count, targets) in ordered:
            history_days = (day - first_day).days

            # 独立按偏差平方求总体方差；未出现事件的历史日以零参与分母。
            def statistics(values):
                if not history_days:
                    return 0.0, 0.0
                mean = sum(values) / history_days
                zeros = history_days - len(values)
                variance = (
                    math.fsum((value - mean) ** 2 for value in values) + zeros * mean**2
                ) / history_days
                return mean, math.sqrt(variance)

            mean, std = statistics(past_events)
            negative_mean, negative_std = statistics(past_negative)
            threshold = max(5.0, mean + 3 * std)
            negative_threshold = max(5.0, negative_mean + 3 * negative_std)
            burst = history_days >= 7 and event_count >= threshold
            negative_burst = history_days >= 7 and negative_count >= negative_threshold
            status = (
                "insufficient_history"
                if history_days < 7
                else ("candidate" if burst or negative_burst else "normal")
            )
            row = dict(
                platform=platform,
                user_id=str(user),
                window_date=day.isoformat(),
                event_count=str(event_count),
                negative_count=str(negative_count),
                distinct_targets=str(len(targets)),
                history_days=str(history_days),
                baseline_mean=str(mean),
                baseline_std=str(std),
                burst_threshold=str(threshold),
                negative_mean=str(negative_mean),
                negative_std=str(negative_std),
                negative_threshold=str(negative_threshold),
                burst=str(int(burst)),
                negative_burst=str(int(negative_burst)),
                status=status,
            )
            windows.append(row)
            if burst or negative_burst:
                alerts.append(row)
            past_events.append(event_count)
            past_negative.append(negative_count)
    summary = {}
    for row in alerts:
        key = row["platform"], row["user_id"]
        value = summary.setdefault(key, [0, 0, 0])
        value[0] += 1
        value[1] += int(row["negative_burst"])
        value[2] = max(value[2], int(row["event_count"]))
    users18 = [
        dict(
            platform=p,
            user_id=u,
            alert_windows=str(v[0]),
            negative_alert_windows=str(v[1]),
            max_event_count=str(v[2]),
        )
        for (p, u), v in sorted(summary.items())
    ]
    users25 = [
        dict(platform=p, user_id=str(u), first_active_month=month)
        for (p, u), month in sorted(first.items())
    ]
    cohort_sizes = collections.Counter((p, month) for (p, u), month in first.items())
    active_counts = collections.Counter(
        (p, first[p, u], month_index(month) - month_index(first[p, u]))
        for p, u, month in active
    )
    contribution_counts = collections.Counter()
    for (platform, user, month), event_count in event_counts.items():
        contribution_counts[platform, first[platform, user], month_index(month)-month_index(first[platform, user])] += event_count
    cells = []
    for (platform, cohort), size in sorted(cohort_sizes.items()):
        observed = month_index(bounds[platform]) - month_index(cohort)
        for offset in range(max(12, observed) + 1):
            count = active_counts[platform, cohort, offset]
            available = offset <= observed
            cells.append(
                dict(
                    platform=platform,
                    cohort_month=cohort,
                    month_offset=str(offset),
                    activity_month=month_text(month_index(cohort) + offset),
                    cohort_size=str(size),
                    active_users=str(count) if available else "N/A",
                    activity_rate=str(count / size) if available else "N/A",
                    event_count=str(contribution_counts[platform, cohort, offset]) if available else "N/A",
                    comparison_eligible=str(size >= 30 and offset < observed).lower(),
                    observation_status=(
                        "not_observed"
                        if not available
                        else ("cutoff_month" if offset == observed else "observed")
                    ),
                )
            )
    return {
        "mr18": {"windows": windows, "alerts": alerts, "users": users18},
        "mr25": {"users": users25, "cells": cells},
    }, dict(valid_events=len(events), rejected_events=rejected, cutoff_month=bounds)


FLOAT_COLUMNS = {
    "baseline_mean",
    "baseline_std",
    "burst_threshold",
    "negative_mean",
    "negative_std",
    "negative_threshold",
    "activity_rate",
}


def identity(job, dataset, row):
    if job == "mr18" and dataset != "users":
        return row["platform"], row["user_id"], row["window_date"]
    if job == "mr25" and dataset == "cells":
        return row["platform"], row["cohort_month"], row["month_offset"]
    return row["platform"], row["user_id"]


def compare(job, dataset, expected, actual):
    left = {identity(job, dataset, row): row for row in expected}
    right = {identity(job, dataset, row): row for row in actual}
    assert len(right) == len(actual), "duplicate business keys"
    assert left.keys() == right.keys(), (
        job,
        dataset,
        "keys",
        len(left),
        len(right),
        list(left.keys() - right.keys())[:3],
        list(right.keys() - left.keys())[:3],
    )
    for key, row in left.items():
        assert row.keys() == right[key].keys(), (key, "columns")
        for column, value in row.items():
            actual_value = right[key][column]
            if column in FLOAT_COLUMNS and value != "N/A":
                assert math.isclose(
                    float(value), float(actual_value), rel_tol=1e-9, abs_tol=1e-8
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default=".")
    parser.add_argument("--results")
    parser.add_argument("--url")
    parser.add_argument("--jobs", default="mr18,mr25")
    parser.add_argument("--report", default="target/temporal-verification.json")
    args = parser.parse_args()
    expected, quality = reference(args.input)
    expected = {job: expected[job] for job in args.jobs.split(",")}
    report = {"quality": quality, "datasets": {}}
    schemas = {
        "mr18": {
            "windows": "platform,user_id,window_date,event_count,negative_count,distinct_targets,history_days,baseline_mean,baseline_std,burst_threshold,negative_mean,negative_std,negative_threshold,burst,negative_burst,status",
            "alerts": "platform,user_id,window_date,event_count,negative_count,distinct_targets,history_days,baseline_mean,baseline_std,burst_threshold,negative_mean,negative_std,negative_threshold,burst,negative_burst,status",
            "users": "platform,user_id,alert_windows,negative_alert_windows,max_event_count",
        },
        "mr25": {
            "users": "platform,user_id,first_active_month",
            "cells": "platform,cohort_month,month_offset,activity_month,cohort_size,active_users,activity_rate,observation_status,event_count,comparison_eligible",
        },
    }
    if args.results:
        for job, datasets in expected.items():
            for dataset, rows in datasets.items():
                directory = Path(args.results) / job / dataset
                assert (directory / "_SUCCESS").exists(), directory
                actual = []
                for part in directory.glob("part-*"):
                    for fields in csv.reader(
                        part.open(encoding="utf-8"), delimiter="\t"
                    ):
                        assert len(fields) == len(
                            schemas[job][dataset].split(",")
                        ), fields
                        actual.append(
                            dict(zip(schemas[job][dataset].split(","), fields))
                        )
                compare(job, dataset, rows, actual)
                report["datasets"][job + "/" + dataset] = len(actual)
    if args.url:
        for job, datasets in expected.items():
            for platform in ["alpha", "otc"]:
                with urllib.request.urlopen(
                    args.url + "/api/dashboard?job=" + job + "&platform=" + platform,
                    timeout=90,
                ) as response:
                    payload = json.load(response)
                assert payload["source"] == "hbase-api"
                Path("target/api-payloads").mkdir(parents=True, exist_ok=True)
                Path("target/api-payloads", job + "-" + platform + ".json").write_text(
                    json.dumps(payload), encoding="utf-8"
                )
                for dataset, rows in datasets.items():
                    actual = payload["datasets"][dataset]
                    compare(
                        job,
                        dataset,
                        [row for row in rows if row["platform"] == platform],
                        actual,
                    )
                    report["datasets"][job + "/" + platform + "/" + dataset] = len(
                        actual
                    )
    assert args.results or args.url, "provide --results or --url"
    Path(args.report).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
