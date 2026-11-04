"""独立原始数据参考；校验所有节点、分数、入度、实际竞赛排名及收敛历史。"""

import argparse, csv, json, math, collections
from decimal import Decimal
from pathlib import Path


def reference(directory):
    nodes = collections.defaultdict(set)
    edges = {}
    for path in sorted(Path(directory).glob("*.csv")):
        platform = "alpha" if "alpha" in path.name else "otc"
        offset = 0
        for raw in path.read_bytes().splitlines(keepends=True):
            event_id = path.name + ":" + str(offset).zfill(20)
            offset += len(raw)
            try:
                source, target, rating, time = raw.decode().strip().split(",")
                source, target, rating = int(source), int(target), int(rating)
                time = Decimal(time)
                if min(source, target, time) < 0 or abs(rating) > 10:
                    continue
            except (ValueError, ArithmeticError):
                continue
            nodes[platform].update((source, target))
            key = platform, source, target
            if key not in edges or (time, rating, event_id) > (
                edges[key][1],
                edges[key][0],
                edges[key][2],
            ):
                edges[key] = (rating, time, event_id)
    expected = {}
    for platform, users in nodes.items():
        incoming = collections.defaultdict(list)
        relations = []
        for (edge_platform, source, target), (rating, time, event_id) in edges.items():
            if edge_platform == platform:
                incoming[target].append(rating)
                relations.append((source, target, rating))
        expected[platform] = {}
        expected[platform]["direct_average"] = {
            user: (
                len(incoming[user]),
                sum(incoming[user]) / len(incoming[user]) if incoming[user] else None,
            )
            for user in users
        }
        for method in ["positive_katz", "signed_katz"]:
            denominators = collections.defaultdict(float)
            for source, target, rating in relations:
                denominators[source] += (
                    max(0, rating) if method == "positive_katz" else abs(rating)
                )
            scores = {user: 1.0 for user in users}
            for iteration in range(1, 201):
                contributions = collections.defaultdict(list)
                for source, target, rating in relations:
                    if denominators[source] and (method == "signed_katz" or rating > 0):
                        contributions[target].append(
                            0.8 * rating / denominators[source] * scores[source]
                        )
                updated = {user: 1 + math.fsum(contributions[user]) for user in users}
                delta = math.fsum(abs(updated[user] - scores[user]) for user in users)
                scores = updated
                if delta < 1e-12:
                    break
            else:
                raise AssertionError("Reference did not converge")
            expected[platform][method] = {
                user: (len(incoming[user]), scores[user]) for user in users
            }
    return expected


def validate(expected, actual, method, platform):
    assert expected.keys() == actual.keys(), (method, platform, "node mismatch")
    for user, (count, score) in expected.items():
        row = actual[user]
        assert int(row["in_degree"]) == count
        if score is None:
            assert row["score"] == "N/A"
        else:
            assert math.isclose(
                float(row["score"]),
                score,
                rel_tol=1e-10,
                abs_tol=(1e-9 if method == "direct_average" else 0.04),
            ), (method, platform, user, score, row["score"])
        if count < 5:
            assert row["rank"] == "null"
    if method != "direct_average":
        total_error = math.fsum(
            abs(float(actual[user]["score"]) - score)
            for user, (count, score) in expected.items()
        )
        assert total_error < 0.04 + 1e-8, (
            method,
            platform,
            "L1 fixed-point error bound exceeded",
            total_error,
        )
    # 排名以实际浮点得分为准，严格核对 1、1、3 规则；独立参考另外核对数值。
    ordered = sorted(
        (user for user in actual if int(actual[user]["in_degree"]) >= 5),
        key=lambda user: (-float(actual[user]["score"]), user),
    )
    previous = None
    rank = 0
    for position, user in enumerate(ordered, 1):
        score = float(actual[user]["score"])
        if previous is None or score != previous:
            rank = position
        assert int(actual[user]["rank"]) == rank, (
            method,
            platform,
            user,
            "bad distributed rank",
            rank,
            actual[user]["rank"],
        )
        previous = score
    return len(actual)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--results", required=True)
    args = parser.parse_args()
    expected = reference(args.input)
    report = {}
    for platform, methods in expected.items():
        for method, users in methods.items():
            root = Path(args.results) / method / platform
            actual = {}
            for part in root.glob("scores-r-*"):
                for row in csv.DictReader(part.open(encoding="utf-8")):
                    user = int(row["user_id"])
                    assert user not in actual
                    actual[user] = row
            report[method + "/" + platform] = validate(users, actual, method, platform)
            if method != "direct_average":
                history = []
                for part in root.glob("iterations-r-*"):
                    history.extend(csv.DictReader(part.open(encoding="utf-8")))
                assert [int(row["iteration"]) for row in history] == list(
                    range(1, len(history) + 1)
                )
                assert history and float(history[-1]["l1_delta"]) < 0.01
                report[method + "/" + platform + "/iterations"] = len(history)
    print(json.dumps(report, indent=2))
    Path(args.results, "mr7-verification.json").write_text(
        json.dumps(report, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
