"""Independent reference verification: no Java core algorithm is reused. Compare actual Hadoop TSV datasets."""

import argparse, collections, csv, datetime, json, math
from decimal import Decimal
from pathlib import Path


def reference(input_directory):
    events = []
    rejected = 0
    for path in sorted(Path(input_directory).glob("*.csv")):
        platform = "alpha" if "alpha" in path.name else "otc"
        offset = 0
        for raw in path.read_bytes().splitlines(keepends=True):
            event_id = path.name + ":" + str(offset).zfill(20)
            offset += len(raw)
            try:
                fields = raw.decode("utf-8").strip().split(",")
                if len(fields) != 4:
                    raise ValueError()
                source, target, rating = map(int, fields[:3])
                time = Decimal(fields[3])
                if min(source, target, time) < 0 or abs(rating) > 10:
                    raise ValueError()
            except (ValueError, UnicodeDecodeError, ArithmeticError):
                rejected += 1
                continue
            events.append((platform, source, target, rating, time, event_id))
    snapshot = {}
    for event in events:
        platform, source, target, rating, time, event_id = event
        if source == target:
            continue
        key = (platform, source, target)
        if key not in snapshot or (time, event_id) > snapshot[key][1:]:
            snapshot[key] = (rating, time, event_id)
    heat = collections.Counter()
    for platform, source, target, rating, time, event_id in events:
        if platform != "otc":
            continue
        timestamp = datetime.datetime.fromtimestamp(int(time), datetime.timezone.utc)
        heat[(platform, str(timestamp.isoweekday()), str(timestamp.hour))] += 1
    mr22 = {key: (count,) for key, count in heat.items()}
    mr26 = {}
    for (platform, source, target), (forward, time, event_id) in snapshot.items():
        if source >= target or (platform, target, source) not in snapshot:
            continue
        reverse = snapshot[(platform, target, source)][0]
        difference = abs(forward - reverse)
        denominator = abs(forward) + abs(reverse)
        mr26[(platform, str(source), str(target))] = (
            forward,
            reverse,
            difference,
            difference / denominator if denominator else 0,
            int(forward * reverse < 0),
        )
    top26 = {}
    for platform in ["alpha", "otc"]:
        pairs = sorted(
            (key for key in mr26 if key[0] == platform),
            key=lambda key: (-mr26[key][2], int(key[1]), int(key[2])),
        )[:10]
        for rank, key in enumerate(pairs, 1):
            top26[(platform, str(rank))] = (int(key[1]), int(key[2])) + mr26[key]
    return {
        "mr22": {"heatmap": mr22},
        "mr26": {"details": mr26, "top": top26},
    }, dict(
        valid_events=len(events), rejected_events=rejected, snapshot_edges=len(snapshot)
    )


def compare(expected, actual):
    assert expected.keys() == actual.keys(), (
        "business keys differ",
        len(expected),
        len(actual),
        list(expected.keys() - actual.keys())[:4],
        list(actual.keys() - expected.keys())[:4],
    )
    for key, values in expected.items():
        assert len(values) == len(actual[key])
        for left, right in zip(values, actual[key]):
            assert math.isclose(
                float(left), float(right), rel_tol=1e-10, abs_tol=1e-9
            ), (key, left, right)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--results", required=True)
    arguments = parser.parse_args()
    expected, quality = reference(arguments.input)
    summary = {"quality": quality, "datasets": {}}
    for job, datasets in expected.items():
        for kind, rows in datasets.items():
            actual = {}
            directory = Path(arguments.results) / job / kind
            assert (directory / "_SUCCESS").exists(), directory
            for part in directory.glob("part-*"):
                for fields in csv.reader(part.open(encoding="utf-8"), delimiter="\t"):
                    width = 3 if job in ("mr22", "mr26") and kind != "top" else 2
                    key = tuple(fields[:width])
                    assert key not in actual
                    actual[key] = tuple(fields[width:])
            compare(rows, actual)
            summary["datasets"][job + "/" + kind] = len(rows)
    print(json.dumps(summary, indent=2))
    Path(arguments.results, "verification.json").write_text(
        json.dumps(summary, indent=2), encoding="utf-8"
    )


if __name__ == "__main__":
    main()
