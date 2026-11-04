"""Verify HBase HTTP datasets against an independent raw-input reference."""

import argparse, json, urllib.request
from verify_analytics import reference, compare


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://192.168.52.3:8002")
    parser.add_argument("--input", default=".")
    parser.add_argument("--jobs", default="mr22,mr26")
    parser.add_argument("--report", default="target/analytics-api-verification.json")
    arguments = parser.parse_args()
    expected, quality = reference(arguments.input)
    report = {"quality": quality, "datasets": {}}
    for job in arguments.jobs.split(","):
        for platform in ["otc"] if job == "mr22" else ["alpha", "otc"]:
            with urllib.request.urlopen(
                arguments.url + "/api/dashboard?job=" + job + "&platform=" + platform,
                timeout=60,
            ) as response:
                payload = json.load(response)
            from pathlib import Path

            payload_directory = Path("target/api-payloads")
            payload_directory.mkdir(parents=True, exist_ok=True)
            (payload_directory / (job + "-" + platform + ".json")).write_text(
                json.dumps(payload), encoding="utf-8"
            )
            assert payload["source"] == "hbase-api" and payload["job"] == job
            for kind, rows in payload["datasets"].items():
                columns = payload["schemas"][kind].split(",")
                width = 3 if job in ("mr22", "mr26") and kind != "top" else 2
                actual = {}
                for row in rows:
                    key = tuple(row[column] for column in columns[:width])
                    assert key not in actual
                    actual[key] = tuple(row[column] for column in columns[width:])
                reference_rows = {
                    key: values
                    for key, values in expected[job][kind].items()
                    if key[0] == platform
                }
                compare(reference_rows, actual)
                report["datasets"][job + "/" + platform + "/" + kind] = len(actual)
            print(job, platform, "HBase API matches independent reference", flush=True)
    from pathlib import Path

    Path(arguments.report).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
