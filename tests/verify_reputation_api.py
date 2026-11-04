"""核对 HBase HTTP 数据与独立原始 CSV 参考，并保存页面回归使用的真实响应。"""

import argparse, json, urllib.request
from pathlib import Path
from verify_reputation import reference, validate


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://192.168.52.3:8002")
    parser.add_argument("--input", default=".")
    parser.add_argument("--report", default="target/reputation-api-verification.json")
    args = parser.parse_args()
    expected = reference(args.input)
    report = {}
    payload_directory = Path("target/api-payloads")
    payload_directory.mkdir(parents=True, exist_ok=True)
    for platform, methods in expected.items():
        with urllib.request.urlopen(
            args.url + "/api/dashboard?job=mr7&platform=" + platform, timeout=90
        ) as response:
            payload = json.load(response)
        assert payload["source"] == "hbase-api"
        for method, users in methods.items():
            actual = {}
            for row in payload["results"][method]:
                user = int(row["user_id"])
                assert user not in actual
                actual[user] = {
                    "in_degree": str(row["in_degree"]),
                    "score": "N/A" if row["score"] is None else str(row["score"]),
                    "rank": "null" if row["rank"] is None else str(row["rank"]),
                }
            report[method + "/" + platform] = validate(users, actual, method, platform)
            assert payload["hdfs_base"][method].startswith(
                "/bitcoin/results/"
            ), payload["hdfs_base"]
            if method != "direct_average":
                history = payload["iterations"][method]
                assert history and history[-1]["l1_delta"] < 0.01
                assert [row["iteration"] for row in history] == list(
                    range(1, len(history) + 1)
                )
                report[method + "/" + platform + "/iterations"] = len(history)
        (payload_directory / ("mr7-" + platform + ".json")).write_text(
            json.dumps(payload), encoding="utf-8"
        )
        print(platform, "all HBase reputation rows match independent reference")
    Path(args.report).write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
