"""从已完成的 HDFS 业务目录收集验收数据；SSH 密码通过 MR_SSH_PASSWORD 提供。"""

import argparse, os, shlex, stat, uuid
from pathlib import Path
import paramiko


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="192.168.52.3")
    parser.add_argument("--user", default="root")
    parser.add_argument("--hdfs-root", default="/bitcoin/results")
    parser.add_argument("--output", default="target/distributed-full-results")
    parser.add_argument(
        "--jobs",
        default="direct_average,positive_katz,signed_katz,mr22,mr26,mr18,mr25,mr27",
    )
    arguments = parser.parse_args()
    project = Path(__file__).resolve().parents[1]
    connection = paramiko.SSHClient()
    connection.load_host_keys(str(project / "scripts/ssh_known_hosts"))
    connection.connect(
        arguments.host,
        username=arguments.user,
        password=os.environ.get("MR_SSH_PASSWORD"),
        timeout=15,
    )
    staging = "/tools/MR/validation-" + uuid.uuid4().hex
    jobs = [job.strip() for job in arguments.jobs.split(",") if job.strip()]
    log_files = {
        "direct_average": "DirectAverage-run.log",
        "positive_katz": "PositiveKatz-run.log",
        "signed_katz": "SignedKatz-run.log",
        "mr22": "ActivityHeatmap-run.log",
        "mr26": "DirectionalAsymmetry-run.log",
        "mr18": "ActivityBursts-run.log",
        "mr25": "CohortActivity-run.log",
        "mr27": "PathConflicts-run.log",
    }
    if not jobs or any(job not in log_files for job in jobs):
        raise ValueError("Unknown or empty job list")
    sources = " ".join(shlex.quote(arguments.hdfs_root + "/" + job) for job in jobs)
    command = (
        "mkdir -p "
        + shlex.quote(staging)
        + " && hadoop fs -get "
        + sources
        + " "
        + shlex.quote(staging)
    )
    _, output, _ = connection.exec_command(
        "bash -lc " + shlex.quote("{ " + command + "; } 2>&1")
    )
    print(output.read().decode("utf-8", errors="replace"))
    assert output.channel.recv_exit_status() == 0, "HDFS download failed"
    sftp = connection.open_sftp()

    def fetch(remote, local):
        local.mkdir(parents=True, exist_ok=True)
        for item in sftp.listdir_attr(remote):
            if item.filename.startswith(".") or item.filename in (
                "stages",
                "bounds",
                "hbase-import",
            ):
                continue
            if stat.S_ISDIR(item.st_mode):
                fetch(remote + "/" + item.filename, local / item.filename)
            else:
                sftp.get(remote + "/" + item.filename, str(local / item.filename))

    fetch(staging, project / arguments.output)
    for job in jobs:
        sftp.get(
            "/tools/MR/" + log_files[job],
            str(project / arguments.output / (job + ".log")),
        )
    connection.close()
    print("Collected:", arguments.output, "; Linux validation copy:", staging)


if __name__ == "__main__":
    main()
