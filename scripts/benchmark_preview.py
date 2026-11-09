"""Compare the chat preview plans on synthetic history; all rows are rolled back.

DATABASE_URL_TEST=mysql://... python3 scripts/benchmark_preview.py --rows 10000
Requires the test database migrations and the mysql command-line client.
"""

import argparse
import json
import os
import re
import statistics
import subprocess
import tempfile
import uuid
from pathlib import Path
from urllib.parse import unquote, urlparse


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--repetitions", type=int, default=7)
    args = parser.parse_args()
    if args.rows < 2 or args.repetitions < 1:
        parser.error("rows must be >= 2 and repetitions >= 1")
    url = urlparse(os.environ["DATABASE_URL_TEST"])
    if url.scheme != "mysql" or not url.path.strip("/"):
        parser.error("DATABASE_URL_TEST must identify a MySQL test database")

    def sql_uuid(value):
        return f"UNHEX('{value.hex}')"

    tenant, member, chat = [sql_uuid(uuid.uuid4()) for _ in range(3)]
    statements = [
        "START TRANSACTION;",
        f"INSERT INTO tenants(id,name) VALUES ({tenant},'preview benchmark');",
        f"INSERT INTO members(id,tenant_id,name,role) VALUES ({member},{tenant},'benchmark','adult');",
        f"INSERT INTO chats(id,tenant_id,is_group,name) VALUES ({chat},{tenant},false,'benchmark');",
    ]
    for start in range(0, args.rows, 500):
        values = []
        for i in range(start, min(start + 500, args.rows)):
            created = f"TIMESTAMPADD(MICROSECOND,{i},'2026-01-01 00:00:00')"
            values.append(f"({sql_uuid(uuid.uuid4())},{chat},{tenant},{member},'text','ciphertext','iv',1,{i % 2},{created})")
        statements.append(
            "INSERT INTO messages(id,chat_id,tenant_id,sender_member_id,type,ciphertext,iv,enc_version,is_deleted,created_at) VALUES "
            + ",".join(values) + ";"
        )
    for name, index in [("before", "messages_chat_created_idx"), ("after", None)]:
        hint = f"FORCE INDEX ({index}) " if index else ""
        for _ in range(args.repetitions):
            statements.extend([
                f"SELECT 'BENCH:{name}';",
                "EXPLAIN ANALYZE SELECT ciphertext,iv,type,created_at FROM messages "
                f"{hint}WHERE chat_id={chat} AND tenant_id={tenant} "
                "AND is_deleted=false ORDER BY created_at DESC,id DESC LIMIT 1;",
            ])
    statements.append("ROLLBACK;")

    def option(value):
        return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'

    with tempfile.TemporaryDirectory(prefix="chat-preview-benchmark-") as folder:
        config = Path(folder) / "mysql.cnf"
        config.write_text("[client]\n" + "\n".join(
            f"{key}={option(value)}" for key, value in {
                "host": url.hostname or "127.0.0.1", "port": url.port or 3306,
                "user": unquote(url.username or ""), "password": unquote(url.password or ""),
                "database": unquote(url.path.lstrip("/")),
            }.items()
        ))
        config.chmod(0o600)
        output = subprocess.run(
            ["mysql", f"--defaults-extra-file={config}", "--batch", "--raw", "--skip-column-names"],
            input="\n".join(statements), text=True, capture_output=True, check=True,
        ).stdout

    result = {"synthetic_messages": args.rows, "deleted_messages": args.rows // 2, "repetitions": args.repetitions}
    for name in ("before", "after"):
        plans = re.findall(rf"BENCH:{name}\n(.*?)(?=BENCH:|$)", output, flags=re.S)
        durations = [float(re.search(r"actual time=[\d.eE+-]+\.\.([\d.eE+-]+)", plan).group(1)) for plan in plans]
        result[name] = {"median_ms": statistics.median(durations), "sample_plan": plans[-1].strip()}
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
