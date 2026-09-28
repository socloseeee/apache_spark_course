"""Read-only persisted Delta evidence for the 2026-09-21 notebook 02 rerun."""
from datetime import datetime, timezone
from pathlib import Path
import hashlib
import json
import sys

from delta.tables import DeltaTable
from pyspark.sql import SparkSession, functions as F


spark = SparkSession.builder.appName("quality-silver-storage-evidence").getOrCreate()
spark.sparkContext.setLogLevel("WARN")
spark.conf.set("spark.sql.session.timeZone", "UTC")
result = {
    "captured_at_utc": datetime.now(timezone.utc).isoformat(),
    "spark_version": spark.version,
    "tables": {},
}
for name in (
    "clients", "accounts", "transactions", "clients_scd2",
    "quarantine/clients", "quarantine/accounts", "quarantine/transactions",
    "quarantine/malformed_json",
):
    path = "s3a://silver/v2/" + name
    frame = spark.read.format("delta").load(path)
    detail = {
        "count": frame.count(),
        "schema": frame.schema.jsonValue(),
        "latest_history": DeltaTable.forPath(spark, path).history(1).first().asDict(recursive=True),
    }
    if "reject_reason" in frame.columns:
        detail["quarantine_by_reason"] = {
            row["reject_reason"]: row["count"]
            for row in frame.groupBy("reject_reason").count().orderBy("reject_reason").collect()
        }
    if name in ("clients", "accounts", "transactions"):
        key = {"clients": "client_id", "accounts": "account_id", "transactions": "tx_id"}[name]
        detail["duplicate_keys"] = frame.groupBy(key).count().filter("count > 1").count()
        detail["null_keys"] = frame.filter(F.col(key).isNull()).count()
        detail["diagnostic_raw_columns"] = sorted(set(frame.columns) & {
            "client_id_raw", "account_id_raw", "balance_raw", "amount_raw"
        })
    result["tables"][name] = detail
    print(name, "count", detail["count"], "version", detail["latest_history"]["version"], flush=True)

result["checks"] = {
    "accepted_keys_unique": all(result["tables"][name]["duplicate_keys"] == 0 for name in ("clients", "accounts", "transactions")),
    "accepted_keys_nonnull": all(result["tables"][name]["null_keys"] == 0 for name in ("clients", "accounts", "transactions")),
    "accepted_diagnostic_columns_absent": all(not result["tables"][name]["diagnostic_raw_columns"] for name in ("clients", "accounts", "transactions")),
}
output = Path("/home/jovyan/results/quality-silver-20260921") / sys.argv[1]
output.write_text(json.dumps(result, ensure_ascii=False, default=str, indent=2) + "\n", encoding="utf-8")
print("STORAGE_DETAIL_SAVED", output, flush=True)
spark.stop()
