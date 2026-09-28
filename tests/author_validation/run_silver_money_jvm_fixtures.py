"""Run the existing money matrix using JVM literal fixtures, without Python workers.

Business expressions, expectations and assertions remain in run_silver_money.py.
Only its explicit-schema createDataFrame fixture boundary is adapted. No storage
writes occur. Useful on Windows where Python worker setup may differ from Linux.
"""
import argparse
import hashlib
import json
from pathlib import Path
from unittest.mock import patch

from pyspark.sql import SparkSession
from run_silver_money import run
from run_silver_quality import jvm_frame


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('notebook', type=Path)
    parser.add_argument('output', type=Path)
    args = parser.parse_args()
    def fixture_frame(spark, rows, schema=None, *extra, **options):
        if not isinstance(rows, list) or not isinstance(schema, str) or extra or options:
            raise AssertionError('Unexpected fixture constructor')
        return jvm_frame(spark, rows, schema)
    with patch.object(SparkSession, 'createDataFrame', fixture_frame):
        code = run(args.notebook, args.output)
    report = json.loads(args.output.read_text(encoding='utf-8'))
    report['fixture_adapter'] = {
        'kind': 'JVM literal expressions over spark.range; actual business transformations unchanged',
        'adapter_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'fixture_helper_sha256': hashlib.sha256(Path(__file__).with_name('run_silver_quality.py').read_bytes()).hexdigest(),
    }
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
    raise SystemExit(code)
