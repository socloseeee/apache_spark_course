"""Real ClickHouse/S3 validation of unmodified functions extracted from notebook 05.

All mutations target a new database and a new bucket unique to this invocation.
Existing gold tables and objects are read only for before/after fingerprints.
No credential, raw SQL containing S3 credentials, or server error body is logged.
"""
import ast
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import re
import sys
import uuid

import boto3
import requests


def run(notebook_path, output_path):
    output_path.mkdir(parents=True, exist_ok=True)
    run_id = uuid.uuid4().hex[:16]
    db = 'course_pubcheck_' + run_id
    bucket = 'course-pubcheck-' + run_id
    assert re.fullmatch(r'course_pubcheck_[0-9a-f]{16}', db)
    assert re.fullmatch(r'course-pubcheck-[0-9a-f]{16}', bucket)
    endpoint = os.environ['MINIO_ENDPOINT'].rstrip('/')
    auth = (os.environ['CLICKHOUSE_USER'], os.environ['CLICKHOUSE_PASSWORD'])
    ch_url = f"http://{os.environ['CLICKHOUSE_HOST']}:{os.environ['CLICKHOUSE_PORT']}/"
    protected_db = os.environ.get('CLICKHOUSE_DB', 'gold')
    assert re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', protected_db)
    report = {
        'started_utc': datetime.now(timezone.utc).isoformat(),
        'run_id': run_id,
        'notebook_sha256': hashlib.sha256(notebook_path.read_bytes()).hexdigest(),
        'temporary_database': db,
        'temporary_bucket': bucket,
        'checks': {},
        'events': [],
        'scope': 'Real source-read and staging-CREATE failures, then successful EXCHANGE; no commit crash, timeout, concurrent writer, or multi-table atomicity test.',
        'passed': False,
    }
    created_db = created_bucket = False
    uploaded_keys = []
    phase = 'extract_functions'
    try:
        nb = json.loads(notebook_path.read_text(encoding='utf-8'))
        functions = []
        wanted = {'ch', 'literal', 'mart_totals', 'load_mart'}
        for cell in nb['cells']:
            if cell['cell_type'] == 'code':
                tree = ast.parse(''.join(cell['source']))
                functions += [node for node in tree.body
                              if isinstance(node, ast.FunctionDef) and node.name in wanted]
        assert {node.name for node in functions} == wanted
        module = ast.Module(body=functions, type_ignores=[])
        report['functions_ast_sha256'] = hashlib.sha256(ast.dump(module).encode()).hexdigest()
        scope = dict(os=os, re=re, uuid=uuid, requests=requests, Decimal=Decimal,
                     CH_URL=ch_url, AUTH=auth, DB=db,
                     MINIO=f'{endpoint}/{bucket}',
                     S3KEY=os.environ['MINIO_ACCESS_KEY'],
                     S3SECRET=os.environ['MINIO_SECRET_KEY'])
        exec(compile(module, str(notebook_path), 'exec'), scope)
        real_ch = scope['ch']
        report['clickhouse_version'] = real_ch('SELECT version()').strip()
        assert report['clickhouse_version'] == '25.3.2.39'
        s3 = boto3.client('s3', endpoint_url=endpoint,
                          aws_access_key_id=scope['S3KEY'], aws_secret_access_key=scope['S3SECRET'])

        def protected_fingerprints():
            tables = {}
            for table in ('volume_by_segment', 'top_clients', 'daily_purchases'):
                data = real_ch(f'SELECT * FROM {protected_db}.{table} ORDER BY ALL FORMAT JSONEachRow')
                tables[table] = hashlib.sha256(data.encode()).hexdigest()
            objects = []
            for page in s3.get_paginator('list_objects_v2').paginate(Bucket='gold'):
                objects += [(row['Key'], row['ETag'], row['Size']) for row in page.get('Contents', [])]
            return {'tables_sha256': tables,
                    'gold_objects_sha256': hashlib.sha256(json.dumps(sorted(objects)).encode()).hexdigest(),
                    'gold_object_count': len(objects)}

        phase = 'fingerprint_existing_gold'
        before_protected = protected_fingerprints()
        report['protected_before'] = before_protected
        phase = 'create_owned_fixtures'
        real_ch(f'CREATE DATABASE {db} ENGINE = Atomic')
        created_db = True
        s3.create_bucket(Bucket=bucket)
        created_bucket = True
        fixture_sql = """SELECT 'new' AS segment, toDecimal64('25.25', 2) AS purchase_volume, toUInt64(2) AS tx_count
UNION ALL SELECT 'other', toDecimal64('10.00', 2), toUInt64(1) FORMAT Parquet"""
        response = requests.post(ch_url, auth=auth, data=fixture_sql.encode(),
                                 params={'wait_end_of_query': '1', 'send_progress_in_http_headers': '0'},
                                 timeout=(10, 60))
        if not response.ok or response.headers.get('X-ClickHouse-Exception-Code') not in (None, '', '0'):
            raise RuntimeError('Fixture Parquet generation failed')
        assert response.content[:4] == b'PAR1' and response.content[-4:] == b'PAR1'
        fixture_key = 'gold/v2/publication_case/part-00000.parquet'
        s3.put_object(Bucket=bucket, Key=fixture_key, Body=response.content)
        uploaded_keys.append(fixture_key)
        report['fixture_sha256'] = hashlib.sha256(response.content).hexdigest()

        def make_old_table(table):
            real_ch(f'CREATE TABLE {db}.{table} (segment String, purchase_volume Decimal(18, 2), tx_count UInt64) ENGINE = MergeTree ORDER BY tuple()')
            real_ch(f"INSERT INTO {db}.{table} VALUES ('old', 7.77, 1)")

        def rows(table):
            return [json.loads(line) for line in real_ch(
                f'SELECT segment, toString(purchase_volume) AS purchase_volume, toString(tx_count) AS tx_count FROM {db}.{table} ORDER BY segment FORMAT JSONEachRow').splitlines() if line]

        make_old_table('publication_case')
        make_old_table('missing_source_case')
        old = [{'segment': 'old', 'purchase_volume': '7.77', 'tx_count': '1'}]
        assert rows('publication_case') == rows('missing_source_case') == old
        report['old_rows'] = old

        def observed_ch(sql):
            # Observe only statement category, never raw SQL or credentials.
            event = {'phase': phase, 'verb': sql.split()[0], 'succeeded': False}
            report['events'].append(event)
            answer = real_ch(sql)
            event['succeeded'] = True
            return answer

        scope['ch'] = observed_ch
        phase = 'real_source_read_failure'
        try:
            scope['load_mart']('missing_source_case', 'tuple()', [], with_tx_count=True)
        except RuntimeError:
            report['checks']['real_missing_source_rejected'] = True
        else:
            raise AssertionError('Missing source was unexpectedly accepted')
        report['checks']['previous_table_readable_after_source_failure'] = rows('missing_source_case') == old
        assert report['checks']['previous_table_readable_after_source_failure']

        phase = 'real_staging_create_failure'
        try:
            scope['load_mart']('publication_case', 'nonexistent_order_key', [], with_tx_count=True)
        except RuntimeError:
            report['checks']['real_create_staging_rejected'] = True
        else:
            raise AssertionError('Invalid staging ORDER BY was unexpectedly accepted')
        failure_events = [e for e in report['events'] if e['phase'] == phase]
        report['checks']['source_read_succeeded_before_create_failure'] = any(e['verb'] == 'SELECT' and e['succeeded'] for e in failure_events)
        report['checks']['create_statement_attempted_and_failed'] = any(e['verb'] == 'CREATE' and not e['succeeded'] for e in failure_events)
        report['checks']['no_switch_after_failure'] = not any(e['verb'] in ('EXCHANGE', 'RENAME') for e in report['events'])
        report['checks']['previous_table_readable_after_create_failure'] = rows('publication_case') == old
        assert all(report['checks'].values())

        phase = 'successful_publication'
        scope['load_mart']('publication_case', 'tuple()', [], with_tx_count=True)
        published = rows('publication_case')
        expected = [{'segment': 'new', 'purchase_volume': '25.25', 'tx_count': '2'},
                    {'segment': 'other', 'purchase_volume': '10', 'tx_count': '1'}]
        # Decimal renders 10.00 as 10 or 10.00 depending on the Parquet-inferred scale.
        normalized = [(r['segment'], Decimal(r['purchase_volume']), int(r['tx_count'])) for r in published]
        normalized_expected = [(r['segment'], Decimal(r['purchase_volume']), int(r['tx_count'])) for r in expected]
        report['new_rows'] = published
        report['checks']['successful_publication_replaced_old_data'] = normalized == normalized_expected
        report['checks']['real_exchange_completed'] = any(e['phase'] == phase and e['verb'] == 'EXCHANGE' and e['succeeded'] for e in report['events'])
        stage_names = real_ch(f"SELECT name FROM system.tables WHERE database = '{db}' AND startsWith(name, 'publication_case_staging_') ORDER BY name").splitlines()
        report['checks']['old_version_available_under_staging_name'] = any(rows(name) == old for name in stage_names)
        after_protected = protected_fingerprints()
        report['protected_after'] = after_protected
        report['checks']['existing_gold_tables_and_objects_unchanged'] = before_protected == after_protected
        assert all(report['checks'].values())
        report['passed'] = True
    except Exception as error:
        report['failure_phase'] = phase
        report['failure_type'] = type(error).__name__
        # Deliberately omit exception text: database/S3 errors may echo credentials.
    finally:
        phase = 'cleanup_owned_fixtures'
        cleanup = {}
        if created_db:
            try:
                real_ch(f'DROP DATABASE {db} SYNC')
                cleanup['temporary_database_removed'] = True
            except Exception:
                cleanup['temporary_database_removed'] = False
        if created_bucket:
            try:
                for key in uploaded_keys:
                    s3.delete_object(Bucket=bucket, Key=key)
                s3.delete_bucket(Bucket=bucket)
                cleanup['temporary_bucket_removed'] = True
            except Exception:
                cleanup['temporary_bucket_removed'] = False
        report['cleanup'] = cleanup
        report['finished_utc'] = datetime.now(timezone.utc).isoformat()
        (output_path/'result.json').write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8')
        print(json.dumps({'passed': report['passed'], 'checks_passed': sum(report['checks'].values()),
                          'checks_total': len(report['checks']), 'failure_phase': report.get('failure_phase'),
                          'failure_type': report.get('failure_type'), 'cleanup': cleanup}, ensure_ascii=False))
    return 0 if report['passed'] and all(report['cleanup'].values()) else 1


if __name__ == '__main__':
    sys.exit(run(Path(sys.argv[1]), Path(sys.argv[2])))
