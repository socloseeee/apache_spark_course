"""Author-only non-money quality matrix using actual notebook expressions.

Usage: python run_silver_quality.py NOTEBOOK OUTPUT_JSON [--observe]
Uses the money runner's AST extraction for save_clean, accounts and transactions.
Fixtures are JVM expressions over spark.range, so no Python workers are needed.
No storage writes occur. Keep both runners together and course_checks.py beside
the notebook. An observe run records failures without treating them as success.
"""
import argparse
import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys

from run_silver_money import extract, source_cell


def fixture_cases():
    cases = {'clients': [], 'accounts': [], 'transactions': []}
    defaults = {
        'clients': dict(client_id='1', full_name='Fixture', city='Moscow', segment='mass', reg_date='2026-01-01'),
        'accounts': dict(account_id='1', client_id='1', balance='1.00', open_date='2026-01-01', status='active', acc_type='debit'),
        'transactions': dict(tx_id='tx-1', batch_id='demo-001', account_id=1, amount='1.00', currency='RUB', tx_type='purchase', merchant='fixture', status='completed', ts='2026-01-01T00:00:00Z'),
    }

    def add(table, name, updates=None, reasons=(), expected=None):
        row = dict(defaults[table])
        number = len(cases[table])
        key = {'clients': 'client_id', 'accounts': 'account_id', 'transactions': 'tx_id'}[table]
        row[key] = ('tx-' if table == 'transactions' else '') + str(1000 + number) if number else row[key]
        row.update(updates or {})
        marker = 'merchant' if table == 'transactions' else '_case'
        row[marker] = name
        cases[table].append(dict(name=name, row=row, reasons=list(reasons), expected=expected or {}))

    for table in cases:
        add(table, 'valid')
    ids = [('integer_spaces', ' 104 ', (), 104), ('leading_zeros', '00105', (), 105),
           ('fractional_id', '101.5', ('id',), None), ('decimal_id', '102.0', ('id',), None),
           ('exponent_id', '103e0', ('id',), None), ('plus_id', '+106', ('id',), None),
           ('zero_id', '0', ('id',), None), ('negative_id', '-1', ('id',), None),
           ('missing_id', None, ('id',), None), ('empty_id', '', ('id',), None),
           ('overflow_id', '9223372036854775808', ('id',), None)]
    for table, key in [('clients', 'client_id'), ('accounts', 'account_id')]:
        for name, raw, reasons, parsed in ids:
            add(table, name, {key: raw}, (key,) if reasons else (), {key: parsed} if not reasons else {})

    for table, column in [('clients', 'reg_date'), ('accounts', 'open_date')]:
        for name, value, valid in [('leap_date', '2024-02-29', True), ('invalid_leap_date', '2025-02-29', False),
                                   ('invalid_month', '2026-13-01', False), ('missing_date', None, False), ('empty_date', '', False)]:
            add(table, name, {column: value}, () if valid else (column,))
    for field in ['city', 'full_name']:
        for name, value in [('missing', None), ('empty', ''), ('spaces', '   ')]:
            add('clients', field + '_' + name, {field: value}, (field,))
    add('clients', 'trim_city', {'city': '  Moscow  '}, expected={'city': 'Moscow'})
    for table, field, valid_values in [
        ('clients', 'segment', ['mass', 'affluent', 'private']),
        ('accounts', 'status', ['active', 'blocked']),
        ('accounts', 'acc_type', ['debit', 'credit', 'savings']),
        ('transactions', 'status', ['completed', 'pending', 'failed']),
        ('transactions', 'tx_type', ['purchase', 'withdrawal', 'transfer', 'refund']),
    ]:
        for value in valid_values:
            add(table, field + '_' + value, {field: value})
        for name, value in [('unknown', 'unsupported'), ('missing', None), ('empty', '')]:
            add(table, field + '_' + name, {field: value}, (field,))

    for name, value, reasons in [
        ('orphan_client', '999999', ('orphan_client',)),
        ('missing_client', None, ('client_id', 'orphan_client')),
        ('zero_client', '0', ('client_id', 'orphan_client')),
        ('fractional_client', '1.5', ('client_id', 'orphan_client')),
        ('decimal_client', '1.0', ('client_id', 'orphan_client')),
    ]:
        add('accounts', name, {'client_id': value}, reasons)
    # This parent could otherwise be fabricated by truncating clients' 101.5.
    add('accounts', 'parent_rejected_at_source', {'client_id': '101'}, ('orphan_client',))
    for name, value in [('missing_tx_id', None), ('empty_tx_id', ''), ('spaces_tx_id', '   '),
                        ('tab_tx_id', '\t'), ('newline_tx_id', '\n')]:
        add('transactions', name, {'tx_id': value}, ('tx_id',))
    for name, value, reasons in [('orphan_account', 999999, ('orphan_account',)),
                                 ('missing_account', None, ('account_id', 'orphan_account')),
                                 ('zero_account', 0, ('account_id', 'orphan_account')),
                                 ('rejected_account', 101, ('orphan_account',))]:
        add('transactions', name, {'account_id': value}, reasons)
    for name, value in [('other_currency', 'USD'), ('missing_currency', None), ('empty_currency', '')]:
        add('transactions', name, {'currency': value}, ('currency',))
    for name, value in [('invalid_ts', '2025-02-29T00:00:00Z'), ('missing_ts', None), ('empty_ts', '')]:
        add('transactions', name, {'ts': value}, ('ts',))
    add('transactions', 'timestamp_offset', {'ts': '2026-01-01T03:00:00+03:00'},
        expected={'ts': '2026-01-01 00:00:00'})
    add('clients', 'multiple_errors', {'city': None, 'segment': 'unknown'}, ('city', 'segment'))
    return cases


def jvm_frame(spark, rows, schema):
    """Small literal fixtures, with no Python-worker or storage dependency."""
    from pyspark.sql import functions as F
    fields = [part.strip().split() for part in schema.split(',')]
    if any(len(f) != 2 or f[1] not in ('string', 'long', 'binary') for f in fields):
        raise AssertionError('Unexpected fixture schema')
    idx = F.col('id').cast('int') + 1
    return spark.range(len(rows)).select(*[
        (F.element_at(F.array(*[F.lit(row[i]) for row in rows]), idx) if rows else F.lit(None))
        .cast(dtype).alias(name) for i, (name, dtype) in enumerate(fields)])


def run(notebook_path, output, observe=False):
    report = dict(started_utc=datetime.now(timezone.utc).isoformat(), passed=False,
                  mode='observe' if observe else 'assert', checks=[], tables={},
                  runner_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                  helper_sha256=hashlib.sha256(Path(__file__).with_name('run_silver_money.py').read_bytes()).hexdigest(),
                  scope='Actual notebook cells 4, 6, 8 and save_clean; JVM literal fixtures; no storage writes; no CSV parser, malformed JSON routing or performance test.')
    spark = None
    phase = 'extract'
    def check(name, condition):
        report['checks'].append(dict(name=name, passed=bool(condition)))
    try:
        raw = notebook_path.read_bytes()
        report['notebook_sha256'] = hashlib.sha256(raw).hexdigest()
        notebook = json.loads(raw)
        setup, cells, extraction = extract(notebook, notebook_path)
        clients_ast = source_cell(notebook, 4)
        assert all(isinstance(n, ast.Assign) for n in clients_ast.body)
        assert {t.id for n in clients_ast.body for t in n.targets} == {'clients', 'clients_clean'}
        cells[4] = compile(clients_ast, str(notebook_path) + ':cell4', 'exec')
        report['extraction'] = extraction
        report['clients_ast_sha256'] = hashlib.sha256(ast.dump(clients_ast).encode()).hexdigest()
        phase = 'spark'
        from pyspark.sql import SparkSession, functions as F
        sys.path.insert(0, str(notebook_path.parent.resolve()))
        from course_checks import quality_split, checked_dedup
        report['course_checks_sha256'] = hashlib.sha256(notebook_path.with_name('course_checks.py').read_bytes()).hexdigest()
        spark = (SparkSession.builder.master('local[2]').appName('author-silver-quality')
                 .config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1')
                 .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        spark.conf.set('spark.sql.ansi.enabled', 'false')
        spark.conf.set('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        report['spark_version'] = spark.version
        check('spark_version', spark.version == '3.5.3')
        captures = {}
        def capture(name, typed, rules, key, good, bad, clean):
            captures[name] = {'typed': typed, 'bad': bad, 'rule_names': list(rules)}
        scope = dict(spark=spark, F=F, BATCH_ID='demo-001', quality_split=quality_split,
                     checked_dedup=checked_dedup, _capture_clean=capture)
        exec(setup, scope)
        fixtures = fixture_cases()
        for table, index, returned in [('clients', 4, 'clients_clean'), ('accounts', 6, 'accounts_clean'), ('transactions', 8, 'tx_clean')]:
            phase = table
            cases = fixtures[table]
            marker = 'merchant' if table == 'transactions' else '_case'
            if table == 'transactions':
                rows = [(json.dumps(c['row'], ensure_ascii=False).encode('utf-8'),) for c in cases]
                scope['_transactions_fixture'] = jvm_frame(spark, rows, 'value binary')
            else:
                columns = list(cases[0]['row'])
                frame = jvm_frame(spark, [tuple(c['row'][k] for k in columns) for c in cases],
                                  ', '.join(k + ' string' for k in columns))
                def read_csv(name, expected=table, df=frame):
                    assert name == expected
                    return df
                scope['read_csv'] = read_csv
            exec(cells[index], scope)
            # Python collect() represents TimestampType using the driver's local
            # timezone. Format in Spark's declared UTC session before comparison.
            def serializable(df):
                return df.withColumn('ts', F.date_format('ts', 'yyyy-MM-dd HH:mm:ss')) if table == 'transactions' else df
            clean_rows = [r.asDict() for r in serializable(scope[returned]).collect()]
            bad_rows = [r.asDict() for r in serializable(captures[table]['bad']).collect()]
            clean = {r[marker]: r for r in clean_rows}
            bad = {r[marker]: r for r in bad_rows}
            check(table + '.partition', len(clean_rows) + len(bad_rows) == len(cases) and
                  len(clean) == len(clean_rows) and len(bad) == len(bad_rows) and not set(clean).intersection(bad))
            check(table + '.marker_coverage', set(clean) | set(bad) == {c['name'] for c in cases})
            if table in ('clients', 'accounts'):
                raw_ids = ['client_id'] if table == 'clients' else ['client_id', 'account_id']
                check(table + '.raw_ids_not_in_clean', all(k + '_raw' not in scope[returned].columns for k in raw_ids))
            observations = []
            for c in cases:
                name = c['name']
                row = clean.get(name, bad.get(name))
                reasons = set(bad[name]['reject_reason'].split(';')) if name in bad else set()
                check(table + '.' + name + '.decision', (name in clean) == (not c['reasons']))
                check(table + '.' + name + '.reasons', reasons == set(c['reasons']))
                if name in bad and table in ('clients', 'accounts'):
                    for key in raw_ids:
                        check(table + '.' + name + '.' + key + '_raw',
                              key + '_raw' in row and row[key + '_raw'] == c['row'][key])
                for key, value in c['expected'].items():
                    actual = row.get(key) if row else None
                    if key == 'ts' and actual is not None:
                        actual = str(actual)
                    check(table + '.' + name + '.' + key, actual == value)
                observations.append(dict(name=name, input=c['row'], accepted=name in clean, expected_reasons=c['reasons'],
                                         actual_reasons=sorted(reasons), row=row))
            report['tables'][table] = dict(case_count=len(cases), rule_names=captures[table]['rule_names'],
                                           accepted=len(clean), rejected=len(bad), cases=observations)
        report['passed'] = all(c['passed'] for c in report['checks'])
    except Exception as error:
        report['failure_phase'] = phase
        report['failure_type'] = type(error).__name__
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:
                report['stop_failure_type'] = type(error).__name__
        report['mismatches'] = [c['name'] for c in report['checks'] if not c['passed']]
        report['finished_utc'] = datetime.now(timezone.utc).isoformat()
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        print(json.dumps({k: report.get(k) for k in ['passed', 'mode', 'failure_phase', 'failure_type', 'mismatches']}))
    return 1 if 'failure_type' in report or 'stop_failure_type' in report or (not report['passed'] and not observe) else 0


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('notebook', type=Path)
    p.add_argument('output', type=Path)
    p.add_argument('--observe', action='store_true')
    a = p.parse_args()
    sys.exit(run(a.notebook, a.output, a.observe))
