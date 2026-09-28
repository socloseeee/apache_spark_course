"""Author-only real-Spark checks of notebook 02's money expressions.

Usage: python run_silver_money.py NOTEBOOK_PATH OUTPUT_JSON [--observe]
The notebook's assignments and quality rules run unchanged. Only fixture reads,
storage writes, and display statements are replaced/omitted; no Delta/S3 writes
occur. --observe records contract mismatches without failing (for a baseline).
Runtime/extraction errors still fail in either mode. No environment is read here.
"""
import argparse
import ast
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import sys


# Twenty scenario groups; variants exercise both sides of each boundary.
# Expectations are fixture data, independent of MONEY_PATTERN and Spark casts.
SCENARIOS = [
    ('minimum_cent', True, [('0.01', '0.01')]),
    ('exact_tenths', True, [('0.10', '0.10'), ('0.20', '0.20')]),
    ('integer', True, [('1', '1.00')]),
    ('one_fraction_digit', True, [('1.2', '1.20')]),
    ('surrounding_ascii_spaces', True, [('  12.34  ', '12.34')]),
    ('zero', True, [('0', '0.00'), ('0.00', '0.00'), ('-0.00', '0.00')]),
    ('negative', True, [('-1', '-1.00')]),
    ('maximum', True, [('9999999999999999.99', '9999999999999999.99')]),
    ('overflow', True, [('10000000000000000.00', None)]),
    ('rounding_overflow', False, [('9999999999999999.995', None)]),
    ('subcent', False, [('0.004', None), ('-0.004', None)]),
    ('rounding_half', False, [('1.005', None)]),
    ('excess_trailing_zeros', False, [('1.2300', None)]),
    ('scientific', False, [('1e2', None), ('1e-3', None)]),
    ('nonfinite', False, [('NaN', None), ('Infinity', None), ('-Infinity', None)]),
    ('missing', False, [(None, None), ('', None), ('   ', None)]),
    ('non_ascii', False, [('１２.３４', None), ('١٢.٣٤', None), ('\u00a012.34\u00a0', None)]),
    ('plus_sign', False, [('+1.00', None)]),
    ('comma', False, [('1,23', None)]),
    ('invalid_decimal_shape', False, [('.50', None), ('1.', None), ('1 2.34', None),
                                      ('1.00\t', None), ('1.00\n', None)]),
]

# Declared fixture contract, not inferred from the result under test.
EXPECTED_CLEAN_SCHEMAS = {
    'balance': 'struct<client_id:bigint,account_id:bigint,balance:decimal(18,2),open_date:date,status:string,acc_type:string,client_exists:boolean>',
    'amount': 'struct<account_id:bigint,tx_id:string,batch_id:string,amount:decimal(18,2),currency:string,tx_type:string,merchant:string,status:string,ts:timestamp,account_exists:boolean>',
}


def fixtures(field):
    cases = []
    for scenario, format_valid, variants in SCENARIOS:
        for raw, normalized in variants:
            value = Decimal(normalized) if normalized is not None else None
            accepted = (format_valid and value is not None and
                        (value >= 0 if field == 'balance' else value > 0))
            # Invalid syntax must have a format reason. A numerical reason may
            # also occur, depending on Spark's cast, which this test observes.
            required = [] if accepted else [field if format_valid else field + '_format']
            allowed = [] if accepted else ([field] if format_valid else [field, field + '_format'])
            cases.append({'id': len(cases) + 1, 'scenario': scenario, 'raw': raw,
                          'expected_accept': accepted, 'expected_value': value if accepted else None,
                          'required_reasons': required, 'allowed_reasons': allowed})
    return cases


def source_cell(notebook, index):
    cell = notebook['cells'][index]
    if cell['cell_type'] != 'code':
        raise AssertionError(f'Expected a code cell at index {index}')
    return ast.parse(''.join(cell['source']))


def display_or_write(node):
    if not isinstance(node, ast.Expr) or not isinstance(node.value, ast.Call):
        return False
    function = node.value.func
    if isinstance(function, ast.Name) and function.id == 'print':
        return True
    if isinstance(function, ast.Attribute) and function.attr in {'show', 'printSchema'}:
        return True
    return (isinstance(function, ast.Attribute) and function.attr == 'save' and
            any(isinstance(part, ast.Attribute) and part.attr == 'write'
                for part in ast.walk(node.value)))


def extract(notebook, path):
    setup = source_cell(notebook, 2)
    constants = [node for node in setup.body if isinstance(node, ast.Assign) and
                 any(isinstance(target, ast.Name) and target.id == 'MONEY_PATTERN'
                     for target in node.targets)]
    if len(constants) > 1:
        raise AssertionError('More than one MONEY_PATTERN assignment')
    functions = [node for node in setup.body
                 if isinstance(node, ast.FunctionDef) and node.name == 'save_clean']
    if len(functions) != 1:
        raise AssertionError('Expected exactly one save_clean function in cell 2')
    save = functions[0]
    removed = []
    kept = []
    capture = ast.parse('_capture_clean(name, typed, rules, key, good, bad, clean)').body[0]
    for node in save.body:
        if display_or_write(node):
            removed.append({'cell': 2, 'statement': ast.unparse(node)})
        elif isinstance(node, ast.Return):
            kept.extend([capture, node])
        else:
            kept.append(node)
    if sum(isinstance(node, ast.Return) for node in save.body) != 1:
        raise AssertionError('Expected one top-level return in save_clean')
    save.body = kept
    setup_module = ast.fix_missing_locations(ast.Module(body=constants + [save], type_ignores=[]))
    modules = {}
    wanted = {6: {'accounts', 'accounts_clean'},
              8: {'schema', 'raw', 'parsed', 'malformed', 'tx', 'tx_clean'}}
    for index, names in wanted.items():
        assignments = []
        for node in source_cell(notebook, index).body:
            if isinstance(node, ast.Assign):
                if not all(isinstance(target, ast.Name) and target.id in names for target in node.targets):
                    raise AssertionError(f'Unexpected assignment in cell {index}')
                if any(target.id == 'raw' for target in node.targets):
                    if ast.unparse(node.value.func) != 'spark.read.parquet':
                        raise AssertionError('Expected the known Parquet read boundary')
                    node.value = ast.Name(id='_transactions_fixture', ctx=ast.Load())
                assignments.append(node)
            elif display_or_write(node):
                removed.append({'cell': index, 'statement': ast.unparse(node)})
            else:
                raise AssertionError(f'Unexpected top-level statement in cell {index}')
        modules[index] = ast.fix_missing_locations(ast.Module(body=assignments, type_ignores=[]))
    fingerprints = {'setup': hashlib.sha256(ast.dump(setup_module).encode()).hexdigest()}
    fingerprints.update({f'cell_{index}': hashlib.sha256(ast.dump(module).encode()).hexdigest()
                         for index, module in modules.items()})
    return (compile(setup_module, str(path) + ':cell2', 'exec'),
            {index: compile(module, str(path) + f':cell{index}', 'exec')
             for index, module in modules.items()},
            {'money_pattern_present': bool(constants), 'executed_ast_sha256': fingerprints,
             'omitted_statements': removed})


def run(notebook_path, output_path, observe=False):
    report = {
        'started_utc': datetime.now(timezone.utc).isoformat(), 'mode': 'observe' if observe else 'assert',
        'notebook': str(notebook_path), 'passed': False, 'checks': [], 'fields': {},
        'runner_sha256': hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        'scope': {
            'engine': 'Real Spark expressions, JSON parsing, joins, casts, quality_split and checked_dedup',
            'executed_source': 'Assignments in notebook cells 6 and 8; save_clean and optional MONEY_PATTERN from cell 2',
            'boundaries': [
                'read_csv returns an explicit-schema fixture DataFrame',
                'spark.read.parquet returns a fixture DataFrame containing UTF-8 JSON bytes in value',
                'save_clean is extracted from the notebook; only storage writes and print/show are omitted',
                'A read-only capture is inserted before save_clean returns',
                'BATCH_ID is set to demo-001; notebook setup I/O functions and its BATCH_ID assignment are not run',
                'Malformed JSON assignment runs, but its write/print and transaction printSchema do not',
            ],
            'not_covered': 'CSV parser, Kafka, S3/MinIO, Delta publication and malformed JSON quarantine persistence',
        },
    }
    spark = None
    phase = 'extract_notebook'

    def check(name, condition):
        report['checks'].append({'name': name, 'passed': bool(condition)})

    try:
        data = notebook_path.read_bytes()
        report['notebook_sha256'] = hashlib.sha256(data).hexdigest()
        setup_code, cells, extraction = extract(json.loads(data), notebook_path)
        report['extraction'] = extraction
        phase = 'start_spark'
        from pyspark.sql import SparkSession, functions as F
        sys.path.insert(0, str(notebook_path.parent.resolve()))
        from course_checks import quality_split, checked_dedup
        spark = (SparkSession.builder.master('local[2]').appName('author-silver-money')
                 .config('spark.sql.shuffle.partitions', '2').getOrCreate())
        spark.conf.set('spark.sql.ansi.enabled', 'false')
        spark.conf.set('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
        spark.conf.set('spark.sql.session.timeZone', 'UTC')
        report['spark_version'] = spark.version
        check('expected_spark_version', spark.version == '3.5.3')
        captures = {}

        def capture(name, typed, rules, key, good, bad, clean):
            captures[name] = {'typed': typed, 'good': good, 'bad': bad, 'clean': clean,
                              'key': key, 'rule_names': list(rules)}

        scope = {'spark': spark, 'F': F, 'BATCH_ID': 'demo-001',
                 'quality_split': quality_split, 'checked_dedup': checked_dedup,
                 '_capture_clean': capture}
        exec(setup_code, scope)
        report['money_pattern'] = scope.get('MONEY_PATTERN')

        def execute(field, rows):
            """Rows are (business ID, source money string), with valid other fields."""
            captures.clear()
            if field == 'balance':
                fixture = spark.createDataFrame(
                    [(str(key), '1', raw, '2026-01-01', 'active', 'debit') for key, raw in rows],
                    'account_id string, client_id string, balance string, open_date string, status string, acc_type string')

                def read_csv(name):
                    if name != 'accounts':
                        raise AssertionError('Unexpected fixture read')
                    return fixture

                scope.update(read_csv=read_csv, clients_clean=spark.createDataFrame([(1,)], 'client_id long'))
                exec(cells[6], scope)
                captures['accounts']['clean'] = scope['accounts_clean']
                return captures['accounts']
            values = []
            for key, raw in rows:
                payload = dict(tx_id=str(key), batch_id='demo-001', account_id=1, amount=raw,
                               currency='RUB', tx_type='purchase', merchant='fixture',
                               status='completed', ts='2026-01-01T00:00:00Z')
                values.append((json.dumps(payload, ensure_ascii=False).encode('utf-8'),))
            scope.update(_transactions_fixture=spark.createDataFrame(values, 'value binary'),
                         accounts_clean=spark.createDataFrame([(1,)], 'account_id long'))
            exec(cells[8], scope)
            captures['transactions']['clean'] = scope['tx_clean']
            return captures['transactions']

        for field in ('balance', 'amount'):
            phase = field + '_matrix'
            cases = fixtures(field)
            captured = execute(field, [(case['id'], case['raw']) for case in cases])
            key = captured['key']
            raw_name = field + '_raw'
            clean_rows = [row.asDict() for row in captured['clean'].collect()]
            bad_rows = [row.asDict() for row in captured['bad'].collect()]
            accepted = {str(row[key]): row for row in clean_rows}
            rejected = {str(row[key]): row for row in bad_rows}
            field_report = {'scenario_groups': len(SCENARIOS), 'case_count': len(cases),
                            'accepted_count': len(clean_rows), 'rejected_count': len(bad_rows),
                            'rule_names': captured['rule_names'],
                            'clean_schema': captured['clean'].schema.simpleString(),
                            'quarantine_schema': captured['bad'].schema.simpleString(), 'cases': []}
            report['fields'][field] = field_report
            check(field + '.full_clean_schema', captured['clean'].schema.simpleString() == EXPECTED_CLEAN_SCHEMAS[field])
            check(field + '.decimal_type', captured['clean'].schema[field].dataType.simpleString() == 'decimal(18,2)')
            check(field + '.raw_not_in_clean', raw_name not in captured['clean'].columns)
            check(field + '.raw_in_quarantine', raw_name in captured['bad'].columns)
            check(field + '.partition_count', len(clean_rows) + len(bad_rows) == len(cases))
            check(field + '.unique_partition', not (set(accepted) & set(rejected)) and
                  len(accepted) + len(rejected) == len(cases))
            for case in cases:
                identity = str(case['id'])
                actual = accepted.get(identity, rejected.get(identity))
                actual_accept = identity in accepted
                reasons = [] if actual_accept or actual is None else actual['reject_reason'].split(';')
                prefix = f"{field}.{case['scenario']}.{case['id']}"
                check(prefix + '.decision', actual is not None and actual_accept == case['expected_accept'])
                if case['expected_accept']:
                    check(prefix + '.exact_decimal', actual_accept and isinstance(actual[field], Decimal)
                          and actual[field] == case['expected_value'] and actual[field].as_tuple().exponent == -2)
                else:
                    check(prefix + '.reasons', not actual_accept and
                          set(case['required_reasons']).issubset(reasons) and
                          set(reasons).issubset(case['allowed_reasons']))
                    check(prefix + '.raw_preserved', identity in rejected and raw_name in actual and
                          actual[raw_name] == case['raw'])
                field_report['cases'].append({**case, 'actual_accept': actual_accept,
                                              'actual_value': actual.get(field) if actual else None,
                                              'actual_reasons': reasons,
                                              'raw_column_present': actual is not None and raw_name in actual,
                                              'actual_raw': actual.get(raw_name) if actual else None})
            tenth_ids = [str(case['id']) for case in cases if case['scenario'] == 'exact_tenths']
            total = (captured['clean'].filter(F.col(key).cast('string').isin(tenth_ids))
                     .agg(F.sum(field)).first()[0])
            field_report['exact_tenths_sum'] = total
            check(field + '.exact_tenths_sum', isinstance(total, Decimal) and total == Decimal('0.30'))

            for label, raw_values in [('exact_repeat', ['1.00', '1.00']),
                                      ('equivalent_notation_repeat', ['1', '1.00'])]:
                phase = field + '_' + label
                repeated = execute(field, [(1, value) for value in raw_values])
                actual_rows = repeated['clean'].collect()
                passed = len(actual_rows) == 1 and actual_rows[0][field] == Decimal('1.00')
                passed = passed and repeated['bad'].count() == 0
                check(field + '.' + label, passed)
                field_report[label] = {'input': raw_values, 'clean_count': len(actual_rows), 'passed': passed}

            phase = field + '_conflicting_accepted_values'
            conflict_rejected = False
            try:
                execute(field, [(1, '1.00'), (1, '2.00')])
            except AssertionError as error:
                # Match the helper's key-conflict assertion; unrelated assertions
                # must not masquerade as successful conflict detection.
                conflict_rejected = str(error) == f'Неуникальный ключ {key}'
                if not conflict_rejected:
                    raise
            check(field + '.conflicting_accepted_values', conflict_rejected)
            field_report['conflicting_accepted_values'] = {
                'input': ['1.00', '2.00'], 'same_business_id': True,
                'assertion_rejected': conflict_rejected}

        report['passed'] = all(item['passed'] for item in report['checks'])
    except Exception as error:
        report['failure_phase'] = phase
        report['failure_type'] = type(error).__name__
        # Spark errors may include host configuration; synthetic fixture results
        # and the exception class are sufficient for this evidence file.
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception as error:
                report['stop_failure_type'] = type(error).__name__
        report['finished_utc'] = datetime.now(timezone.utc).isoformat()
        report['mismatches'] = [item['name'] for item in report['checks'] if not item['passed']]
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, default=str), encoding='utf-8')
        print(json.dumps({'passed': report['passed'], 'mode': report['mode'],
                          'checks_total': len(report['checks']), 'mismatch_count': len(report['mismatches']),
                          'failure_phase': report.get('failure_phase'),
                          'failure_type': report.get('failure_type'), 'output': str(output_path)}, ensure_ascii=False))
    failed_runtime = 'failure_type' in report or 'stop_failure_type' in report
    return 1 if failed_runtime or (not report['passed'] and not observe) else 0


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('notebook_path', type=Path)
    parser.add_argument('output_json', type=Path)
    parser.add_argument('--observe', action='store_true', help='Record contract mismatches without failing the baseline run')
    args = parser.parse_args()
    sys.exit(run(args.notebook_path, args.output_json, args.observe))
