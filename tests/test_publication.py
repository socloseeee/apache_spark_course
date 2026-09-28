"""Проверяем порядок команд реальной функции notebook без подключения к ClickHouse."""
import ast
from decimal import Decimal
import json
from pathlib import Path
import re
import uuid
from types import SimpleNamespace
import pytest
import requests


def loader(responder):
    root = Path(__file__).resolve().parents[1]
    path = root / 'notebooks/05_clickhouse.ipynb'
    if not path.exists():
        path = root / '05_clickhouse.ipynb'
    nb = json.loads(path.read_text(encoding='utf-8'))
    tree = ast.parse(next(''.join(c['source']) for c in nb['cells'] if 'def load_mart' in ''.join(c['source'])))
    functions = ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef)],type_ignores=[])
    scope = dict(re=re,uuid=uuid,Decimal=Decimal,ch=responder,MINIO='http://minio:9000',S3KEY='test',S3SECRET='test',DB='gold')
    exec(compile(functions,'publication-functions','exec'),scope)
    return scope['load_mart']


def test_failed_load_does_not_drop_or_exchange_published_table():
    commands=[]
    def ch(sql):
        commands.append(sql)
        if sql.startswith('CREATE TABLE'):
            raise RuntimeError('simulated load failure')
        return '5\t50.50\t10'
    with pytest.raises(RuntimeError):
        loader(ch)('volume_by_segment','tuple()',[])
    assert not any(s.startswith(('DROP','EXCHANGE','RENAME')) for s in commands)


def test_validated_load_precedes_exchange():
    commands=[]
    def ch(sql):
        commands.append(sql)
        return '1' if sql.startswith('EXISTS') else '5\t50.50\t10'
    loader(ch)('volume_by_segment','tuple()',[],with_tx_count=True)
    create=next(i for i,s in enumerate(commands) if s.startswith('CREATE TABLE'))
    check=next(i for i,s in enumerate(commands) if s.startswith('SELECT count(),') and ' FROM gold.' in s)
    exchange=next(i for i,s in enumerate(commands) if s.startswith('EXCHANGE'))
    assert create < check < exchange
    assert not any(s.startswith('DROP') for s in commands)


def test_null_key_blocks_load():
    commands=[]
    def ch(sql):
        commands.append(sql)
        return '1'
    with pytest.raises(AssertionError,match='NULL'):
        loader(ch)('top_clients','(city, rank_in_city)',['city','rank_in_city'])
    assert not any(s.startswith('CREATE TABLE') for s in commands)


@pytest.mark.parametrize('actual', [
    '4\t9007199254740993.01\t10',
    '5\t9007199254740993.02\t10',
    '5\t9007199254740993.01\t9',
])
def test_mismatched_count_amount_or_operations_blocks_publication(actual):
    commands=[]
    def ch(sql):
        commands.append(sql)
        if sql.startswith('SELECT count(),'):
            return actual if ' FROM gold.' in sql else '5\t9007199254740993.01\t10'
        return ''
    with pytest.raises(AssertionError,match='Staging не совпадает'):
        loader(ch)('volume_by_segment','tuple()',[],with_tx_count=True)
    assert not any(s.startswith(('EXCHANGE','RENAME')) for s in commands)


def test_decimal_equal_values_and_first_publication():
    commands=[]
    def ch(sql):
        commands.append(sql)
        if sql.startswith('SELECT count(),'):
            return '5\t9007199254740993.010\t10' if ' FROM gold.' in sql else '5\t9007199254740993.01\t10'
        return '0' if sql.startswith('EXISTS') else ''
    loader(ch)('volume_by_segment','tuple()',[],with_tx_count=True)
    assert any(s.startswith('RENAME') for s in commands)
    assert not any(s.startswith('EXCHANGE') for s in commands)


def test_top_clients_checks_own_amount_without_tx_count():
    commands=[]
    def ch(sql):
        commands.append(sql)
        if sql.startswith('SELECT count(),'):
            return '2\t50.50'
        if sql.startswith('SELECT count() FROM'):
            return '0'
        return '1' if sql.startswith('EXISTS') else ''
    loader(ch)('top_clients','(city, rank_in_city)',['city','rank_in_city'])
    summary_queries=[s for s in commands if s.startswith('SELECT count(),')]
    assert len(summary_queries) == 2
    assert '/top_clients/' in summary_queries[0]
    assert all('tx_count' not in s for s in summary_queries)
    assert any(s.startswith('EXCHANGE') for s in commands)


def http_helper(post):
    root = Path(__file__).resolve().parents[1]
    path = root / 'notebooks/05_clickhouse.ipynb'
    if not path.exists():
        path = root / '05_clickhouse.ipynb'
    nb = json.loads(path.read_text(encoding='utf-8'))
    tree = ast.parse(next(''.join(c['source']) for c in nb['cells'] if 'def ch(sql)' in ''.join(c['source'])))
    functions = ast.Module(body=[n for n in tree.body if isinstance(n, ast.FunctionDef)],type_ignores=[])
    transport=SimpleNamespace(post=post,RequestException=requests.RequestException)
    scope=dict(requests=transport,re=re,CH_URL='http://clickhouse:8123/',AUTH=('test','test'))
    exec(compile(functions,'http-functions','exec'),scope)
    return scope['ch']


@pytest.mark.parametrize('status,headers,body', [
    (500,{},'server error containing dummy-secret'),
    (200,{'X-ClickHouse-Exception-Code':'62'},'dummy-secret'),
    (200,{},'1\nCode: 395. DB::Exception: dummy-secret'),
])
def test_http_errors_are_rejected_without_echoing_sql_or_response(status,headers,body):
    def post(*args,**kwargs):
        return SimpleNamespace(ok=status < 400,status_code=status,headers=headers,text=body)
    with pytest.raises(RuntimeError) as error:
        http_helper(post)("SELECT 'dummy-secret'")
    assert 'dummy-secret' not in str(error.value)


def test_http_success_requests_buffering_and_accepts_zero_exception_code():
    calls=[]
    def post(*args,**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(ok=True,status_code=200,headers={'X-ClickHouse-Exception-Code':'0'},text='1\n')
    assert http_helper(post)('SELECT 1') == '1\n'
    assert calls[0]['params']['wait_end_of_query'] == '1'
    assert calls[0]['params']['send_progress_in_http_headers'] == '0'
    assert calls[0]['auth'] == ('test','test')


def test_http_timeout_does_not_retry_or_echo_transport_details():
    calls=[]
    def post(*args,**kwargs):
        calls.append(kwargs)
        raise requests.Timeout('dummy-secret')
    with pytest.raises(RuntimeError,match='не повторяйте EXCHANGE') as error:
        http_helper(post)('EXCHANGE TABLES gold.a AND gold.b')
    assert len(calls) == 1
    assert 'dummy-secret' not in str(error.value)
    assert error.value.__suppress_context__
