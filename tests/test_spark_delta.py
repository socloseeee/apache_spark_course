"""Integration: python -m pytest tests/test_spark_delta.py -q (в контейнере Jupyter)."""
import sys
from datetime import datetime, timedelta
from pathlib import Path
import pytest

pytest.importorskip('pyspark')
pytest.importorskip('delta')
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'notebooks'))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from course_checks import apply_scd2, scd_invariants, quality_split, checked_dedup
from pyspark.sql import SparkSession, functions as F


@pytest.fixture(scope='module')
def spark():
    # В лабораторном контейнере JAR/Delta extensions заданы spark-defaults.conf.
    from delta import configure_spark_with_delta_pip
    builder = (SparkSession.builder.master('local[2]').appName('course-tests')
               .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
               .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
               .config('spark.sql.shuffle.partitions', '2'))
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.conf.set('spark.sql.ansi.enabled', 'false')
    session.conf.set('spark.sql.legacy.timeParserPolicy', 'CORRECTED')
    session.conf.set('spark.sql.session.timeZone', 'UTC')
    yield session
    session.stop()


def update(spark, rows):
    return spark.createDataFrame(rows, 'client_id long, full_name string, city string, segment string, effective_at timestamp')


def test_scd2_replay_new_key_and_temporal_join(spark, tmp_path):
    path = str(tmp_path / 'history')
    t1, t2 = datetime(2026,1,1), datetime(2026,1,2,12)
    first = update(spark, [(1,'А','Москва',None,t1)])
    second = update(spark, [(1,'А',None,'private',t2), (2,'Б','Казань','mass',t2)])
    apply_scd2(spark,path,first)
    apply_scd2(spark,path,second)
    expected = spark.read.format('delta').load(path).collect()
    apply_scd2(spark,path,second)
    apply_scd2(spark,path,first)  # Полный Run All: старый уже применённый batch.
    history = spark.read.format('delta').load(path)
    assert set(map(tuple,history.collect())) == set(map(tuple,expected))
    assert len(expected) == 3
    scd_invariants(history)
    facts = spark.createDataFrame([(1,t1),(1,t2)], 'client_id long, ts timestamp').alias('f')
    h = history.alias('h')
    joined = facts.join(h, (F.col('f.client_id') == F.col('h.client_id')) &
                       (F.col('f.ts') >= F.col('h.valid_from')) &
                       (F.col('h.valid_to').isNull() | (F.col('f.ts') < F.col('h.valid_to'))))
    result = joined.orderBy('ts').select('segment').collect()
    assert [r.segment for r in result] == [None,'private']


def test_late_conflict_rejected_without_mutation(spark,tmp_path):
    path = str(tmp_path/'history')
    apply_scd2(spark,path,update(spark,[(1,'А','Москва','mass',datetime(2026,1,2))]))
    before = spark.read.format('delta').load(path).collect()
    with pytest.raises(AssertionError,match='Позднее'):
        apply_scd2(spark,path,update(spark,[(1,'А','Казань','mass',datetime(2026,1,1))]))
    assert spark.read.format('delta').load(path).collect() == before


def test_scd2_same_day_changes_delimiters_nulls_boundaries_and_full_replay(spark, tmp_path):
    path = str(tmp_path / 'history')
    t0, t1, t2 = (datetime(2026, 1, 2, 9), datetime(2026, 1, 2, 12, 30),
                  datetime(2026, 1, 2, 18, 45))
    # Все три состояния дают одинаковый concat_ws('|', ...), но атрибуты разные.
    a0, a1, a2 = ('А|Б', 'В', None), ('А', 'Б|В', None), ('А', None, 'Б|В')
    batches = [update(spark, [(7, *attrs, at)])
               for attrs, at in ((a0, t0), (a1, t1), (a2, t2))]
    for batch in batches:
        apply_scd2(spark, path, batch)

    columns = ['client_id', 'full_name', 'city', 'segment',
               'valid_from', 'valid_to', 'is_current']
    expected = [(7, *a0, t0, t1, False), (7, *a1, t1, t2, False),
                (7, *a2, t2, None, True)]

    def snapshot():
        return [tuple(row) for row in spark.read.format('delta').load(path)
                .select(*columns).orderBy('valid_from').collect()]

    before_replay = snapshot()
    assert before_replay == expected
    assert sum(row[-1] for row in before_replay) == 1
    history = spark.read.format('delta').load(path)
    scd_invariants(history)

    tick = timedelta(microseconds=1)
    points = [(0, t0 - tick), (1, t0), (2, t1 - tick), (3, t1),
              (4, t2 - tick), (5, t2), (6, t2 + tick)]
    facts = spark.createDataFrame([(probe, 7, ts) for probe, ts in points],
                                 'probe int, client_id long, ts timestamp').alias('f')
    h = history.alias('h')
    joined = facts.join(h, (F.col('f.client_id') == F.col('h.client_id')) &
                       (F.col('f.ts') >= F.col('h.valid_from')) &
                       (F.col('h.valid_to').isNull() | (F.col('f.ts') < F.col('h.valid_to'))),
                       'left')
    matches = joined.select('f.probe', 'h.valid_from', 'h.full_name', 'h.city', 'h.segment')
    assert [tuple(row) for row in matches.orderBy('probe').collect()] == [
        (0, None, None, None, None), (1, t0, *a0), (2, t0, *a0),
        (3, t1, *a1), (4, t1, *a1), (5, t2, *a2), (6, t2, *a2)]

    # Повтор полного набора: старые версии уже закрыты, но их batch известны.
    for batch in batches:
        apply_scd2(spark, path, batch)
        assert snapshot() == before_replay


def test_conflicting_source_ids_are_not_arbitrarily_dropped(spark):
    df = spark.createDataFrame([(1,'x'),(1,'y')], 'id long, value string')
    with pytest.raises(AssertionError,match='Неуникальный'):
        checked_dedup(df,'id')
    assert checked_dedup(spark.createDataFrame([(1,'x'),(1,'x')],df.schema),'id').count() == 1


def test_quality_and_decimal(spark):
    df = spark.createDataFrame([(1,'  ','31-02-2026'),(2,'Казань','2026-01-01')], 'id long, city string, day string')
    df = df.withColumn('city',F.trim('city')).withColumn('day',F.to_date('day','yyyy-MM-dd'))
    good,bad = quality_split(df,{'city':F.length('city')>0,'date':F.col('day').isNotNull()})
    assert good.count() == bad.count() == 1
    assert bad.first().reject_reason == 'city;date'
    from decimal import Decimal
    amounts = spark.createDataFrame([('0.10',),('0.20',)],'amount string')
    assert amounts.select(F.col('amount').cast('decimal(18,2)').alias('amount')).agg(F.sum('amount')).first()[0] == Decimal('0.30')
