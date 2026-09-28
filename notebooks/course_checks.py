"""Готовые проверки лабораторной. Студент читает результаты, не пишет фреймворк."""
from functools import reduce
from pyspark.sql import functions as F, Window


def require_empty(df, message):
    if df.limit(1).count():
        raise AssertionError(message)


def unique_keys(df, key):
    require_empty(df.filter(F.col(key).isNull()), f"NULL в ключе {key}")
    require_empty(df.groupBy(key).count().filter("count > 1"), f"Неуникальный ключ {key}")


def checked_dedup(df, key):
    # Можно удалять точные повторы. Разные строки с одним ID — конфликт источника.
    distinct = df.distinct()
    unique_keys(distinct, key)
    return distinct


def quality_split(df, rules):
    reasons = [F.when(~F.coalesce(condition, F.lit(False)), F.lit(name))
               for name, condition in rules.items()]
    tagged = df.withColumn("reject_reason", F.concat_ws(";", *reasons))
    good = tagged.filter("reject_reason = ''").drop("reject_reason")
    bad = tagged.filter("reject_reason <> ''")
    return good, bad


def scd_invariants(df):
    require_empty(df.filter("client_id IS NULL OR valid_from IS NULL"), "NULL в SCD2 ключе/времени")
    current_counts = df.groupBy("client_id").agg(F.sum(F.col("is_current").cast("int")).alias("n"))
    require_empty(current_counts.filter("n <> 1"), "Должна быть одна актуальная версия")
    require_empty(df.filter("is_current <> (valid_to IS NULL) OR is_current IS NULL"), "Флаг current не соответствует интервалу")
    require_empty(df.filter("valid_to <= valid_from"), "Пустой или отрицательный интервал")
    w = Window.partitionBy("client_id").orderBy("valid_from")
    spans = df.withColumn("next_start", F.lead("valid_from").over(w))
    require_empty(spans.filter("next_start IS NOT NULL AND (valid_to IS NULL OR valid_to > next_start)"), "Пересечение интервалов")


def changed_attributes(left="s", right="t"):
    return reduce(lambda a, b: a | b,
                  [~F.col(f"{left}.{c}").eqNullSafe(F.col(f"{right}.{c}"))
                   for c in ("full_name", "city", "segment")])


def apply_scd2(spark, path, updates):
    """Один writer; события по времени; по одному состоянию ключа на batch.

    Повтор старого batch разрешён, если его версия уже записана без изменений.
    Поздние неизвестные изменения отклоняются; автоматическая перестройка истории
    находится за пределами базовой лабораторной.
    """
    from delta.tables import DeltaTable
    cols = ["client_id", "full_name", "city", "segment", "effective_at"]
    updates = updates.select(*cols).distinct()
    unique_keys(updates, "client_id")
    require_empty(updates.filter("effective_at IS NULL"), "Нет effective_at")
    if not DeltaTable.isDeltaTable(spark, path):
        initial = (updates.withColumnRenamed("effective_at", "valid_from")
                   .withColumn("valid_to", F.lit(None).cast("timestamp"))
                   .withColumn("is_current", F.lit(True)))
        initial.write.format("delta").mode("errorifexists").save(path)
        return

    target = DeltaTable.forPath(spark, path)
    history = target.toDF()
    scd_invariants(history)
    # Прежний уже применённый batch распознаём по началу и атрибутам версии.
    replay_condition = ((F.col("s.client_id") == F.col("h.client_id")) &
                        (F.col("s.effective_at") == F.col("h.valid_from")) &
                        ~changed_attributes("s", "h"))
    pending = updates.alias("s").join(history.alias("h"), replay_condition, "left_anti")
    current = history.filter("is_current").alias("t")
    compared = pending.alias("s").join(current, "client_id", "left")
    require_empty(compared.filter(F.col("t.valid_from").isNotNull() &
                                  (F.col("s.effective_at") <= F.col("t.valid_from"))),
                  "Позднее или конфликтующее изменение: требуется отдельная обработка")
    # Две staging-строки для изменённого клиента: одна закрывает, вторая вставляет.
    changed = (compared.filter(F.col("t.valid_from").isNotNull() & changed_attributes())
               .select("s.*"))
    staged = (pending.withColumn("merge_key", F.col("client_id"))
              .unionByName(changed.withColumn("merge_key", F.lit(None).cast("long"))))
    insert = {c: f"s.{c}" for c in ("client_id", "full_name", "city", "segment")}
    insert.update(valid_from="s.effective_at", valid_to="CAST(NULL AS TIMESTAMP)", is_current="true")
    (target.alias("t").merge(staged.alias("s"), "t.client_id = s.merge_key AND t.is_current")
     .whenMatchedUpdate(condition=changed_attributes(), set={"is_current": "false", "valid_to": "s.effective_at"})
     .whenNotMatchedInsert(values=insert).execute())
    scd_invariants(spark.read.format("delta").load(path))
