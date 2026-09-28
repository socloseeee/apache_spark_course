# Статический аудит связности ученического маршрута

Дата актуализации: **20.09.2026**. Статус найденного дефекта: **FIXED**.

Проверены README, docker-compose.yml, .env.example, исходные markdown/code cells notebooks 00–05 и непосредственно связанные конфигурация Spark, requirements, генератор, archive_batch и инструкция Metabase. Исходники под sources/ не изменялись. Браузер, приватные credentials/checker, сеть и Docker не использовались; ранее выполненные интеграционные прогоны не повторялись.

## Исправленное противоречие

**[P2, FIXED] Notebook 00 после смены batch-id читал manifest прежнего demo-001.**

До исправления инструкция предписывала изменить имя только в аргументе генератора notebook 00 и в BATCH_ID notebooks 01/02. Следующая ячейка notebook 00 читала жёстко заданный `landing/demo-001/manifest.json`. Для первого запуска с другим именем это давало FileNotFoundError, а при существующем demo-001 выводило старый manifest.

После исправления (пути относительно `audit-work/apache_spark_course`):

- `notebooks/00_generate_data.ipynb:55`: одна переменная `BATCH_ID = "demo-001"`; default сохранён.
- `notebooks/00_generate_data.ipynb:57`: значение BATCH_ID передаётся в `--batch-id`.
- `notebooks/00_generate_data.ipynb:70`: manifest читается через `Path("landing") / BATCH_ID / "manifest.json"`.
- `notebooks/00_generate_data.ipynb:12–14` и `README.md:172–173`: для нового batch указано одинаковое значение BATCH_ID в notebooks 00, 01 и 02.

## Проверки 20.09.2026

1. **PASS:** `nbformat.read(..., as_version=4)` и `nbformat.validate(...)` для notebooks 00–05, всего 6 файлов.
2. **PASS:** все 30 code cells преобразованы `IPython.core.inputtransformer2.TransformerManager.transform_cell` и скомпилированы через `compile(..., "exec")`. Магия `%pip` проверялась после преобразования, установка пакетов не выполнялась.
3. **PASS:** AST ячейки генерации содержит ровно одно присваивание BATCH_ID с исходным значением `demo-001`.
4. **PASS:** локальные временные фикстуры выполнили исходные ячейки генерации/чтения manifest с подменой только `subprocess.run`. Подмена проверяла аргументы `--batch-id`, `--seed 42`, `--as-of 2026-02-01T00:00:00` и `check=True`, затем создавала небольшой JSON manifest в соответствующей папке. Проверены три сценария: `demo-001` на чистом пути; `demo-002` без папки demo-001; `demo-002` при существующем старом manifest demo-001. Во всех случаях ячейка прочла manifest выбранного batch, а не старый файл. Временные папки удалены стандартным TemporaryDirectory.

Генератор, Kafka и полный pipeline в этих проверках не запускались; данные не публиковались. Из рабочих файлов этой правкой затронуты только README и notebook 00; дополнительно обновлён этот отчёт. Stage, commit и упаковка не выполнялись.

Других конкретных release-blocking противоречий в проверенных путях, названиях витрин, версиях, передаче учебных credentials и ресурсных инструкциях не обнаружено. Это вывод статического аудита; он не расширяет доказательства прежних runtime-прогонов.
