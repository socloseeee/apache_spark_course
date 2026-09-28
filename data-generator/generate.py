#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════
#  Генератор синтетических финтех-данных для пет-проекта.
#
#  Имитирует ИСТОЧНИКИ (как CDC/файловая выгрузка в реальном банке):
#    • батч: клиенты и счета  -> CSV-файлы в папку landing/
#    • поток: транзакции      -> Kafka topic "transactions"
#
#  Запуск ИЗ КОНТЕЙНЕРА Jupyter (там Python уже есть):
#    !pip install -r /home/jovyan/work/requirements.txt
#    !python /home/jovyan/data-generator/generate.py
#
#  Внутри контейнера Kafka доступна по адресу kafka:19092.
#  CSV пишутся в /home/jovyan/work/landing/ (видна в проекте как notebooks/landing/).
# ═══════════════════════════════════════════════════════════════════

import argparse
import hashlib
from pathlib import Path
import csv
import json
import os
import random
import time
from datetime import datetime, timedelta

# ── Параметры объёма (правьте под свою машину) ──────────────────────
N_CLIENTS = 10_000          # клиентов (батч)
N_ACCOUNTS_MAX = 2          # до 2 счетов на клиента
N_TRANSACTIONS = 200_000    # транзакций в поток Kafka

# ── Доля «грязи» (реалистичный брак из источника) ───────────────────
# Источники почти никогда не дают идеально чистые данные: повторные
# выгрузки порождают дубли, сбои — NULL и битые значения. Silver это отсекает.
DIRTY_RATE = 0.05           # ~5% записей с дефектом
DUP_RATE   = 0.03           # ~3% дублей (повторная выгрузка)

# ── Пути и адреса ───────────────────────────────────────────────────
LANDING_DIR = os.environ.get("LANDING_DIR", "/home/jovyan/work/landing")
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:19092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "transactions")

BATCH_ID = "demo-001"
SEED = 42
AS_OF = datetime.fromisoformat("2026-02-01T00:00:00")

# ── Справочники для реалистичности ──────────────────────────────────
CITIES = ["Москва", "Санкт-Петербург", "Новосибирск", "Екатеринбург",
          "Казань", "Ростов-на-Дону", "Краснодар", "Самара"]
SEGMENTS = ["mass", "mass", "mass", "affluent", "private"]  # перекос в mass
ACCOUNT_TYPES = ["debit", "debit", "credit", "savings"]
TX_TYPES = ["purchase", "purchase", "purchase", "withdrawal", "transfer", "refund"]
MERCHANTS = ["Пятёрочка", "Озон", "Wildberries", "Яндекс", "Лукойл",
             "Магнит", "DNS", "Аптека", "ЖКХ", "Ресторан"]
STATUSES = ["completed", "completed", "completed", "pending", "failed"]

FIRST = ["Иван", "Мария", "Алексей", "Елена", "Дмитрий", "Ольга",
         "Сергей", "Анна", "Андрей", "Татьяна", "Михаил", "Наталья"]
LAST = ["Иванов", "Петров", "Смирнов", "Кузнецов", "Попов", "Соколов",
        "Лебедев", "Новиков", "Морозов", "Волков", "Козлов", "Орлов"]


def money(cents: int) -> str:
    """Точная десятичная строка из целых копеек, без float."""
    sign = "-" if cents < 0 else ""
    units, fraction = divmod(abs(cents), 100)
    return f"{sign}{units}.{fraction:02d}"


def _rand_date(start_days_ago: int, end_days_ago: int = 0) -> str:
    """Случайная дата в диапазоне [end_days_ago, start_days_ago] назад от сегодня."""
    days = random.randint(end_days_ago, start_days_ago)
    return (AS_OF - timedelta(days=days)).strftime("%Y-%m-%d")


def generate_clients_and_accounts():
    """Батч-источник: клиенты и счета -> CSV в landing.
    Намеренно содержит брак (NULL, мусор) и дубли — как реальный источник."""
    random.seed(SEED)  # Один seed сохраняет те же справочники во всех batch.
    os.makedirs(LANDING_DIR, exist_ok=True)

    clients_path = os.path.join(LANDING_DIR, "clients.csv")
    accounts_path = os.path.join(LANDING_DIR, "accounts.csv")

    account_id = 1
    client_rows = []   # копим для возможного дублирования
    account_rows = []

    for client_id in range(1, N_CLIENTS + 1):
        name = "{} {}".format(random.choice(FIRST), random.choice(LAST))
        city = random.choice(CITIES)
        segment = random.choice(SEGMENTS)
        reg_date = _rand_date(2000, 30)

        # внедряем брак клиентов
        if random.random() < DIRTY_RATE:
            defect = random.choice(["null_id", "empty_city", "bad_date"])
            if defect == "null_id":
                client_id_out = ""              # NULL в ключе -> отсечётся
            elif defect == "empty_city":
                client_id_out = client_id
                city = "  "                     # пустой город -> нормализуется/отсечётся
            else:
                client_id_out = client_id
                reg_date = "31-02-2026"          # битая дата -> станет NULL при to_date
        else:
            client_id_out = client_id

        client_rows.append([client_id_out, name, city, segment, reg_date])

        for _ in range(random.randint(1, N_ACCOUNTS_MAX)):
            acc_type = random.choice(ACCOUNT_TYPES)
            balance = money(random.randint(0, 50_000_000))
            open_date = _rand_date(1500, 1)
            status = random.choice(["active", "active", "active", "blocked"])

            # внедряем брак счетов
            if random.random() < DIRTY_RATE:
                defect = random.choice(["null_acc", "neg_balance", "null_client"])
                if defect == "null_acc":
                    acc_id_out = ""             # NULL в ключе -> отсечётся
                    cid_out = client_id
                elif defect == "neg_balance":
                    acc_id_out = account_id
                    cid_out = client_id
                    balance = money(-random.randint(100, 1_000_000))  # отрицательный -> отсечётся
                else:
                    acc_id_out = account_id
                    cid_out = ""                # NULL клиента -> отсечётся
            else:
                acc_id_out = account_id
                cid_out = client_id

            account_rows.append([acc_id_out, cid_out, acc_type, balance, open_date, status])
            account_id += 1

    # дубли (повторная выгрузка из источника)
    n_client_dups = int(len(client_rows) * DUP_RATE)
    n_account_dups = int(len(account_rows) * DUP_RATE)
    client_rows += [list(r) for r in random.sample(client_rows, n_client_dups)]
    account_rows += [list(r) for r in random.sample(account_rows, n_account_dups)]
    random.shuffle(client_rows)
    random.shuffle(account_rows)

    with open(clients_path, "w", newline="", encoding="utf-8") as cf:
        w = csv.writer(cf)
        w.writerow(["client_id", "full_name", "city", "segment", "reg_date"])
        w.writerows(client_rows)
    with open(accounts_path, "w", newline="", encoding="utf-8") as af:
        w = csv.writer(af)
        w.writerow(["account_id", "client_id", "acc_type", "balance", "open_date", "status"])
        w.writerows(account_rows)

    print("Батч записан (с браком и дублями):")
    print("  clients :", clients_path, "({} строк, включая дубли)".format(len(client_rows)))
    print("  accounts:", accounts_path, "({} строк, включая дубли)".format(len(account_rows)))
    return account_id - 1  # сколько уникальных счетов сгенерировано


def transaction_events(n_accounts: int):
    """Replay: тот же batch, seed и as_of дают те же сообщения."""
    rng = random.Random(f"{SEED}:{BATCH_ID}")
    for sequence in range(1, N_TRANSACTIONS + 1):
        tx_id = f"{BATCH_ID}:{sequence}"
        account_id = rng.randint(1, n_accounts)
        cents = rng.randint(1000, 5_000_000)
        if rng.random() < DIRTY_RATE:
            defect = rng.choice(["null_id", "null_acc", "bad_amount"])
            if defect == "null_id":
                tx_id = None
            elif defect == "null_acc":
                account_id = None
            else:
                cents = -rng.randint(0, 500_000)
        event = {
            "tx_id": tx_id, "batch_id": BATCH_ID, "account_id": account_id,
            "amount": money(cents), "currency": "RUB",
            "tx_type": rng.choice(TX_TYPES), "merchant": rng.choice(MERCHANTS),
            "status": rng.choice(STATUSES),
            "ts": (AS_OF - timedelta(seconds=rng.randint(0, 86400))).isoformat(),
        }
        yield event
        if tx_id is not None and rng.random() < DUP_RATE:
            yield dict(event)


def prepare_batch():
    """Имена batch неизменяемы: другой seed/объём требуют другого каталога."""
    manifest = {"batch_id": BATCH_ID, "seed": SEED, "as_of": AS_OF.isoformat(),
                "n_clients": N_CLIENTS, "n_transactions": N_TRANSACTIONS,
                "n_accounts_max": N_ACCOUNTS_MAX,
                "dirty_rate": DIRTY_RATE, "dup_rate": DUP_RATE,
                "generator_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    dest = Path(LANDING_DIR)
    manifest_path = dest / "manifest.json"
    if manifest_path.exists():
        existing = json.loads(manifest_path.read_text(encoding="utf-8"))
        if {k: existing.get(k) for k in manifest} != manifest:
            raise ValueError("Этот batch уже существует с другими параметрами. Используйте новый batch-id.")
    n_accounts = generate_clients_and_accounts()
    events_path = dest / "transactions.jsonl"
    with events_path.open("w", encoding="utf-8", newline="\n") as out:
        for event in transaction_events(n_accounts):
            out.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    manifest["sha256"] = {name: hashlib.sha256((dest / name).read_bytes()).hexdigest()
                          for name in ("clients.csv", "accounts.csv", "transactions.jsonl")}
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return events_path


def publish(events_path):
    from kafka import KafkaProducer
    producer = KafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, acks="all", retries=3)
    pending = []
    try:
        with events_path.open("rb") as source:
            for line in source:
                pending.append(producer.send(KAFKA_TOPIC, value=line.rstrip(b"\n")))
                if len(pending) >= 1000:
                    for future in pending:
                        future.get(timeout=60)
                    pending.clear()
        for future in pending:
            future.get(timeout=60)
        producer.flush(timeout=60)
    finally:
        producer.close(timeout=60)


def main():
    global BATCH_ID, SEED, AS_OF, LANDING_DIR, N_CLIENTS, N_TRANSACTIONS
    parser = argparse.ArgumentParser(description="Воспроизводимый учебный источник")
    parser.add_argument("--batch-id", default="demo-001")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--as-of", default="2026-02-01T00:00:00")
    parser.add_argument("--clients", type=int, default=10_000)
    parser.add_argument("--transactions", type=int, default=200_000)
    parser.add_argument("--no-kafka", action="store_true")
    args = parser.parse_args()
    import re
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", args.batch_id):
        parser.error("batch-id: только латинские буквы, цифры, _ и -")
    if args.clients < 1 or args.transactions < 1:
        parser.error("clients и transactions должны быть положительными")
    BATCH_ID, SEED = args.batch_id, args.seed
    AS_OF = datetime.fromisoformat(args.as_of)
    if AS_OF.tzinfo is not None:
        parser.error("as-of задаётся в UTC без суффикса часового пояса")
    N_CLIENTS, N_TRANSACTIONS = args.clients, args.transactions
    LANDING_DIR = str(Path(LANDING_DIR) / BATCH_ID)
    events_path = prepare_batch()
    if not args.no_kafka:
        publish(events_path)
    print(f"Batch {BATCH_ID} готов: {LANDING_DIR}")


if __name__ == "__main__":
    main()
