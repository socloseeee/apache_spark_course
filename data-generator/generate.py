#!/usr/bin/env python3
# ═══════════════════════════════════════════════════════════════════
#  Генератор синтетических финтех-данных для пет-проекта.
#
#  Имитирует ИСТОЧНИКИ (как CDC/файловая выгрузка в реальном банке):
#    • батч: клиенты и счета  -> CSV-файлы в папку landing/
#    • поток: транзакции      -> Kafka topic "transactions"
#
#  Запуск ИЗ КОНТЕЙНЕРА Jupyter (там Python уже есть):
#    !pip install faker kafka-python
#    !python /home/jovyan/data-generator/generate.py
#
#  Внутри контейнера Kafka доступна по адресу kafka:19092.
#  CSV пишутся в /home/jovyan/work/landing/ (видна в проекте как notebooks/landing/).
# ═══════════════════════════════════════════════════════════════════

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


def _rand_date(start_days_ago: int, end_days_ago: int = 0) -> str:
    """Случайная дата в диапазоне [end_days_ago, start_days_ago] назад от сегодня."""
    days = random.randint(end_days_ago, start_days_ago)
    return (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")


def generate_clients_and_accounts():
    """Батч-источник: клиенты и счета -> CSV в landing.
    Намеренно содержит брак (NULL, мусор) и дубли — как реальный источник."""
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
            balance = round(random.uniform(0, 500_000), 2)
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
                    balance = -round(random.uniform(1, 10_000), 2)  # отрицательный -> отсечётся
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


def generate_transactions(n_accounts: int):
    """Стрим-источник: транзакции -> Kafka topic."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("ОШИБКА: не установлен kafka-python. Выполните: !pip install kafka-python")
        return

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        linger_ms=50,
        batch_size=32_768,
    )

    print("Отправка ~{} транзакций (с браком и дублями) в Kafka topic '{}' ({})...".format(
        N_TRANSACTIONS, KAFKA_TOPIC, KAFKA_BOOTSTRAP))

    sent = 0
    for tx_id in range(1, N_TRANSACTIONS + 1):
        amount = round(random.uniform(10, 50_000), 2)
        account_id = random.randint(1, n_accounts)
        tx_id_out = tx_id

        # внедряем брак транзакций
        if random.random() < DIRTY_RATE:
            defect = random.choice(["null_id", "null_acc", "bad_amount"])
            if defect == "null_id":
                tx_id_out = None                # NULL в ключе -> отсечётся
            elif defect == "null_acc":
                account_id = None               # NULL счёта -> отсечётся
            else:
                amount = random.choice([0, -round(random.uniform(1, 5_000), 2)])  # <=0 -> отсечётся

        event = {
            "tx_id": tx_id_out,
            "account_id": account_id,
            "amount": amount,
            "tx_type": random.choice(TX_TYPES),
            "merchant": random.choice(MERCHANTS),
            "status": random.choice(STATUSES),
            "ts": (datetime.now() - timedelta(seconds=random.randint(0, 86_400))).isoformat(),
        }
        producer.send(KAFKA_TOPIC, event)
        sent += 1

        # дубль (повторная доставка события) — тот же tx_id ещё раз
        if tx_id_out is not None and random.random() < DUP_RATE:
            producer.send(KAFKA_TOPIC, event)
            sent += 1

        if sent % 20_000 == 0:
            print("  отправлено {}".format(sent))

    producer.flush()
    producer.close()
    print("Готово: {} сообщений отправлено в Kafka (включая дубли).".format(sent))


def main():
    t0 = time.time()
    print("=== Генерация финтех-данных ===")
    n_accounts = generate_clients_and_accounts()
    generate_transactions(n_accounts)
    print("=== Завершено за {:.1f} сек ===".format(time.time() - t0))


if __name__ == "__main__":
    main()