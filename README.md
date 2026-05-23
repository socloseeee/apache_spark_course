# Apache Spark Course — End-to-End Data Pipeline

Учебный, но приближенный к проду пайплайн обработки данных на **Apache Spark** с медальон-архитектурой (bronze → silver → gold). Весь стек поднимается одной командой через Docker Compose.

Проект сделан в рамках курса «PySpark с нуля».

---

## Стек

| Сервис | Роль | Прод-аналог |
|---|---|---|
| **Apache Spark** (+ Jupyter) | Движок обработки + среда разработки | Databricks / EMR / DataProc |
| **PostgreSQL** | OLTP-источник и витринная БД для BI | Любая реляционная БД |
| **MinIO** | S3-совместимое объектное хранилище (слои данных) | AWS S3 / Yandex Object Storage |
| **Apache Kafka** | Поток событий (стриминг), KRaft-режим | Kafka в проде |

Все образы multi-arch — работают на Apple Silicon (ARM64) и amd64.

---

## Архитектура

![](assets/Architecture.png)

**Медальон-архитектура:**
- 🥉 **Bronze** — сырые данные «как пришли» из источника
- 🥈 **Silver** — очищенные, типизированные, дедуплицированные (правила качества)
- 🥇 **Gold** — бизнес-витрины: агрегаты и метрики для аналитики

---

## Быстрый старт

### Требования
- Docker и Docker Compose ([инструкция по установке](#установка-docker))
- 8 ГБ RAM минимум (16 ГБ комфортно), 15 ГБ свободного диска

### Запуск

```bash
# 1. Клонировать репозиторий
git clone https://github.com/socloseeee/apache_spark_course.git
cd apache_spark_course

# 2. Создать файл с секретами из шаблона
cp .env.example .env

# 3. Поднять стек (первый запуск долгий — качаются образы)
docker compose up -d

# 4. Проверить что все сервисы живы
docker compose ps
```

### Доступ к интерфейсам

| Сервис | URL | Логин / пароль |
|---|---|---|
| JupyterLab | http://localhost:8888/lab?token=spark | токен `spark` |
| MinIO-консоль | http://localhost:9001 | `minioadmin` / `minioadmin` |
| Spark UI | http://localhost:4040 | (когда запущена Spark-сессия) |

---

## Структура проекта

```
apache_spark_course/
├── docker-compose.yml       # описание стека (4 сервиса + Jupyter)
├── .env.example             # шаблон секретов (копируется в .env)
├── .gitignore
├── conf/
│   └── spark-defaults.conf  # настройки Spark: MinIO (s3a), JDBC, Kafka
├── data-generator/
│   └── generate.py          # генератор реалистичных данных
└── notebooks/               # ноутбуки пайплайна (bronze/silver/gold)
```

---

## Подключение к сервисам

Адрес сервиса зависит от того, **откуда** подключаешься:

| Сервис | Из контейнера (Jupyter) | С хоста |
|---|---|---|
| Postgres | `postgres:5432` | `localhost:5432` |
| MinIO | `minio:9000` | `localhost:9000` |
| Kafka | `kafka:19092` | `localhost:9092` |

Правило: **внутри сети Docker — по имени сервиса, с хоста — через localhost**.

---

## Управление стеком

```bash
docker compose up -d        # запустить
docker compose ps           # статус сервисов
docker compose logs -f <s>  # логи сервиса
docker compose stop         # остановить (данные сохраняются)
docker compose down         # удалить контейнеры (данные в volumes целы)
docker compose down -v      # удалить всё, включая данные
```

---

## Установка Docker

- **Windows:** Docker Desktop + WSL2 (включить виртуализацию в BIOS)
- **macOS:** Docker Desktop (выбрать версию под Apple Silicon или Intel)
- **Linux:** `curl -fsSL https://get.docker.com | sudo sh`, затем добавить себя в группу `docker`

---

## Автор

**Bogdan** — Data Engineer
[GitHub](https://github.com/socloseeee) · [Telegram](https://t.me/socloseeee)