"""Сохранение исходных файлов и чтение снимка для учебного batch."""
import hashlib
import json
import os
from pathlib import Path


def archive_batch(landing, batch_id):
    import boto3
    from botocore.exceptions import ClientError
    s3 = boto3.client("s3", endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
                      aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
                      aws_secret_access_key=os.environ["MINIO_SECRET_KEY"])
    folder = Path(landing) / batch_id
    manifest = json.loads((folder / "manifest.json").read_text(encoding="utf-8"))
    if manifest["batch_id"] != batch_id:
        raise ValueError("Неверный batch в manifest")
    for name, expected in manifest["sha256"].items():
        if name not in {"clients.csv", "accounts.csv", "transactions.jsonl"}:
            raise ValueError("Неизвестное имя файла в manifest")
        if hashlib.sha256((folder / name).read_bytes()).hexdigest() != expected:
            raise ValueError(f"Изменён исходный файл {name}")
    for name in [*manifest["sha256"], "manifest.json"]:
        body = (folder / name).read_bytes()
        key = f"source_files/{batch_id}/{name}"
        try:
            existing = s3.get_object(Bucket="bronze", Key=key)["Body"].read()
        except ClientError as error:
            if error.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
                raise
        else:
            if existing != body:
                raise ValueError(f"Нельзя менять уже архивированный batch: {name}")
            continue
        s3.put_object(Bucket="bronze", Key=key, Body=body)
    return manifest
