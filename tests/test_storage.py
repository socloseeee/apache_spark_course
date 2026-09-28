import hashlib
import io
import json
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace
import pytest

root=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(root/'notebooks'))
sys.path.insert(0,str(root))
from course_storage import archive_batch


@pytest.fixture
def storage(tmp_path,monkeypatch):
    class ClientError(Exception):
        response={'Error':{'Code':'NoSuchKey'}}
    objects={}
    class S3:
        def get_object(self,Bucket,Key):
            if Key not in objects:
                raise ClientError()
            return {'Body':io.BytesIO(objects[Key])}
        def put_object(self,Bucket,Key,Body):
            objects[Key]=Body
    monkeypatch.setitem(sys.modules,'boto3',SimpleNamespace(client=lambda *a,**k:S3()))
    monkeypatch.setitem(sys.modules,'botocore',ModuleType('botocore'))
    monkeypatch.setitem(sys.modules,'botocore.exceptions',SimpleNamespace(ClientError=ClientError))
    monkeypatch.setenv('MINIO_ACCESS_KEY','test')
    monkeypatch.setenv('MINIO_SECRET_KEY','test')
    folder=tmp_path/'demo-001';folder.mkdir()
    (folder/'clients.csv').write_bytes(b'id,city\n1,Kazan\n')
    manifest={'batch_id':'demo-001','sha256':{'clients.csv':hashlib.sha256((folder/'clients.csv').read_bytes()).hexdigest()}}
    (folder/'manifest.json').write_text(json.dumps(manifest),encoding='utf-8')
    return tmp_path,objects


def test_archive_replay_preserves_original_bytes(storage):
    landing,objects=storage
    archive_batch(landing,'demo-001')
    original=dict(objects)
    archive_batch(landing,'demo-001')
    assert objects==original
    assert objects['source_files/demo-001/clients.csv']==b'id,city\n1,Kazan\n'


def test_changed_source_fails_before_archive_mutation(storage):
    landing,objects=storage
    archive_batch(landing,'demo-001')
    original=dict(objects)
    (landing/'demo-001/clients.csv').write_bytes(b'changed')
    with pytest.raises(ValueError,match='Изменён'):
        archive_batch(landing,'demo-001')
    assert objects==original
