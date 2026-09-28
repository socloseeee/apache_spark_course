import importlib.util
import json
from pathlib import Path
import pytest


@pytest.fixture
def gen(tmp_path):
    root = Path(__file__).resolve().parents[1]
    source = root / 'data-generator/generate.py'
    if not source.exists():
        source = Path('/home/jovyan/data-generator/generate.py')
    spec = importlib.util.spec_from_file_location('generator', source)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.N_CLIENTS, module.N_TRANSACTIONS = 100, 1000
    module.LANDING_DIR = str(tmp_path)
    return module


def test_exact_replay(gen):
    gen.prepare_batch()
    before = {p.name: p.read_bytes() for p in Path(gen.LANDING_DIR).iterdir()}
    gen.prepare_batch()
    assert before == {p.name: p.read_bytes() for p in Path(gen.LANDING_DIR).iterdir()}


def test_new_batch_has_disjoint_event_ids(gen):
    first = {e['tx_id'] for e in gen.transaction_events(100) if e['tx_id']}
    gen.BATCH_ID = 'demo-002'
    second = {e['tx_id'] for e in gen.transaction_events(100) if e['tx_id']}
    assert first.isdisjoint(second)


def test_duplicate_id_always_has_same_payload(gen):
    seen = {}
    repeats = 0
    for e in gen.transaction_events(100):
        assert isinstance(e['amount'], str)
        if e['tx_id'] is None:
            continue
        if e['tx_id'] in seen:
            assert seen[e['tx_id']] == e
            repeats += 1
        seen[e['tx_id']] = e
    assert repeats > 0


def test_existing_batch_rejects_parameter_change(gen):
    gen.prepare_batch()
    old = (Path(gen.LANDING_DIR) / 'clients.csv').read_bytes()
    gen.SEED += 1
    with pytest.raises(ValueError, match='параметрами'):
        gen.prepare_batch()
    assert (Path(gen.LANDING_DIR) / 'clients.csv').read_bytes() == old


def test_manifest_matches_payload(gen):
    import hashlib
    gen.prepare_batch()
    folder = Path(gen.LANDING_DIR)
    manifest = json.loads((folder / 'manifest.json').read_text(encoding='utf-8'))
    for name, digest in manifest['sha256'].items():
        assert hashlib.sha256((folder / name).read_bytes()).hexdigest() == digest
