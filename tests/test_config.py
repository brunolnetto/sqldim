import pytest
from sqldim.config import SqldimConfig

def test_defaults():
    cfg = SqldimConfig()
    assert cfg.scd_default_type == 2
    assert cfg.checksum_algorithm == "md5"
    assert cfg.default_batch_size == 10_000
    assert cfg.sk_lookup_cache is True
    assert cfg.allow_destructive is False
    assert cfg.migration_dir == "migrations"

def test_env_override(monkeypatch):
    monkeypatch.setenv("SQLDIM_ALLOW_DESTRUCTIVE", "true")
    monkeypatch.setenv("SQLDIM_DEFAULT_BATCH_SIZE", "500")
    cfg = SqldimConfig()
    assert cfg.allow_destructive is True
    assert cfg.default_batch_size == 500

def test_checksum_algorithm_values():
    for algo in ("md5", "sha1", "sha256"):
        cfg = SqldimConfig(checksum_algorithm=algo)
        assert cfg.checksum_algorithm == algo
