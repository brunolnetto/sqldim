from datetime import datetime
from typing import Literal

from pydantic_settings import BaseSettings


class SqldimConfig(BaseSettings):
    # SCD defaults
    scd_default_type: int = 2
    scd_epoch: datetime = datetime(1970, 1, 1)
    scd_infinity: datetime = datetime(9999, 12, 31)
    checksum_algorithm: Literal["md5", "sha1", "sha256"] = "md5"

    # Load defaults
    default_batch_size: int = 10_000
    sk_lookup_cache: bool = True
    sk_lookup_cache_ttl: int = 3600  # seconds

    # Migration defaults
    allow_destructive: bool = False
    migration_dir: str = "migrations"

    model_config = {"env_prefix": "SQLDIM_"}
