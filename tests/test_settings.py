"""
Unit tests for dev/prod environment resolution in config.settings.
"""

from pathlib import Path

import pytest

from config.settings import DEFAULT_ENVIRONMENT, resolve_environment


def test_defaults_to_dev():
    env, root, data_dir = resolve_environment({"DAT535_DATA_DIR": "/data"})
    assert env == DEFAULT_ENVIRONMENT == "dev"
    assert data_dir == Path("/data") / "dev"


def test_prod_gets_its_own_subfolder():
    env, root, data_dir = resolve_environment({"DAT535_ENV": "prod", "DAT535_DATA_DIR": "/data"})
    assert env == "prod"
    assert root == Path("/data")
    assert data_dir == Path("/data") / "prod"


def test_dev_and_prod_never_share_a_data_dir():
    _, _, dev_dir = resolve_environment({"DAT535_ENV": "dev", "DAT535_DATA_DIR": "/data"})
    _, _, prod_dir = resolve_environment({"DAT535_ENV": "prod", "DAT535_DATA_DIR": "/data"})
    assert dev_dir != prod_dir


def test_env_name_is_normalized():
    env, _, data_dir = resolve_environment({"DAT535_ENV": "  PROD ", "DAT535_DATA_DIR": "/data"})
    assert env == "prod"
    assert data_dir == Path("/data") / "prod"


@pytest.mark.parametrize("value", ["production", "main", "staging", ""])
def test_unknown_env_is_rejected(value):
    with pytest.raises(ValueError, match="DAT535_ENV"):
        resolve_environment({"DAT535_ENV": value})


def test_default_root_used_when_data_dir_unset():
    env, root, data_dir = resolve_environment({})
    assert env == "dev"
    assert data_dir == root / "dev"
