import json
from pathlib import Path

_config_cache = None


def load_config() -> dict:
    global _config_cache
    if _config_cache is not None:
        return _config_cache

    cfg_path = Path("config.json")
    if not cfg_path.exists():
        raise FileNotFoundError("config.json not found in project root.")

    with cfg_path.open("r", encoding="utf-8") as f:
        _config_cache = json.load(f)

    return _config_cache


def get_path(key: str) -> Path:
    """
    Return a Path object for a named directory key from config.json.
    """
    cfg = load_config()
    p = cfg["paths"][key]
    return Path(p)


def get_config_value(*keys):
    """
    Access arbitrary nested config:
    get_config_value("fuzzy", "score_threshold")
    """
    cfg = load_config()
    value = cfg
    for k in keys:
        value = value[k]
    return value