from rapidfuzz import fuzz
from .config import get_config_value


THRESHOLD = get_config_value("fuzzy", "score_threshold")
HIGH = get_config_value("fuzzy", "high_confidence")


def fuzzy_title_score(a: str, b: str) -> int:
    return fuzz.ratio(a, b)


def confidence_label(score: int) -> str:
    if score >= HIGH:
        return "High (≥ high_confidence)"
    elif score >= THRESHOLD:
        return "Medium (>= score_threshold)"
    return "Low"