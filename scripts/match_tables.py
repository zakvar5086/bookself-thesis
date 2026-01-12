"""
Universal table matching tool with exact and fuzzy matching support.

Usage:
    # Match using predefined strategy
    python -m scripts.match_tables Books Books1 --database merged --strategy books
    python -m scripts.match_tables Journals OldJournals --database merged --strategy journals

    # Match with custom exact columns
    python -m scripts.match_tables Books Books1 --database db1 --exact-match ISBN Title Publisher

    # Match with fuzzy matching
    python -m scripts.match_tables Books Books1 --database merged --fuzzy-match Title --threshold 85

    # Combine exact and fuzzy
    python -m scripts.match_tables Books Books1 --exact-match ISBN --fuzzy-match Title --threshold 80

Requires config.json with database paths defined.
"""

import sys
import json
import argparse
import pandas as pd
from pathlib import Path

try:
    from rapidfuzz import fuzz

    HAS_FUZZ = True
except ImportError:
    HAS_FUZZ = False

STRATEGIES = {
    "books": {
        "desc": "Match books by ISBN, Title, Publisher",
        "exact": ["ISBN", "Title", "Publisher"],
        "fuzzy": "Title",
        "fuzzy_on": "Publisher",
    },
    "journals": {
        "desc": "Match journals by ISSN and period",
        "exact": ["ISSN", "StartMonth", "EndMonth", "Year"],
        "fuzzy": None,
    },
    "authors": {
        "desc": "Match authors by name",
        "exact": ["FirstName", "LastName"],
        "fuzzy": "FirstName",
    },
}


def load_config():
    with open("config.json") as f:
        return json.load(f)


def get_path(key):
    return Path(load_config()["paths"][key])


def load_csv(path):
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except:
        return None


def normalize(v):
    if pd.isna(v):
        return ""
    return str(v).replace("\r", " ").replace("\n", " ").strip().lower()


def add_norm_cols(df, cols):
    df = df.copy()
    for col in cols:
        if col in df.columns:
            df[f"{col}_norm"] = df[col].apply(normalize)
    return df


def fuzzy_score(a, b):
    if not HAS_FUZZ:
        return 100 if a == b else 0
    return fuzz.ratio(a, b)


def confidence_label(score, thresh=80, high=95):
    if score >= high:
        return "High"
    elif score >= thresh:
        return "Medium"
    return "Low"


def exact_match(df1, df2, cols, label1, label2):
    norm_cols = [f"{c}_norm" for c in cols]
    df1 = add_norm_cols(df1, cols)
    df2 = add_norm_cols(df2, cols)

    matches = pd.merge(
        df1, df2, on=norm_cols, how="inner", suffixes=(f"_{label1}", f"_{label2}")
    )
    matches = matches[(matches[norm_cols] != "").all(axis=1)]
    matches["Confidence"] = f"Exact {'+'.join(cols)}"
    matches["MatchType"] = "Exact"
    return matches


def fuzzy_match(df1, df2, fuzzy_col, match_on, threshold, label1, label2):
    if not HAS_FUZZ:
        print("[WARN] rapidfuzz not installed, using exact match only")

    df1 = add_norm_cols(df1, [fuzzy_col] + ([match_on] if match_on else []))
    df2 = add_norm_cols(df2, [fuzzy_col] + ([match_on] if match_on else []))

    fuzzy_norm = f"{fuzzy_col}_norm"

    if match_on:
        match_norm = f"{match_on}_norm"
        pairs = df1.merge(df2, on=match_norm, suffixes=(f"_{label1}", f"_{label2}"))
    else:
        df1["_k"], df2["_k"] = 1, 1
        pairs = df1.merge(df2, on="_k", suffixes=(f"_{label1}", f"_{label2}"))
        pairs = pairs.drop("_k", axis=1)

    if len(pairs) == 0:
        return pd.DataFrame()

    pairs["similarity_score"] = pairs.apply(
        lambda r: fuzzy_score(r[f"{fuzzy_norm}_{label1}"], r[f"{fuzzy_norm}_{label2}"]),
        axis=1,
    )
    pairs["Confidence"] = pairs["similarity_score"].apply(confidence_label)
    pairs["MatchType"] = "Fuzzy"

    return pairs[pairs["similarity_score"] >= threshold]


def match_with_strategy(table1, table2, database, strategy, out_dir):
    if strategy not in STRATEGIES:
        print(f"[FAIL] Unknown strategy: {strategy}")
        print(f"Available: {', '.join(STRATEGIES.keys())}")
        sys.exit(1)

    strat = STRATEGIES[strategy]
    print("=" * 60)
    print(f"MATCHING: {table1} vs {table2}")
    print(f"Strategy: {strategy} - {strat['desc']}")
    print("=" * 60)

    db_path = get_path(database)
    df1 = load_csv(db_path / f"{table1}.csv")
    df2 = load_csv(db_path / f"{table2}.csv")

    if df1 is None or df2 is None:
        print("[FAIL] Could not load tables")
        return

    print(f"{table1}: {len(df1)} rows")
    print(f"{table2}: {len(df2)} rows\n")

    if strat["exact"]:
        print(f"Exact matching on: {', '.join(strat['exact'])}")
        exact = exact_match(df1, df2, strat["exact"], table1, table2)
        out_file = out_dir / f"{table1}_{table2}_exact.csv"
        exact.to_csv(out_file, index=False)
        print(f"[PASS] {len(exact)} exact matches -> {out_file}")

    if strat["fuzzy"]:
        cfg = load_config()
        thresh = cfg.get("fuzzy", {}).get("score_threshold", 80)
        print(f"\nFuzzy matching on: {strat['fuzzy']} (threshold={thresh})")
        if strat.get("fuzzy_on"):
            print(f"  Requiring match on: {strat['fuzzy_on']}")

        fuzzy = fuzzy_match(
            df1, df2, strat["fuzzy"], strat.get("fuzzy_on"), thresh, table1, table2
        )
        out_file = out_dir / f"{table1}_{table2}_fuzzy.csv"
        fuzzy.to_csv(out_file, index=False)
        print(f"[PASS] {len(fuzzy)} fuzzy matches -> {out_file}")


def match_custom(table1, table2, database, exact_cols, fuzzy_col, threshold, out_dir):
    print("=" * 60)
    print(f"CUSTOM MATCHING: {table1} vs {table2}")
    print("=" * 60)

    db_path = get_path(database)
    df1 = load_csv(db_path / f"{table1}.csv")
    df2 = load_csv(db_path / f"{table2}.csv")

    if df1 is None or df2 is None:
        print("[FAIL] Could not load tables")
        return

    print(f"{table1}: {len(df1)} rows")
    print(f"{table2}: {len(df2)} rows\n")

    if exact_cols:
        missing = [
            c for c in exact_cols if c not in df1.columns or c not in df2.columns
        ]
        if missing:
            print(f"[FAIL] Missing columns: {missing}")
            return

        print(f"Exact matching on: {', '.join(exact_cols)}")
        exact = exact_match(df1, df2, exact_cols, table1, table2)
        out_file = out_dir / f"{table1}_{table2}_exact.csv"
        exact.to_csv(out_file, index=False)
        print(f"[PASS] {len(exact)} exact matches -> {out_file}")

    if fuzzy_col:
        if fuzzy_col not in df1.columns or fuzzy_col not in df2.columns:
            print(f"[FAIL] Fuzzy column '{fuzzy_col}' not found")
            return

        print(f"\nFuzzy matching on: {fuzzy_col} (threshold={threshold})")
        fuzzy = fuzzy_match(df1, df2, fuzzy_col, None, threshold, table1, table2)
        out_file = out_dir / f"{table1}_{table2}_fuzzy.csv"
        fuzzy.to_csv(out_file, index=False)
        print(f"[PASS] {len(fuzzy)} fuzzy matches -> {out_file}")


def main():
    parser = argparse.ArgumentParser(description="Table matching tool")
    parser.add_argument("table1", help="First table name")
    parser.add_argument("table2", help="Second table name")
    parser.add_argument(
        "--database", default="merged", help="Database to use (default: merged)"
    )
    parser.add_argument(
        "--strategy", choices=list(STRATEGIES.keys()), help="Predefined strategy"
    )
    parser.add_argument(
        "--exact-match", nargs="+", metavar="COL", help="Columns for exact match"
    )
    parser.add_argument("--fuzzy-match", metavar="COL", help="Column for fuzzy match")
    parser.add_argument(
        "--threshold",
        type=float,
        default=80,
        help="Fuzzy threshold 0-100 (default: 80)",
    )
    parser.add_argument(
        "--output-dir", help="Output directory (default: matched_results)"
    )
    args = parser.parse_args()

    table1 = args.table1.removesuffix(".csv")
    table2 = args.table2.removesuffix(".csv")

    cfg = load_config()
    out_dir = (
        Path(args.output_dir)
        if args.output_dir
        else Path(cfg["paths"].get("matched_results", "matched_results"))
    )
    out_dir.mkdir(exist_ok=True)
    print(f"Output: {out_dir}\n")

    if args.strategy:
        match_with_strategy(table1, table2, args.database, args.strategy, out_dir)
    elif args.exact_match or args.fuzzy_match:
        match_custom(
            table1,
            table2,
            args.database,
            args.exact_match,
            args.fuzzy_match,
            args.threshold,
            out_dir,
        )
    else:
        print("[FAIL] Specify --strategy or --exact-match/--fuzzy-match")
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
