from __future__ import annotations

import argparse
import csv
import hashlib
import json
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from clearfeed_dashboard.config import load_config
from clearfeed_dashboard.db import (
    bootstrap,
    managed_connection,
    record_event,
    upsert_candidate,
    upsert_scraped_post,
)
from clearfeed_dashboard.types import ScrapedPost


TEXT_FIELDS = (
    "text",
    "full_text",
    "tweetText",
    "tweet_text",
    "content",
    "body",
)
AUTHOR_FIELDS = (
    "author_handle",
    "author_username",
    "reply_author_username",
    "username",
    "screen_name",
    "handle",
)
AUTHOR_NAME_FIELDS = ("author_name", "name", "display_name")
URL_FIELDS = ("url", "tweet_url", "tweetUrl", "permalink")
ID_FIELDS = ("tweet_id", "tweetId", "id", "conversation_id")
TIME_FIELDS = (
    "tweetCreatedAt",
    "tweet_created_at",
    "reply_created_at",
    "created_at",
    "createdAt",
    "timestamp",
)
STATUS_FIELDS = ("status", "review_status", "approval_status")
METRIC_FIELDS = (
    "likes",
    "like_count",
    "retweets",
    "retweet_count",
    "replies",
    "reply_count",
    "views",
    "impressions",
)
APPROVED_STATUSES = {"approved", "published", "ready", "reviewed", "selected"}
BLOCKED_STATUSES = {
    "draft",
    "needs_review",
    "not_approved",
    "not_reviewed",
    "pending",
    "rejected",
    "unreviewed",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import reviewed Xquik or TweetClaw exports into the Clearfeed queue."
    )
    parser.add_argument("--input", required=True, help="Path to a CSV, JSON, or JSONL export.")
    parser.add_argument(
        "--include-unreviewed",
        action="store_true",
        help="Import rows without an explicit approved/reviewed status.",
    )
    parser.add_argument("--source-key", default="xquik_export", help="Source key stored in Clearfeed.")
    parser.add_argument("--source-url", default="", help="Optional source URL stored with imported posts.")
    return parser.parse_args()


def normalize_status(value: Any) -> str:
    return str(value or "").strip().lower().replace("-", "_").replace(" ", "_")


def row_is_reviewed(row: dict[str, Any], include_unreviewed: bool) -> bool:
    statuses = [normalize_status(row.get(field)) for field in STATUS_FIELDS]
    statuses = [status for status in statuses if status]
    if not statuses:
        return include_unreviewed
    return any(status in APPROVED_STATUSES for status in statuses) and not any(
        status in BLOCKED_STATUSES for status in statuses
    )


def first_string(row: dict[str, Any], fields: Iterable[str]) -> str:
    for field in fields:
        value = row.get(field)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def nested_author_handle(row: dict[str, Any]) -> str:
    author = row.get("author")
    if isinstance(author, dict):
        return first_string(author, ("username", "screen_name", "handle"))
    return ""


def normalize_handle(value: str) -> str:
    return value.strip().removeprefix("@")


def parse_datetime(value: str) -> datetime | None:
    text = value.strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = f"{text[:-1]}+00:00"
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def parse_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        digits = value.replace(",", "").strip()
        if digits.isdigit():
            return int(digits)
    return None


def extract_metrics(row: dict[str, Any]) -> dict[str, int]:
    metrics: dict[str, int] = {}
    for field in METRIC_FIELDS:
        value = parse_int(row.get(field))
        if value is not None:
            metrics[field] = value
    nested = row.get("metrics")
    if isinstance(nested, dict):
        for key, value in nested.items():
            parsed = parse_int(value)
            if parsed is not None:
                metrics[str(key)] = parsed
    return metrics


def fallback_tweet_id(row: dict[str, Any], text: str, author_handle: str) -> str:
    stable = json.dumps(row, sort_keys=True, default=str)
    digest = hashlib.sha256(
        f"{author_handle}\n{text}\n{stable}".encode("utf-8")
    ).hexdigest()
    return f"xquik-{digest[:16]}"


def build_post(row: dict[str, Any], source_key: str, source_url: str) -> ScrapedPost | None:
    text = first_string(row, TEXT_FIELDS)
    if not text:
        return None
    raw_author_handle = (
        first_string(row, AUTHOR_FIELDS)
        or nested_author_handle(row)
        or "xquik-export"
    )
    author_handle = normalize_handle(raw_author_handle)
    author_name = first_string(row, AUTHOR_NAME_FIELDS) or author_handle
    tweet_id = first_string(row, ID_FIELDS) or fallback_tweet_id(row, text, author_handle)
    url = first_string(row, URL_FIELDS)
    posted_at = parse_datetime(first_string(row, TIME_FIELDS))
    return ScrapedPost(
        tweet_id=tweet_id,
        source_key=source_key,
        source_url=source_url,
        author_handle=author_handle,
        author_name=author_name,
        text=text,
        posted_at=posted_at,
        url=url,
        linked_url=None,
        metrics=extract_metrics(row),
        raw=row,
    )


def flatten_items(value: Any) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        for key in ("items", "tweets", "results", "data"):
            nested = value.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
        return [value]
    return []


def read_json(path: Path) -> list[dict[str, Any]]:
    text = path.read_text(encoding="utf-8")
    if path.suffix.lower() == ".jsonl":
        rows: list[dict[str, Any]] = []
        for line in text.splitlines():
            if line.strip():
                value = json.loads(line)
                if isinstance(value, dict):
                    rows.append(value)
        return rows
    return flatten_items(json.loads(text))


def read_csv(path: Path) -> list[dict[str, Any]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def read_rows(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() in {".json", ".jsonl"}:
        return read_json(path)
    return read_csv(path)


def import_rows(
    path: Path,
    include_unreviewed: bool,
    source_key: str,
    source_url: str,
) -> int:
    config = load_config()
    resolved_source_url = source_url or str(path)
    imported = 0
    with managed_connection(config.database_path) as conn:
        bootstrap(conn)
        for row in read_rows(path):
            if not row_is_reviewed(row, include_unreviewed):
                continue
            post = build_post(row, source_key, resolved_source_url)
            if post is None:
                continue
            upsert_scraped_post(conn, post)
            candidate_id = upsert_candidate(
                conn,
                tweet_id=post.tweet_id,
                source_key=source_key,
                heuristic_score=1.0,
                llm_score=0.0,
                total_score=1.0,
                opportunity_bucket="xquik_export",
                recommended_action="Review imported Xquik/TweetClaw post.",
                why="Imported from a reviewed Xquik/TweetClaw export for manual Clearfeed triage.",
                score_payload={"source": source_key, "import_path": str(path)},
            )
            record_event(
                conn,
                "candidate",
                candidate_id,
                "imported_xquik_export",
                {"tweet_id": post.tweet_id, "source_key": source_key},
            )
            imported += 1
    return imported


def main() -> None:
    args = parse_args()
    count = import_rows(
        Path(args.input),
        args.include_unreviewed,
        args.source_key,
        args.source_url,
    )
    print(f"Imported {count} reviewed Xquik/TweetClaw rows into Clearfeed.")


if __name__ == "__main__":
    main()
