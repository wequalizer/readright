"""Twitter/X data archive parser — tweets.js and related files."""

from __future__ import annotations

import json
import re
from datetime import datetime

from envelope.envelope import ContextEnvelope, FieldAnnotation, SchemaAnnotation
from envelope.parser import BaseParser, ParseResult
from envelope.registry import registry

# Twitter archive wraps JSON in a JS assignment:
# window.YTD.tweets.part0 = [...]
# window.YTD.like.part0 = [...]
_JS_WRAPPER_RE = re.compile(r"^window\.YTD\.\w+\.part\d+\s*=\s*", re.MULTILINE)

# Twitter's created_at format: "Wed Jan 01 12:00:00 +0000 2024"
_TWITTER_DATE_FORMAT = "%a %b %d %H:%M:%S %z %Y"


def _strip_js_wrapper(content: str) -> str:
    """Remove the window.YTD.xxx.partN = prefix."""
    return _JS_WRAPPER_RE.sub("", content, count=1).strip()


def _parse_twitter_datetime(dt_str: str) -> str:
    if not dt_str:
        return ""
    try:
        return datetime.strptime(dt_str, _TWITTER_DATE_FORMAT).isoformat()
    except ValueError:
        pass
    # ISO fallback
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).isoformat()
    except ValueError:
        return dt_str


def _extract_urls(entities: dict) -> list[str]:
    urls = []
    for u in entities.get("urls", []):
        expanded = u.get("expanded_url") or u.get("url", "")
        if expanded:
            urls.append(expanded)
    return urls


def _extract_media(entities: dict, extended: dict) -> tuple[bool, str]:
    """Return (has_media, media_type)."""
    # Check extended_entities first (more complete)
    media_list = extended.get("media", []) or entities.get("media", [])
    if not media_list:
        return False, ""
    media_type = media_list[0].get("type", "photo") if media_list else ""
    return True, media_type


class TwitterArchiveParser(BaseParser):
    def source_type(self) -> str:
        return "twitter_archive_js"

    def source_label(self) -> str:
        return "Twitter/X Data Archive (tweets.js)"

    def detect(self, content: bytes, filename: str) -> float:
        fname = filename.lower()

        # Primary: tweets.js
        if fname == "tweets.js":
            return 0.98

        # Other Twitter archive files
        _KNOWN_TWITTER_FILES = {
            "tweet.js", "like.js", "following.js", "follower.js",
            "direct-message.js", "profile.js", "account.js",
        }
        if fname in _KNOWN_TWITTER_FILES:
            return 0.90

        if not fname.endswith(".js"):
            return 0.0

        # Check for JS wrapper pattern
        try:
            text = content[:512].decode("utf-8", errors="ignore")
        except Exception:
            return 0.0

        if _JS_WRAPPER_RE.match(text.strip()):
            if "YTD" in text:
                return 0.85
            return 0.50

        return 0.0

    def schema(self) -> SchemaAnnotation:
        return SchemaAnnotation(
            source_type=self.source_type(),
            source_label=self.source_label(),
            fields=[
                FieldAnnotation(name="id", dtype="string", description="Tweet ID (snowflake string)"),
                FieldAnnotation(name="timestamp", dtype="datetime", description="Tweet creation time", format="ISO 8601"),
                FieldAnnotation(name="text", dtype="string", description="Full tweet text"),
                FieldAnnotation(name="is_retweet", dtype="boolean", description="True if this is a retweet (RT @...)"),
                FieldAnnotation(name="is_reply", dtype="boolean", description="True if this is a reply to another tweet"),
                FieldAnnotation(name="reply_to_user", dtype="string", description="Username of the user being replied to", nullable=True),
                FieldAnnotation(name="reply_to_tweet_id", dtype="string", description="ID of the tweet being replied to", nullable=True),
                FieldAnnotation(name="is_quote", dtype="boolean", description="True if this is a quote tweet"),
                FieldAnnotation(name="has_media", dtype="boolean", description="True if tweet has photo/video/GIF"),
                FieldAnnotation(name="media_type", dtype="string", description="Type of media: photo, video, animated_gif", nullable=True),
                FieldAnnotation(name="urls", dtype="string", description="Expanded URLs mentioned in the tweet (comma-separated)", nullable=True),
                FieldAnnotation(name="favorite_count", dtype="integer", description="Number of likes at time of export", nullable=True),
                FieldAnnotation(name="retweet_count", dtype="integer", description="Number of retweets at time of export", nullable=True),
                FieldAnnotation(name="lang", dtype="string", description="Detected language code (e.g. 'en', 'nl')", nullable=True),
                FieldAnnotation(name="source", dtype="string", description="Client used to post (Twitter for iPhone, TweetDeck, etc.)", nullable=True),
            ],
            conventions=[
                "Twitter archive wraps JSON in a JavaScript assignment: window.YTD.tweets.part0 = [...]. The JS wrapper is stripped during parsing.",
                "Each array element has a 'tweet' key containing the actual tweet object.",
                "Tweet IDs are snowflakes (64-bit integers as strings). Creation time is encoded in the ID.",
                "full_text is used over text — it contains the complete tweet without truncation.",
                "Retweets start with 'RT @username:' in the text.",
                "t.co URLs in the text are short-forms; expanded_url is the actual destination.",
                "Counts (favorites, retweets) reflect the state at archive export time, not now.",
                "For archives with multiple parts (tweets-part1.js etc.), parse each file separately.",
            ],
        )

    def parse(self, content: bytes, filename: str) -> ParseResult:
        try:
            raw = content.decode("utf-8-sig", errors="replace")
        except Exception as e:
            return ParseResult(success=False, error=f"Could not decode file: {e}")

        # Strip the JS wrapper
        json_str = _strip_js_wrapper(raw)

        try:
            data = json.loads(json_str)
        except json.JSONDecodeError as e:
            return ParseResult(success=False, error=f"JSON parse error after stripping JS wrapper: {e}")

        if not isinstance(data, list):
            return ParseResult(success=False, error="Expected a JSON array after stripping JS wrapper")

        messages = []
        warnings = []

        for i, item in enumerate(data):
            if not isinstance(item, dict):
                warnings.append(f"Item {i}: not a dict, skipped")
                continue

            # Twitter archive: each item is {"tweet": {...}} or directly the tweet
            tweet = item.get("tweet") or item.get("retweeted_status") or item
            if not isinstance(tweet, dict):
                warnings.append(f"Item {i}: could not locate tweet object, skipped")
                continue

            try:
                tweet_id = tweet.get("id_str") or str(tweet.get("id", ""))
                created_at = tweet.get("created_at", "")
                timestamp = _parse_twitter_datetime(created_at)

                # full_text is preferred; fallback to text
                text = tweet.get("full_text") or tweet.get("text", "")

                entities = tweet.get("entities") or {}
                extended_entities = tweet.get("extended_entities") or {}

                is_retweet = text.startswith("RT @")
                reply_to_user = tweet.get("in_reply_to_screen_name") or ""
                reply_to_tweet_id = tweet.get("in_reply_to_status_id_str") or ""
                is_reply = bool(reply_to_tweet_id)
                is_quote = bool(tweet.get("quoted_status_id_str") or tweet.get("is_quote_status"))

                has_media, media_type = _extract_media(entities, extended_entities)
                urls = _extract_urls(entities)

                # Strip source HTML tags: <a href="...">Twitter for iPhone</a>
                source_raw = tweet.get("source", "")
                source = re.sub(r"<[^>]+>", "", source_raw).strip() if source_raw else ""

                messages.append({
                    "id": tweet_id,
                    "timestamp": timestamp,
                    "text": text,
                    "is_retweet": is_retweet,
                    "is_reply": is_reply,
                    "reply_to_user": reply_to_user,
                    "reply_to_tweet_id": reply_to_tweet_id,
                    "is_quote": is_quote,
                    "has_media": has_media,
                    "media_type": media_type,
                    "urls": ", ".join(urls) if urls else "",
                    "favorite_count": int(tweet.get("favorite_count", 0) or 0),
                    "retweet_count": int(tweet.get("retweet_count", 0) or 0),
                    "lang": tweet.get("lang", ""),
                    "source": source,
                })
            except Exception as e:
                warnings.append(f"Item {i}: parse error ({e}), skipped")

        if not messages:
            return ParseResult(success=False, error="No tweets could be parsed")

        envelope = ContextEnvelope(schema=self.schema(), data=messages, warnings=warnings)
        return ParseResult(success=True, envelope=envelope, warnings=warnings)


registry.register(TwitterArchiveParser())
