"""
YouTube 채널 수집기 (YouTube Data API v3 공식 API 전용 — LegalByDesign)

리포트 키워드별로 최근 N시간 업로드 영상을 조회순 검색해
그 주제를 다루는 채널을 뽑는다.

할당량:
  - search.list 는 수동 검색과 같은 하루 한도를 공유하므로
    봇은 config.YOUTUBE_DAILY_SEARCH_LIMIT 회까지만 호출한다
  - 실패한 요청도 할당량이 차감되므로 호출 직전에 카운트한다
  - 하루 경계는 태평양 시간 자정 (할당량 리셋 기준)
  - videos.list / channels.list 는 search 예산과 별개

YouTube 데이터는 다른 소스와 결합하지 않고 리포트에서 별도 섹션으로 표기한다.
결과는 일일 JSON 저장소에 저장하지 않는다.
"""

import asyncio
import html
import json
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import aiohttp

import config

logger = logging.getLogger(__name__)

API_BASE = "https://www.googleapis.com/youtube/v3"
SEARCH_URL = f"{API_BASE}/search"
VIDEOS_URL = f"{API_BASE}/videos"
CHANNELS_URL = f"{API_BASE}/channels"

REQUEST_TIMEOUT = aiohttp.ClientTimeout(total=15)
PT_ZONE = ZoneInfo("America/Los_Angeles")
QUOTA_REASONS = {"quotaExceeded", "dailyLimitExceeded"}
STATS_BATCH_SIZE = 50


class _QuotaExhausted(Exception):
    """할당량 소진 (봇 상한 도달 또는 API quotaExceeded)"""


# ──────────────────────────────────────────────
# 할당량 가드
# ──────────────────────────────────────────────

def _pt_today() -> str:
    return datetime.now(PT_ZONE).strftime("%Y-%m-%d")


def _load_search_calls() -> int:
    """오늘(PT) 사용한 search 호출 수. 날짜가 다르거나 파일 이상이면 0"""
    path = config.YOUTUBE_QUOTA_PATH
    if not path.exists():
        return 0
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(f"YouTube 할당량 파일 읽기 실패 → 0부터 시작: {e}")
        return 0
    if not isinstance(data, dict) or data.get("pt_date") != _pt_today():
        return 0
    calls = data.get("search_calls", 0)
    return calls if isinstance(calls, int) and calls >= 0 else 0


def _save_search_calls(calls: int) -> None:
    try:
        config.YOUTUBE_QUOTA_PATH.write_text(
            json.dumps({"pt_date": _pt_today(), "search_calls": calls}),
            encoding="utf-8",
        )
    except OSError as e:
        logger.warning(f"YouTube 할당량 파일 저장 실패: {e}")


def _consume_search() -> None:
    """search 1회 사용 기록. 상한 도달 시 _QuotaExhausted"""
    calls = _load_search_calls()
    if calls >= config.YOUTUBE_DAILY_SEARCH_LIMIT:
        raise _QuotaExhausted("봇 search 상한 도달")
    _save_search_calls(calls + 1)


# ──────────────────────────────────────────────
# API 호출
# ──────────────────────────────────────────────

def _is_quota_error(body: dict) -> bool:
    errors = (body or {}).get("error", {}).get("errors", []) or []
    return any(e.get("reason") in QUOTA_REASONS for e in errors if isinstance(e, dict))


async def _search_once(session: aiohttp.ClientSession, query: str) -> list[dict] | None:
    """search.list 1회. 성공 시 items, 일반 실패 시 None, 할당량 오류 시 _QuotaExhausted"""
    _consume_search()
    published_after = (
        datetime.now(timezone.utc) - timedelta(hours=config.YOUTUBE_LOOKBACK_HOURS)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    params = {
        "part": "snippet",
        "type": "video",
        "q": query,
        "regionCode": "KR",
        "relevanceLanguage": "ko",
        "order": "viewCount",
        "publishedAfter": published_after,
        "maxResults": config.YOUTUBE_SEARCH_MAX_RESULTS,
        "key": config.YOUTUBE_API_KEY,
    }
    try:
        async with session.get(SEARCH_URL, params=params, timeout=REQUEST_TIMEOUT) as resp:
            try:
                body = await resp.json()
            except (aiohttp.ContentTypeError, json.JSONDecodeError):
                body = {}
            if resp.status == 200:
                return body.get("items", []) or []
            if resp.status == 403 and _is_quota_error(body):
                raise _QuotaExhausted("API quotaExceeded")
            logger.warning(f"YouTube 검색 HTTP {resp.status} ({query})")
            return None
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.warning(f"YouTube 검색 연결 실패 ({query}): {type(e).__name__}")
        return None


async def _search(session: aiohttp.ClientSession, query: str) -> list[dict] | None:
    """실패 시 1회 재시도"""
    items = await _search_once(session, query)
    if items is None:
        items = await _search_once(session, query)
    return items


def _pick_channels(items: list[dict]) -> list[dict]:
    """채널 기준 중복 제거 (조회순 유지), 채널별 첫 영상이 대표"""
    seen: set[str] = set()
    channels: list[dict] = []
    for item in items:
        snippet = item.get("snippet", {}) or {}
        channel_id = snippet.get("channelId", "")
        video_id = (item.get("id", {}) or {}).get("videoId", "")
        if not channel_id or not video_id or channel_id in seen:
            continue
        seen.add(channel_id)
        channels.append({
            "channel_id": channel_id,
            "channel_title": html.unescape(snippet.get("channelTitle", "")),
            "subscriber_count": None,
            "video_title": html.unescape(snippet.get("title", "")),
            "video_id": video_id,
            "view_count": None,
        })
        if len(channels) >= config.YOUTUBE_CHANNELS_PER_KEYWORD:
            break
    return channels


def _to_int(value) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


async def _fetch_statistics(
    session: aiohttp.ClientSession, url: str, ids: list[str],
) -> dict[str, dict]:
    """videos.list / channels.list statistics. 실패한 배치는 빈 결과"""
    stats: dict[str, dict] = {}
    for i in range(0, len(ids), STATS_BATCH_SIZE):
        batch = ids[i:i + STATS_BATCH_SIZE]
        params = {"part": "statistics", "id": ",".join(batch), "key": config.YOUTUBE_API_KEY}
        try:
            async with session.get(url, params=params, timeout=REQUEST_TIMEOUT) as resp:
                if resp.status != 200:
                    logger.warning(f"YouTube 통계 HTTP {resp.status}")
                    continue
                body = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError, json.JSONDecodeError) as e:
            logger.warning(f"YouTube 통계 조회 실패: {type(e).__name__}")
            continue
        for item in body.get("items", []) or []:
            if item.get("id"):
                stats[item["id"]] = item.get("statistics", {}) or {}
    return stats


def _apply_statistics(results: list[dict], video_stats: dict, channel_stats: dict) -> None:
    for entry in results:
        for ch in entry["channels"]:
            v = video_stats.get(ch["video_id"], {})
            ch["view_count"] = _to_int(v.get("viewCount"))
            c = channel_stats.get(ch["channel_id"], {})
            if c.get("hiddenSubscriberCount"):
                ch["subscriber_count"] = None
            else:
                ch["subscriber_count"] = _to_int(c.get("subscriberCount"))


# ──────────────────────────────────────────────
# 수집 메인
# ──────────────────────────────────────────────

async def fetch_youtube_channels(queries: list[tuple[str, str]]) -> list[dict]:
    """
    키워드별 YouTube 채널 수집 (실패해도 예외를 던지지 않음)

    Args:
        queries: (표시 키워드, 검색어) 리스트

    Returns:
        [{"keyword", "query", "channels": [{"channel_id", "channel_title",
          "subscriber_count", "video_title", "video_id", "view_count"}]}]
    """
    if not config.YOUTUBE_API_KEY:
        logger.warning("YOUTUBE_API_KEY 없음 → YouTube 섹션 생략")
        return []
    if not queries:
        return []

    results: list[dict] = []
    try:
        async with aiohttp.ClientSession() as session:
            for keyword, query in queries:
                try:
                    items = await _search(session, query)
                except _QuotaExhausted as e:
                    logger.warning(f"YouTube 검색 중단: {e} (남은 키워드 건너뜀)")
                    break
                if items is None:
                    continue
                results.append({
                    "keyword": keyword,
                    "query": query,
                    "channels": _pick_channels(items),
                })

            video_ids = [c["video_id"] for r in results for c in r["channels"]]
            channel_ids = list(dict.fromkeys(
                c["channel_id"] for r in results for c in r["channels"]
            ))
            if video_ids:
                video_stats = await _fetch_statistics(session, VIDEOS_URL, video_ids)
                channel_stats = await _fetch_statistics(session, CHANNELS_URL, channel_ids)
                _apply_statistics(results, video_stats, channel_stats)
    except Exception as e:
        logger.error(f"YouTube 수집 실패 (리포트는 계속): {type(e).__name__}")
        return results

    logger.info(
        f"YouTube 수집 완료: 키워드 {len(results)}개 / "
        f"오늘(PT) search 사용 {_load_search_calls()}/{config.YOUTUBE_DAILY_SEARCH_LIMIT}"
    )
    return results
