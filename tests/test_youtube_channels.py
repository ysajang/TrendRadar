"""
collector/youtube_channels.py 테스트

실제 네트워크 호출 없이 aiohttp.ClientSession 을 목킹한다.
할당량 파일은 tmp_path 로 바꿔 실제 data/ 를 건드리지 않는다.
"""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import config
from collector import youtube_channels as yt


# ── 픽스처 ──

@pytest.fixture(autouse=True)
def _yt_config(monkeypatch, tmp_path):
    monkeypatch.setattr(config, "YOUTUBE_API_KEY", "test_key")
    monkeypatch.setattr(config, "YOUTUBE_DAILY_SEARCH_LIMIT", 20)
    monkeypatch.setattr(config, "YOUTUBE_LOOKBACK_HOURS", 48)
    monkeypatch.setattr(config, "YOUTUBE_SEARCH_MAX_RESULTS", 10)
    monkeypatch.setattr(config, "YOUTUBE_CHANNELS_PER_KEYWORD", 3)
    monkeypatch.setattr(config, "YOUTUBE_QUOTA_PATH", tmp_path / "youtube_quota.json")


def _resp(status: int = 200, body: dict | None = None):
    r = AsyncMock()
    r.status = status
    r.json = AsyncMock(return_value=body or {})
    r.__aenter__ = AsyncMock(return_value=r)
    r.__aexit__ = AsyncMock(return_value=False)
    return r


def _search_body(items: list[tuple[str, str, str, str]]) -> dict:
    """items: (video_id, video_title, channel_id, channel_title)"""
    return {
        "items": [
            {
                "id": {"kind": "youtube#video", "videoId": vid},
                "snippet": {"title": vt, "channelId": cid, "channelTitle": ct},
            }
            for vid, vt, cid, ct in items
        ]
    }


QUOTA_ERROR = {"error": {"code": 403, "errors": [{"reason": "quotaExceeded"}]}}


class _FakeApi:
    """URL별로 응답을 돌려주는 가짜 세션. search 응답은 큐에서 순서대로 꺼낸다."""

    def __init__(self, search_responses, videos=None, channels=None):
        self.search_responses = list(search_responses)
        self.videos = videos if videos is not None else _resp(200, {"items": []})
        self.channels = channels if channels is not None else _resp(200, {"items": []})
        self.calls: list[tuple[str, dict]] = []

    def get(self, url, params=None, **kwargs):
        self.calls.append((url, dict(params or {})))
        if url.endswith("/search"):
            item = self.search_responses.pop(0)
            if isinstance(item, Exception):
                raise item
            return item
        if url.endswith("/videos"):
            return self.videos
        if url.endswith("/channels"):
            return self.channels
        raise AssertionError(f"unexpected url {url}")

    def search_calls(self):
        return [c for c in self.calls if c[0].endswith("/search")]


def _run(api: _FakeApi, queries):
    session = AsyncMock()
    session.get = MagicMock(side_effect=api.get)
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)
    with patch("collector.youtube_channels.aiohttp.ClientSession", return_value=session):
        return asyncio.run(yt.fetch_youtube_channels(queries))


def _write_quota(pt_date: str, calls: int):
    config.YOUTUBE_QUOTA_PATH.write_text(
        json.dumps({"pt_date": pt_date, "search_calls": calls}), encoding="utf-8",
    )


def _read_quota() -> dict:
    return json.loads(config.YOUTUBE_QUOTA_PATH.read_text(encoding="utf-8"))


# ── 테스트 ──

class TestFetchYoutubeChannels:
    def test_no_api_key_returns_empty_without_http(self, monkeypatch):
        monkeypatch.setattr(config, "YOUTUBE_API_KEY", "")
        with patch("collector.youtube_channels.aiohttp.ClientSession") as cls:
            result = asyncio.run(yt.fetch_youtube_channels([("버스", "버스 노사")]))
        assert result == []
        cls.assert_not_called()

    def test_empty_queries_returns_empty(self):
        api = _FakeApi([])
        assert _run(api, []) == []
        assert api.calls == []

    def test_search_params(self):
        api = _FakeApi([_resp(200, _search_body([]))])
        _run(api, [("버스", "버스 노사")])
        _, params = api.search_calls()[0]
        assert params["part"] == "snippet"
        assert params["type"] == "video"
        assert params["q"] == "버스 노사"
        assert params["regionCode"] == "KR"
        assert params["relevanceLanguage"] == "ko"
        assert params["order"] == "viewCount"
        assert params["maxResults"] == 10
        assert params["key"] == "test_key"
        assert params["publishedAfter"].endswith("Z")

    def test_same_channel_once_first_video_kept(self):
        api = _FakeApi([_resp(200, _search_body([
            ("v1", "첫 영상", "c1", "채널A"),
            ("v2", "둘째 영상", "c1", "채널A"),
            ("v3", "다른 채널 영상", "c2", "채널B"),
        ]))])
        result = _run(api, [("버스", "버스 노사")])
        channels = result[0]["channels"]
        assert [c["channel_id"] for c in channels] == ["c1", "c2"]
        assert channels[0]["video_id"] == "v1"
        assert channels[0]["video_title"] == "첫 영상"
        assert result[0]["keyword"] == "버스"
        assert result[0]["query"] == "버스 노사"

    def test_channels_per_keyword_limit(self):
        api = _FakeApi([_resp(200, _search_body([
            (f"v{i}", f"영상{i}", f"c{i}", f"채널{i}") for i in range(6)
        ]))])
        result = _run(api, [("AI", "AI")])
        assert len(result[0]["channels"]) == 3

    def test_html_entities_unescaped(self):
        api = _FakeApi([_resp(200, _search_body([
            ("v1", "&quot;속보&quot; 버스 &amp; 파업", "c1", "A&#39;s TV"),
        ]))])
        ch = _run(api, [("버스", "버스")])[0]["channels"][0]
        assert ch["video_title"] == '"속보" 버스 & 파업'
        assert ch["channel_title"] == "A's TV"

    def test_daily_limit_stops_searches(self):
        _write_quota(yt._pt_today(), 18)
        api = _FakeApi([_resp(200, _search_body([])) for _ in range(5)])
        result = _run(api, [(f"k{i}", f"k{i}") for i in range(5)])
        assert len(api.search_calls()) == 2
        assert len(result) == 2
        assert _read_quota()["search_calls"] == 20

    def test_pt_date_change_resets_counter(self):
        _write_quota("2000-01-01", 20)
        api = _FakeApi([_resp(200, _search_body([]))])
        _run(api, [("버스", "버스")])
        assert len(api.search_calls()) == 1
        q = _read_quota()
        assert q["pt_date"] == yt._pt_today()
        assert q["search_calls"] == 1

    def test_quota_exceeded_stops_remaining(self):
        api = _FakeApi([
            _resp(403, QUOTA_ERROR),
            _resp(200, _search_body([])),
            _resp(200, _search_body([])),
        ])
        result = _run(api, [("a", "a"), ("b", "b"), ("c", "c")])
        assert len(api.search_calls()) == 1
        assert result == []

    def test_other_error_retries_once_then_skips(self):
        api = _FakeApi([
            _resp(500, {}),
            _resp(500, {}),
            _resp(200, _search_body([("v1", "t", "c1", "A")])),
        ])
        result = _run(api, [("a", "a"), ("b", "b")])
        assert len(api.search_calls()) == 3
        assert [r["keyword"] for r in result] == ["b"]
        assert _read_quota()["search_calls"] == 3

    def test_network_error_retries_once(self):
        import aiohttp
        api = _FakeApi([
            aiohttp.ClientError("boom"),
            _resp(200, _search_body([("v1", "t", "c1", "A")])),
        ])
        result = _run(api, [("a", "a")])
        assert len(api.search_calls()) == 2
        assert result[0]["channels"][0]["channel_id"] == "c1"

    def test_statistics_filled(self):
        api = _FakeApi(
            [_resp(200, _search_body([("v1", "t", "c1", "A"), ("v2", "t2", "c2", "B")]))],
            videos=_resp(200, {"items": [
                {"id": "v1", "statistics": {"viewCount": "45000"}},
                {"id": "v2", "statistics": {"viewCount": "120"}},
            ]}),
            channels=_resp(200, {"items": [
                {"id": "c1", "statistics": {"subscriberCount": "123000", "hiddenSubscriberCount": False}},
                {"id": "c2", "statistics": {"hiddenSubscriberCount": True}},
            ]}),
        )
        chs = _run(api, [("a", "a")])[0]["channels"]
        assert chs[0]["view_count"] == 45000
        assert chs[0]["subscriber_count"] == 123000
        assert chs[1]["view_count"] == 120
        assert chs[1]["subscriber_count"] is None

    def test_statistics_failure_keeps_results(self):
        api = _FakeApi(
            [_resp(200, _search_body([("v1", "t", "c1", "A")]))],
            videos=_resp(500, {}),
            channels=_resp(500, {}),
        )
        chs = _run(api, [("a", "a")])[0]["channels"]
        assert chs[0]["channel_id"] == "c1"
        assert chs[0]["view_count"] is None
        assert chs[0]["subscriber_count"] is None

    def test_statistics_calls_not_counted(self):
        api = _FakeApi([_resp(200, _search_body([("v1", "t", "c1", "A")]))])
        _run(api, [("a", "a")])
        assert _read_quota()["search_calls"] == 1

    def test_corrupt_quota_file_starts_at_zero(self):
        config.YOUTUBE_QUOTA_PATH.write_text("{not json", encoding="utf-8")
        api = _FakeApi([_resp(200, _search_body([]))])
        _run(api, [("a", "a")])
        assert _read_quota()["search_calls"] == 1

    def test_unexpected_exception_does_not_raise(self):
        api = _FakeApi([RuntimeError("unexpected")])
        assert _run(api, [("a", "a")]) == []

    def test_api_key_not_logged(self, caplog):
        api = _FakeApi([_resp(500, {}), _resp(500, {})])
        with caplog.at_level("DEBUG"):
            _run(api, [("a", "a")])
        assert "test_key" not in caplog.text


class TestRequestRejected:
    """4xx(할당량·429 제외)는 재시도해도 같은 결과 → 재시도 없이 전체 중단 (할당량 보호)"""

    KEY_INVALID = {"error": {"code": 400, "errors": [{"reason": "keyInvalid"}]}}

    def test_400_no_retry_and_stops_remaining(self):
        api = _FakeApi([_resp(400, self.KEY_INVALID) for _ in range(6)])
        result = _run(api, [("a", "a"), ("b", "b"), ("c", "c")])
        assert len(api.search_calls()) == 1
        assert result == []
        assert _read_quota()["search_calls"] == 1

    def test_400_reason_logged_without_key(self, caplog):
        api = _FakeApi([_resp(400, self.KEY_INVALID)])
        with caplog.at_level("WARNING"):
            _run(api, [("a", "a")])
        assert "keyInvalid" in caplog.text
        assert "test_key" not in caplog.text

    def test_429_is_retried(self):
        api = _FakeApi([
            _resp(429, {}),
            _resp(200, _search_body([("v1", "t", "c1", "A")])),
        ])
        result = _run(api, [("a", "a")])
        assert len(api.search_calls()) == 2
        assert result[0]["channels"][0]["channel_id"] == "c1"

    def test_results_before_rejection_kept(self):
        api = _FakeApi([
            _resp(200, _search_body([("v1", "t", "c1", "A")])),
            _resp(400, self.KEY_INVALID),
        ])
        result = _run(api, [("a", "a"), ("b", "b")])
        assert [r["keyword"] for r in result] == ["a"]
