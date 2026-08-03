"""NEXON 길드 명단 조회 — 닉네임 철자 정답지.

2단계 호출 (2026-08 라이브 실측 확인):
  1) GET /guild/id?guild_name=&world_name=      -> {"oguild_id": "..."}
  2) GET /guild/basic?oguild_id=&date=YYYY-MM-DD -> {..., "guild_member": [...]}

guild_member 배열은 소유자별 부캐 그룹이 인접 배치되어 있어,
철자 정답지뿐 아니라 본캐 추정의 힌트로도 쓴다(resolve 참조).
"""
from __future__ import annotations

import json
import urllib.parse
import urllib.request

from ..config import NEXON_BASE, GUILD_NAME, WORLD_NAME, require_nexon_key


def _get(path: str, params: dict) -> dict:
    key = require_nexon_key()
    url = f"{NEXON_BASE}{path}?{urllib.parse.urlencode(params)}"
    req = urllib.request.Request(url, headers={"x-nxopen-api-key": key})
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read().decode("utf-8"))


def week_to_date(week: str) -> str:
    """주차 키(YYYYMMDD) -> NEXON date 파라미터(YYYY-MM-DD).

    주차 키는 정산 종료일(수요일)이다.
    """
    w = week.strip()
    if len(w) != 8 or not w.isdigit():
        raise ValueError(f"주차 키는 YYYYMMDD 형식이어야 합니다: {week!r}")
    return f"{w[:4]}-{w[4:6]}-{w[6:]}"


def get_guild_id() -> str:
    data = _get("/guild/id", {"guild_name": GUILD_NAME, "world_name": WORLD_NAME})
    oguild_id = data.get("oguild_id")
    if not oguild_id:
        raise RuntimeError(f"길드 ID를 찾지 못했습니다: {GUILD_NAME}/{WORLD_NAME}")
    return oguild_id


def fetch_roster(week: str) -> dict:
    """해당 주차(수요일) 시점의 길드원 정식 닉네임 목록.

    Returns:
        {
          "week": "20260729",
          "date": "2026-07-29",
          "guild_name": "나비",
          "member_count": 656,
          "members": ["러카프", "곰쑨", ...]   # 배열 순서 = 부캐 그룹 인접
        }
    """
    date = week_to_date(week)
    basic = _get("/guild/basic", {"oguild_id": get_guild_id(), "date": date})
    members = basic.get("guild_member") or []
    return {
        "week": week,
        "date": date,
        "guild_name": basic.get("guild_name"),
        "member_count": basic.get("guild_member_count", len(members)),
        "members": members,
    }
