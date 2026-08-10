from fastapi import FastAPI, HTTPException, Cookie, Response, Body, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# ── 시간대 ──────────────────────────────────────────────────────
KST = timezone(timedelta(hours=9))


def _kst_now_iso() -> str:
    """벌금 관리 작업 감사 로그에 쓰는 ISO 8601 timestamp (KST)."""
    return datetime.now(KST).isoformat(timespec="seconds")
import asyncio
import statistics
import hashlib
import hmac
import time
import unicodedata
import boto3
import httpx
import os


def _load_env_file(path: str = ".env") -> None:
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env_file()

app = FastAPI()

# ── DynamoDB 설정 ──────────────────────────────────────────────────
TABLE_NAME = os.environ.get("DYNAMODB_TABLE", "maple_guild")
AWS_REGION  = os.environ.get("AWS_REGION", "ap-northeast-1")

dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table    = dynamodb.Table(TABLE_NAME)

# ── Nexon Open API 설정 ───────────────────────────────────────────
NEXON_API_KEY = os.environ.get("NEXON_API_KEY", "")
NEXON_BASE    = "https://open.api.nexon.com/maplestory/v1"
PROFILE_TTL   = 600  # 프로필 캐시 10분

_ocid_cache: dict[str, str] = {}
_profile_cache: dict[str, tuple[float, dict]] = {}

# ── 응답 캐시 (F5 난타/트래픽 폭주 방어) ───────────────────────────
# 무거운 DynamoDB scan 결과를 짧게 캐싱. 데이터 변경 시 즉시 무효화.
RESPONSE_CACHE_TTL = 60  # 초
_response_cache: dict[str, tuple[float, object]] = {}


def _cache_get(key: str):
    entry = _response_cache.get(key)
    if entry and time.time() < entry[0]:
        return entry[1]
    return None


def _cache_set(key: str, value):
    _response_cache[key] = (time.time() + RESPONSE_CACHE_TTL, value)


def _cache_clear():
    """데이터 변경 시 전체 응답 캐시 무효화 (벌금/스펙다운/baseline 등)"""
    _response_cache.clear()

# ── 관리자 인증 설정 ───────────────────────────────────────────────
# 관리자 닉네임 화이트리스트 (아이디로 사용)
ADMIN_NAMES = {"볼땡글", "NAVYSEAL", "또치와댕댕이", "멜빵군", "바가", "외칼", "코료룽", "빠민"}
# 비밀번호는 평문 미저장. SHA-256(salt + password) 해시만 코드에 보관.
_PW_SALT = "naby_suro_2026_salt"
_PW_HASH = "3d3815fcafd5e08b806cb7a33bee30d7f0f3339d77b41a67aa254b285fb6b5ba"
# 세션 토큰 서명 키 (환경변수 우선, 없으면 해시 재사용)
SESSION_SECRET = os.environ.get("SESSION_SECRET", _PW_HASH)
SESSION_COOKIE = "naby_admin"
SESSION_MAX_AGE = 86400  # 1일
# 운영(HTTPS)은 secure 쿠키, 로컬 HTTP 테스트는 COOKIE_SECURE=false 로 비활성
COOKIE_SECURE = os.environ.get("COOKIE_SECURE", "true").lower() not in ("false", "0", "no")


def _check_password(pw: str) -> bool:
    # 한글 정규화: 직접 타이핑(NFD)과 복붙(NFC) 입력을 동일하게 처리
    pw = unicodedata.normalize("NFC", pw or "")
    h = hashlib.sha256((_PW_SALT + pw).encode("utf-8")).hexdigest()
    return hmac.compare_digest(h, _PW_HASH)


def _make_session_token(username: str) -> str:
    import urllib.parse
    msg = f"admin:{username}".encode("utf-8")
    sig = hmac.new(SESSION_SECRET.encode("utf-8"), msg, hashlib.sha256).hexdigest()
    # 쿠키 value는 ASCII만 허용 → 한글 username은 URL 인코딩
    return f"{urllib.parse.quote(username)}:{sig}"


def _verify_session(cookie_val: str | None) -> str | None:
    """유효하면 관리자 닉네임 반환, 아니면 None"""
    import urllib.parse
    if not cookie_val or ":" not in cookie_val:
        return None
    enc_username, sig = cookie_val.rsplit(":", 1)
    username = unicodedata.normalize("NFC", urllib.parse.unquote(enc_username))
    if username not in ADMIN_NAMES:
        return None
    expected = _make_session_token(username).rsplit(":", 1)[1]
    return username if hmac.compare_digest(sig, expected) else None


def is_admin(naby_admin: str = Cookie(None)) -> bool:
    return _verify_session(naby_admin) is not None


def require_admin(naby_admin: str = Cookie(None)) -> str:
    username = _verify_session(naby_admin)
    if username is None:
        raise HTTPException(status_code=401, detail="관리자 로그인이 필요합니다.")
    return username


def get_latest_week() -> str:
    """메타 아이템에서 최신 주차 조회"""
    resp = table.get_item(Key={"week": "METADATA", "rank": 0})
    item = resp.get("Item")
    if not item:
        raise HTTPException(status_code=404, detail="데이터가 없습니다.")
    return item["latest_week"]


def get_week_display(week: str) -> str:
    """주차 YYYYMMDD를 m/d~m/d 형식으로 변환 (목요일~다음주 수요일)"""
    try:
        wed = datetime.strptime(week, "%Y%m%d")
        thu = wed - timedelta(days=6)
        return f"{thu.month}/{thu.day}~{wed.month}/{wed.day}"
    except:
        return week


def get_members(week: str) -> list[dict]:
    """해당 주차 길드원 전체 조회 (자리표시 레코드 is_ghost 제외)"""
    from boto3.dynamodb.conditions import Key
    resp = table.query(
        KeyConditionExpression=Key("week").eq(week)
        & Key("rank").gt(0)
    )
    items = [i for i in resp.get("Items", []) if not i.get("is_ghost")]
    # rank 기준 정렬, Decimal → int 변환
    return sorted(
        [{"rank": int(i["rank"]), "name": i["name"], "job": i["job"], "score": int(i["score"]),
          "fine_count": int(i.get("fine_count", 0)), "weapon_tier": i.get("weapon_tier")} for i in items],
        key=lambda x: x["rank"]
    )


def _find_member_at_week(week: str, name: str) -> dict | None:
    """해당 주차에서 닉네임으로 회원 레코드 1건 조회 (자리표시 제외)."""
    from boto3.dynamodb.conditions import Key, Attr
    resp = table.query(
        KeyConditionExpression=Key("week").eq(week) & Key("rank").gt(0),
        FilterExpression=Attr("name").eq(name),
    )
    items = [i for i in resp.get("Items", []) if not i.get("is_ghost")]
    return items[0] if items else None


# ── 이상치 탐지 (공통) ─────────────────────────────────────────────
# 리더보드의 이상치 표시 정의. 불성실 참여 탐지는 별도 규칙을 쓴다.
OUTLIER_RATIO = 0.9      # 직전 추세 평균 대비 10% 이상 하락 = 이상치
TRAIL_WEEKS = 3          # 비교 기준: 직전 최대 N주 참여 점수
MIN_TRAIL = 2            # 판정에 필요한 최소 직전 이력 주차 (미만이면 판정 불가)
MIN_PARTICIPATION = 3    # 분석 가능한 최소 참여 주차


def detect_outliers_trailing(
    participating: list[tuple[str, int]],
) -> tuple[set[str], dict[str, float]]:
    """최근 추세(직전 최대 TRAIL_WEEKS주) 기반 이상치 식별.

    Args:
        participating: [(week_str, score), ...] — score>0 + baseline 이상으로 이미 필터된 회원의 참여 주차.

    Returns:
        (outlier_weeks_set, outlier_base):
            outlier_weeks_set: 이상치로 판정된 주차들의 set.
            outlier_base: {week: 그 주차의 판정 기준이 된 직전 N주 평균값}.
                          드롭률 표시 등 사후 가공에 사용.
    """
    outlier_weeks_set: set[str] = set()
    outlier_base: dict[str, float] = {}
    sorted_part = sorted(participating, key=lambda x: x[0])  # 주차 오름차순
    for idx in range(len(sorted_part)):
        w, s = sorted_part[idx]
        prior = [ps for _, ps in sorted_part[max(0, idx - TRAIL_WEEKS):idx]]
        if len(prior) < MIN_TRAIL:   # 직전 이력 부족 → 판정 불가
            continue
        base = sum(prior) / len(prior)
        if base > 0 and s < base * OUTLIER_RATIO:
            outlier_weeks_set.add(w)
            outlier_base[w] = base
    return outlier_weeks_set, outlier_base


def find_latest_consecutive_outlier_group(
    outlier_weeks_sorted: list[str],
    all_weeks_sorted: list[str],
) -> list[str] | None:
    """outlier_weeks 중 인접 연속 길이 ≥ 2인 그룹 가운데 가장 최근 것을 반환.

    Args:
        outlier_weeks_sorted: 오름차순 정렬된 이상치 주차 리스트.
        all_weeks_sorted:     오름차순 정렬된 참여 주차 리스트(인접 판정 기준).

    Returns:
        가장 최근 연속 그룹(주차 리스트). 조건을 만족하는 그룹이 없으면 None.
    """
    if not outlier_weeks_sorted:
        return None
    index_map = {w: i for i, w in enumerate(all_weeks_sorted)}
    groups: list[list[str]] = []
    cur: list[str] = []
    prev_idx = -10
    for w in outlier_weeks_sorted:
        idx = index_map.get(w, -1)
        if idx == prev_idx + 1:
            cur.append(w)
        else:
            if cur:
                groups.append(cur)
            cur = [w]
        prev_idx = idx
    if cur:
        groups.append(cur)
    valid = [g for g in groups if len(g) >= 2]
    return valid[-1] if valid else None


@app.get("/api/data")
def get_data():
    cached = _cache_get("data")
    if cached is not None:
        return cached

    week    = get_latest_week()
    members = get_members(week)

    scores  = [m["score"] for m in members if m["score"] > 0]

    stats = {
        "week":           week,
        "week_display":   get_week_display(week),
        "total":          len(members),
        "active":         len(scores),
        "total_score":    sum(scores),
        "avg_score":      int(sum(scores) / len(scores)) if scores else 0,
        "max_score":      max(scores) if scores else 0,
    }

    result = {
        "stats":            stats,
        "top30":            members[:30],
        "all_members":      members,
    }
    _cache_set("data", result)
    return result


@app.get("/api/week/{week}")
def get_week(week: str):
    members = get_members(week)
    if not members:
        raise HTTPException(status_code=404, detail="해당 주차 데이터가 없습니다.")
    return {"week": week, "week_display": get_week_display(week), "all_members": members}


import json as _json

# 오로라 주간 지하수로 길드 랭킹 (chuchu.gg 프록시). 주간 갱신이라 5분 캐시.
_SURO_RANK_CACHE = {"ts": 0.0, "data": None}


def _parse_suro_world(html: str, world: str):
    """chuchu.gg /ranking/suro 서버렌더 HTML(RSC flight)에서 해당 월드
    길드 수로 랭킹 배열을 추출한다. 데이터는 이스케이프된 JSON 문자열로 박혀 있다."""
    key = '\\"worldName\\":\\"%s\\"' % world
    i = html.find(key)
    if i < 0:
        return None
    j = html.find('\\"ranking\\":[', i)
    if j < 0:
        return None
    start = html.index('[', j)
    depth = 0
    end = None
    for k in range(start, len(html)):
        c = html[k]
        if c == '[':
            depth += 1
        elif c == ']':
            depth -= 1
            if depth == 0:
                end = k + 1
                break
    if end is None:
        return None
    frag = html[start:end].replace('\\"', '"').replace('\\\\', '\\')
    return _json.loads(frag)


@app.get("/api/suro-ranking")
def get_suro_ranking(refresh: bool = False):
    """오로라 서버 주간 지하수로 길드 랭킹 상위 10 (chuchu.gg 프록시)."""
    now = time.time()
    floor = 15 if refresh else 300
    if _SURO_RANK_CACHE["data"] and now - _SURO_RANK_CACHE["ts"] < floor:
        return _SURO_RANK_CACHE["data"]
    try:
        resp = httpx.get(
            "https://chuchu.gg/ranking/suro",
            headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                                   "AppleWebKit/537.36 (KHTML, like Gecko)"},
            timeout=15,
            follow_redirects=True,
        )
        resp.raise_for_status()
        rk = _parse_suro_world(resp.text, "오로라")
        if not rk:
            raise ValueError("parse failed")
    except Exception:
        if _SURO_RANK_CACHE["data"]:
            return {**_SURO_RANK_CACHE["data"], "stale": True}
        raise HTTPException(status_code=502, detail="수로 랭킹을 불러오지 못했습니다.")
    top = rk[:10]
    out = {
        "date": top[0].get("date") if top else None,
        "world": "오로라",
        "our_guild": "나비",
        "ranking": [
            {
                "rank": g.get("ranking"),
                "name": g.get("guild_name", ""),
                "point": int(g.get("guild_point") or 0),
                "members": int((g.get("basic") or {}).get("guild_user_count") or 0),
                "master": g.get("guild_master_name", ""),
            }
            for g in top
        ],
        "fetched_at": int(now),
    }
    _SURO_RANK_CACHE.update(ts=now, data=out)
    return out


@app.get("/api/history")
def get_history():
    cached = _cache_get("history")
    if cached is not None:
        return cached

    from boto3.dynamodb.conditions import Attr
    resp = table.scan(FilterExpression=Attr("rank").gt(0))
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=Attr("rank").gt(0),
            ExclusiveStartKey=resp["LastEvaluatedKey"]
        )
        items.extend(resp.get("Items", []))

    weeks = defaultdict(list)
    for item in items:
        week = item["week"]
        if week == "METADATA":
            continue
        if item.get("is_ghost"):
            continue  # 벌금 이월용 자리표시 — 점수 통계 제외
        weeks[week].append(int(item["score"]))

    def quantile(arr, p):
        if not arr: return 0
        s = sorted(arr)
        idx = p * (len(s) - 1)
        lo, hi = int(idx), min(int(idx) + 1, len(s) - 1)
        return int(s[lo] + (s[hi] - s[lo]) * (idx - lo))

    result = []
    for week in sorted(weeks.keys()):
        scores = weeks[week]
        active = [s for s in scores if s > 0]
        mean = int(sum(active) / len(active)) if active else 0
        stddev = int((sum((s - mean) ** 2 for s in active) / len(active)) ** 0.5) if active else 0
        result.append({
            "week": week,
            "week_display": get_week_display(week),
            "total_score": sum(active),
            "active": len(active),
            "mean": mean,
            "stddev": stddev,
            "median_score": quantile(active, 0.5),
            "q1": quantile(active, 0.25),
            "q3": quantile(active, 0.75),
        })
    _cache_set("history", result)
    return result


@app.get("/api/member/{name}")
def get_member_history(name: str, naby_admin: str = Cookie(None)):
    """회원 히스토리 — 벌금 누적 납부 횟수(fine_count)는 관리자에게만 포함"""
    from boto3.dynamodb.conditions import Attr
    admin = _verify_session(naby_admin) is not None

    resp = table.scan(FilterExpression=Attr("name").eq(name) & Attr("rank").gt(0))
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=Attr("name").eq(name) & Attr("rank").gt(0),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    # 자리표시 레코드는 히스토리 표시에서 제외 (점수 0 / 가짜 rank)
    items = [i for i in items if not i.get("is_ghost")]

    history = sorted(
        [
            {
                "week":         i["week"],
                "week_display": get_week_display(i["week"]),
                "rank":         int(i["rank"]),
                "job":          i["job"],
                "score":        int(i["score"]),
                **({"fine_count": int(i.get("fine_count", 0))} if admin else {}),
            }
            for i in items
        ],
        key=lambda x: x["week"],
    )
    result = {"name": name, "history": history}
    if admin:
        result["fine_count"] = int(items and history[-1].get("fine_count", 0) or 0)
    return result


async def _nexon_get(client: httpx.AsyncClient, path: str, params: dict) -> dict:
    headers = {"x-nxopen-api-key": NEXON_API_KEY}
    r = await client.get(f"{NEXON_BASE}{path}", params=params, headers=headers, timeout=10.0)
    r.raise_for_status()
    return r.json()


async def _get_ocid(client: httpx.AsyncClient, name: str) -> str:
    if name in _ocid_cache:
        return _ocid_cache[name]
    data = await _nexon_get(client, "/id", {"character_name": name})
    ocid = data.get("ocid")
    if not ocid:
        raise HTTPException(status_code=404, detail="character_not_found")
    _ocid_cache[name] = ocid
    return ocid


def _weapon_tier_from_name(weapon_name: str) -> str | None:
    """무기 이름으로 등급 판별. 제네시스/데스티니 무기는 이름이 각각
    '제네시스'/'데스티니'로 시작한다. (해방 무기는 이 접두어가 고유하다)"""
    w = (weapon_name or "").strip()
    if w.startswith("데스티니"):
        return "destiny"
    if w.startswith("제네시스"):
        return "genesis"
    return None


async def _fetch_weapon_tier(client: httpx.AsyncClient, name: str) -> tuple[str | None, str]:
    """캐릭터의 착용 무기 등급과 원본 무기명을 반환. (tier, weapon_name)
    tier 는 'genesis' | 'destiny' | None."""
    ocid = await _get_ocid(client, name)
    data = await _nexon_get(client, "/character/item-equipment", {"ocid": ocid})
    weapon_name = ""
    for it in data.get("item_equipment", []) or []:
        # 주무기 슬롯만 확인 (보조무기/엠블렘 제외)
        if it.get("item_equipment_part") == "무기" or it.get("item_equipment_slot") == "무기":
            weapon_name = it.get("item_name") or ""
            break
    return _weapon_tier_from_name(weapon_name), weapon_name


# 나비 길드 현재 인원 (NEXON /guild/basic). 하루 단위 갱신이라 1시간 캐시.
_GUILD_COUNT_CACHE = {"ts": 0.0, "data": None}


@app.get("/api/guild-count")
async def get_guild_count(refresh: bool = False):
    """나비 길드 현재 등록 인원(캐릭터 수). NEXON 데이터는 하루 단위(전일 기준)."""
    now = time.time()
    floor = 60 if refresh else 3600
    if _GUILD_COUNT_CACHE["data"] and now - _GUILD_COUNT_CACHE["ts"] < floor:
        return _GUILD_COUNT_CACHE["data"]
    if not NEXON_API_KEY:
        if _GUILD_COUNT_CACHE["data"]:
            return _GUILD_COUNT_CACHE["data"]
        raise HTTPException(status_code=503, detail="NEXON_API_KEY 미설정")
    try:
        async with httpx.AsyncClient() as client:
            gid = (await _nexon_get(
                client, "/guild/id",
                {"guild_name": "나비", "world_name": "오로라"},
            )).get("oguild_id")
            basic = await _nexon_get(client, "/guild/basic", {"oguild_id": gid})
    except Exception:
        if _GUILD_COUNT_CACHE["data"]:
            return {**_GUILD_COUNT_CACHE["data"], "stale": True}
        raise HTTPException(status_code=502, detail="길드 인원 조회 실패")
    out = {
        "user_count": int(basic.get("guild_user_count") or 0),     # 계정 수
        "char_count": int(basic.get("guild_member_count") or 0),   # 캐릭터 수(부캐 포함)
        "guild": basic.get("guild_name") or "나비",
        "date": (basic.get("date") or "")[:10] or None,
    }
    _GUILD_COUNT_CACHE.update(ts=now, data=out)
    return out


@app.get("/api/member/{name}/profile")
async def get_member_profile(name: str):
    if not NEXON_API_KEY:
        raise HTTPException(status_code=503, detail="NEXON_API_KEY 미설정")

    cached = _profile_cache.get(name)
    if cached and (time.time() - cached[0] < PROFILE_TTL):
        return cached[1]

    async with httpx.AsyncClient() as client:
        try:
            ocid = await _get_ocid(client, name)
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            if status in (400, 404):
                raise HTTPException(status_code=404, detail="character_not_found")
            raise HTTPException(status_code=502, detail=f"nexon_error_{status}")

        try:
            basic, stat, hexa = await asyncio.gather(
                _nexon_get(client, "/character/basic", {"ocid": ocid}),
                _nexon_get(client, "/character/stat", {"ocid": ocid}),
                _nexon_get(client, "/character/hexamatrix", {"ocid": ocid}),
            )
        except httpx.HTTPStatusError as e:
            raise HTTPException(status_code=502, detail=f"nexon_error_{e.response.status_code}")

    combat_power = None
    for s in stat.get("final_stat", []) or []:
        if s.get("stat_name") == "전투력":
            try:
                combat_power = int(s["stat_value"])
            except (TypeError, ValueError):
                pass
            break

    profile = {
        "name":         basic.get("character_name"),
        "level":        basic.get("character_level"),
        "exp_rate":     basic.get("character_exp_rate"),
        "world":        basic.get("world_name"),
        "job":          basic.get("character_class"),
        "job_level":    basic.get("character_class_level"),
        "guild":        basic.get("character_guild_name"),
        "image":        basic.get("character_image"),
        "combat_power": combat_power,
        "hexa_cores": [
            {
                "name":  c.get("hexa_core_name"),
                "level": c.get("hexa_core_level"),
                "type":  c.get("hexa_core_type"),
            }
            for c in (hexa.get("character_hexa_core_equipment") or [])
        ],
    }

    _profile_cache[name] = (time.time(), profile)
    return profile


@app.post("/api/refresh-weapon-marks")
async def refresh_weapon_marks(_admin: str = Depends(require_admin)):
    """최신 주차 회원 전원의 착용 무기를 넥슨 API로 조회해 weapon_tier 를 갱신한다.
    무기는 자주 바뀌지 않으므로 이 갱신은 관리자가 필요할 때만 수동 실행한다.

    - 성공 + 제네시스/데스티니 → weapon_tier SET
    - 성공 + 해당 없음 → weapon_tier REMOVE
    - 조회 실패(캐릭터 없음/개명/API 오류) → 기존 값 유지 (덮어쓰지 않음)
    """
    if not NEXON_API_KEY:
        raise HTTPException(status_code=503, detail="NEXON_API_KEY 미설정")

    from boto3.dynamodb.conditions import Key

    latest = get_latest_week()
    resp = table.query(KeyConditionExpression=Key("week").eq(latest) & Key("rank").gt(0))
    members = [i for i in resp.get("Items", []) if not i.get("is_ghost")]

    sem = asyncio.Semaphore(8)   # 넥슨 rate limit 보호
    counts = {"genesis": 0, "destiny": 0, "none": 0, "failed": 0}
    samples: list[str] = []
    updates: list[tuple[int, str | None]] = []   # (rank, tier|None) — 성공 건만

    async with httpx.AsyncClient() as client:
        async def one(item):
            name = item.get("name")
            rank = int(item["rank"])
            async with sem:
                try:
                    tier, wname = await _fetch_weapon_tier(client, name)
                except Exception:
                    counts["failed"] += 1
                    return
                updates.append((rank, tier))
                if tier:
                    counts[tier] += 1
                    if len(samples) < 12:
                        samples.append(f"{name}: {wname}")
                else:
                    counts["none"] += 1
        await asyncio.gather(*[one(m) for m in members])

    # DynamoDB 반영 — 성공 건만 (실패 건은 기존 값 유지)
    for rank, tier in updates:
        if tier:
            table.update_item(
                Key={"week": latest, "rank": rank},
                UpdateExpression="SET weapon_tier = :t",
                ExpressionAttributeValues={":t": tier},
            )
        else:
            table.update_item(
                Key={"week": latest, "rank": rank},
                UpdateExpression="REMOVE weapon_tier",
            )

    _cache_clear()
    return {
        "week": latest,
        "total": len(members),
        "genesis": counts["genesis"],
        "destiny": counts["destiny"],
        "none": counts["none"],
        "failed": counts["failed"],
        "samples": samples,   # 판별 검증용 — 실제 감지된 무기명 일부
    }


@app.get("/api/zero-score/{week}")
def get_zero_score_members(week: str, _admin: str = Depends(require_admin)):
    """벌금 납부 대상자 조회 — 해당 주차 0점 + 미해결 부과 이월 회원
    - left_guild = True 회원은 자동 제외
    - 응답에 pending_weeks / pending_count / is_carryover 필드 포함
    """
    from boto3.dynamodb.conditions import Key, Attr

    cache_key = f"zero-score:{week}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    # 해당 주차 전체 회원 조회 (점수 필터 없이)
    resp = table.query(
        KeyConditionExpression=Key("week").eq(week) & Key("rank").gt(0)
    )
    all_week_items = resp.get("Items", [])

    # 표시 대상 필터: 0점 OR pending 잔존 OR 이번 주 결제 처리됨,
    # left_guild != True, 이번 주 면제 != True
    # (이번 주 결제 처리된 회원도 "벌금 취소" 버튼으로 되돌릴 수 있도록 목록에 유지)
    items: list[dict] = []
    for i in all_week_items:
        if bool(i.get("left_guild")):
            continue
        exempt_weeks = set(i.get("fine_exempt_weeks") or [])
        if week in exempt_weeks:
            continue  # 이번 주 벌금 면제 처리된 회원 제외
        score = int(i.get("score", 0))
        pending = list(i.get("pending_weeks") or [])
        paid_this_week = i.get("last_fine_week") == week
        if score == 0 or len(pending) > 0 or paid_this_week:
            items.append(i)

    if not items:
        empty = {"week": week, "members": []}
        _cache_set(cache_key, empty)
        return empty

    zero_names = {i["name"] for i in items}

    # 0점 회원의 모든 주차 score를 한 번의 scan으로 수집
    all_resp = table.scan(FilterExpression=Attr("rank").gt(0))
    all_items = all_resp.get("Items", [])
    while "LastEvaluatedKey" in all_resp:
        all_resp = table.scan(
            FilterExpression=Attr("rank").gt(0),
            ExclusiveStartKey=all_resp["LastEvaluatedKey"],
        )
        all_items.extend(all_resp.get("Items", []))

    weekly_by_name: dict[str, dict[str, int]] = defaultdict(dict)
    for i in all_items:
        name = i.get("name")
        if name in zero_names and i.get("week") != "METADATA":
            weekly_by_name[name][i["week"]] = int(i.get("score", 0))

    def calc_consecutive(name: str) -> int:
        weekly = weekly_by_name.get(name, {})
        sorted_weeks = sorted(weekly.keys(), reverse=True)
        if not sorted_weeks or sorted_weeks[0] != week:
            return 0
        count = 0
        for w in sorted_weeks:
            if weekly[w] == 0:
                count += 1
            else:
                break
        return count

    def _build_row(i):
        pending = list(i.get("pending_weeks") or [])
        score = int(i.get("score", 0))
        is_ghost = bool(i.get("is_ghost"))
        return {
            # 자리표시 레코드는 실제 등수가 없으므로 "-" 표시
            "rank":               "-" if is_ghost else int(i["rank"]),
            "name":               i["name"],
            "job":                i["job"],
            "score":              score,
            "fine_count":         int(i.get("fine_count", 0)),
            "last_fine_week":     i.get("last_fine_week"),
            "consecutive_misses": calc_consecutive(i["name"]),
            "pending_weeks":      pending,
            "pending_count":      len(pending),
            # 이번 주는 정상 참여(score > 0)인데 과거 미해결로 잔존하는 이월 회원
            # 자리표시 레코드도 이월 케이스이므로 동일 처리
            "is_carryover":       score > 0 or is_ghost,
            "is_ghost":           is_ghost,
            # 감사 정보: 어느 관리자가 언제 어떤 작업을 했는지
            "processed_by":       i.get("processed_by"),
            "processed_at":       i.get("processed_at"),
            "processed_action":   i.get("processed_action"),
        }

    # 자리표시(ghost)는 마지막으로, 일반 회원은 rank 순으로 정렬
    zero_members = sorted(
        [_build_row(i) for i in items],
        key=lambda x: (x["rank"] == "-", x["rank"] if x["rank"] != "-" else 0),
    )
    result = {"week": week, "members": zero_members}
    _cache_set(cache_key, result)
    return result


@app.post("/api/fine/{week}/{name}")
def record_fine_payment(week: str, name: str, _admin: str = Depends(require_admin)):
    """벌금 납부 기록 (미해결 pending_weeks 일괄 처리)

    - 회원의 pending_weeks가 N개면 한 번 클릭으로 fine_count += N
    - pending_weeks가 비었으면 단일 부과 (fine_count += 1) 폴백
    - last_fine_week == week 이면 차단 (이미 이번 주 처리 완료)
    - left_guild == True 이면 차단
    """
    from boto3.dynamodb.conditions import Key, Attr

    resp = table.query(
        KeyConditionExpression=Key("week").eq(week) & Key("rank").gt(0),
        FilterExpression=Attr("name").eq(name)
    )
    items = resp.get("Items", [])
    if not items:
        raise HTTPException(status_code=404, detail="길드원을 찾을 수 없습니다.")

    member = items[0]

    if bool(member.get("left_guild")):
        raise HTTPException(status_code=400, detail="길드 탈퇴 처리된 회원입니다.")

    last_fine_week = member.get("last_fine_week")
    if last_fine_week == week:
        raise HTTPException(status_code=400, detail="이 주차에 이미 벌금이 부과되었습니다.")

    cur_fine_count = int(member.get("fine_count", 0))
    pending_weeks = list(member.get("pending_weeks") or [])
    process_count = max(len(pending_weeks), 1)
    new_fine_count = cur_fine_count + process_count

    # 누진 산식: amount(N회차) = 10 + (N - 1)
    total_amount = sum(10 + (fc - 1) for fc in range(cur_fine_count + 1, new_fine_count + 1))

    # 취소 대비: 처리한 pending_weeks / 처리 건수를 백업 필드로 저장 후 pending_weeks 제거
    # processed_by/at/action: 어느 관리자가 언제 어떤 작업을 했는지 감사 기록
    table.update_item(
        Key={"week": week, "rank": int(member["rank"])},
        UpdateExpression=(
            "SET fine_count = :fc, last_fine_week = :lfw, "
            "paid_pending_weeks = :pp, paid_pending_count = :pc, "
            "processed_by = :pb, processed_at = :pat, processed_action = :pact "
            "REMOVE pending_weeks"
        ),
        ExpressionAttributeValues={
            ":fc": new_fine_count,
            ":lfw": week,
            ":pp": pending_weeks,
            ":pc": process_count,
            ":pb": _admin,
            ":pat": _kst_now_iso(),
            ":pact": "납부",
        },
    )

    _cache_clear()
    return {
        "name":            name,
        "fine_count":      new_fine_count,
        "processed_count": process_count,
        "total_amount":    total_amount,
        "week":            week,
    }


@app.delete("/api/fine/{week}/{name}")
def undo_fine_payment(week: str, name: str, _admin: str = Depends(require_admin)):
    """벌금 납부 취소 (이번 주 부과 건만 취소, 취소 후 last_fine_week 제거하여 재부과 가능 상태로 전환)"""
    from boto3.dynamodb.conditions import Key, Attr

    resp = table.query(
        KeyConditionExpression=Key("week").eq(week) & Key("rank").gt(0),
        FilterExpression=Attr("name").eq(name)
    )
    items = resp.get("Items", [])
    if not items:
        raise HTTPException(status_code=404, detail="길드원을 찾을 수 없습니다.")

    member = items[0]
    fine_count = int(member.get("fine_count", 0))
    last_fine_week = member.get("last_fine_week")

    # 이번 주 부과된 벌금만 취소 대상
    if last_fine_week != week:
        raise HTTPException(status_code=400, detail="이 주차에 부과된 벌금이 없습니다.")
    if fine_count <= 0:
        raise HTTPException(status_code=400, detail="취소할 누적 벌금이 없습니다.")

    # 결제 시 저장한 백업으로 정확한 차감 + pending_weeks 복원
    paid_pending = list(member.get("paid_pending_weeks") or [])
    paid_count = int(member.get("paid_pending_count", 1))
    if paid_count <= 0:
        paid_count = 1
    fine_count = max(0, fine_count - paid_count)

    # last_fine_week / 백업 필드 / 처리자 감사필드 제거. pending_weeks는 복원이 있을 때만 SET
    if paid_pending:
        table.update_item(
            Key={"week": week, "rank": int(member["rank"])},
            UpdateExpression=(
                "SET fine_count = :fc, pending_weeks = :pw "
                "REMOVE last_fine_week, paid_pending_weeks, paid_pending_count, "
                "processed_by, processed_at, processed_action"
            ),
            ExpressionAttributeValues={":fc": fine_count, ":pw": paid_pending},
        )
    else:
        table.update_item(
            Key={"week": week, "rank": int(member["rank"])},
            UpdateExpression=(
                "SET fine_count = :fc "
                "REMOVE last_fine_week, paid_pending_weeks, paid_pending_count, "
                "processed_by, processed_at, processed_action"
            ),
            ExpressionAttributeValues={":fc": fine_count},
        )

    _cache_clear()
    return {"name": name, "fine_count": fine_count, "week": week}


@app.post("/api/leave-guild/{name}")
def leave_guild(name: str, _admin: str = Depends(require_admin)):
    """길드 탈퇴 처리 — 벌금 납부 섹션에서만 제외
    - 최신 주차 레코드에 left_guild = True SET
    - pending_weeks 모두 소멸 (REMOVE)
    - 다른 화면(전체 길드원/리더보드/히스토리)에는 영향 없음
    """
    from boto3.dynamodb.conditions import Key, Attr

    latest = get_latest_week()
    resp = table.query(
        KeyConditionExpression=Key("week").eq(latest) & Key("rank").gt(0),
        FilterExpression=Attr("name").eq(name)
    )
    items = resp.get("Items", [])
    if not items:
        raise HTTPException(status_code=404, detail="길드원을 찾을 수 없습니다.")

    member = items[0]
    table.update_item(
        Key={"week": latest, "rank": int(member["rank"])},
        UpdateExpression=(
            "SET left_guild = :t, "
            "processed_by = :pb, processed_at = :pat, processed_action = :pact "
            "REMOVE pending_weeks"
        ),
        ExpressionAttributeValues={
            ":t": True,
            ":pb": _admin,
            ":pat": _kst_now_iso(),
            ":pact": "탈퇴",
        },
    )
    _cache_clear()
    return {"name": name, "left_guild": True, "week": latest}


@app.post("/api/spec-down/{name}")
def confirm_spec_down(name: str, _admin: str = Depends(require_admin)):
    """스펙 다운 인정 — 감지된 주차를 그 회원의 새 기준선으로 SET.

    장비 환산 점수가 실제로 하락한 것이 확인된 회원만 대상이다(탐지 시스템의
    '스펙 다운 감지' 태그). 인정하면 그 주차 이전 기록은 판정에서 제외되어,
    낮아진 스펙에 맞는 새 점수대가 그 회원의 기준이 된다. 이 처리가 없으면
    예전 고점 기록이 계속 기대치를 끌어올려 매주 탐지되는 문제가 생긴다.

    누적 탐지 횟수는 기준선 이전 기록이 판정에서 빠져도 사라지면 안 되므로
    (히스토리 관리 목적), 인정 시점의 횟수를 spec_down_prior_count 로 동결하고
    이후 새 기준선에서 발생한 건만 여기에 더한다.
    """
    detection = get_detection(_admin=_admin)
    hit = next((h for h in detection["detected"] if h["name"] == name), None)
    if hit is None:
        raise HTTPException(
            status_code=400,
            detail="이번 주 탐지 목록에 없는 회원입니다.",
        )
    if hit.get("tag") != "스펙 다운 감지":
        raise HTTPException(
            status_code=400,
            detail="장비 스펙 하락이 확인되지 않은 회원입니다. "
                   "'스펙 다운 감지' 태그가 붙은 경우에만 인정할 수 있습니다.",
        )

    week = detection["week"]
    member = _find_member_at_week(week, name)
    if member is None:
        raise HTTPException(status_code=404, detail="최신 주차 데이터가 없습니다.")

    prior = int(hit.get("detected_count") or 0)
    table.update_item(
        Key={"week": week, "rank": int(member["rank"])},
        UpdateExpression="SET spec_down_from_week = :sd, spec_down_prior_count = :pc",
        ExpressionAttributeValues={":sd": week, ":pc": prior},
    )

    _cache_clear()
    return {
        "name":                name,
        "spec_down_from_week": week,
        "spec_down_prior_count": prior,
        "spec_change_pct":     hit.get("spec_change_pct"),
    }


@app.delete("/api/spec-down/{name}")
def cancel_spec_down(name: str, _admin: str = Depends(require_admin)):
    """스펙 다운 인정 취소 — 이번 주차에 처리한 건만 되돌린다.

    오클릭 복구용이므로 처리한 그 주차 안에서만 허용한다. 다음 주차 데이터가
    올라오면 기준선이 이월되어 이미 판정에 반영된 상태이므로 되돌리지 않는다.
    """
    latest = get_latest_week()
    member = _find_member_at_week(latest, name)
    if member is None:
        raise HTTPException(status_code=404, detail="길드원을 찾을 수 없습니다.")
    if (member.get("spec_down_from_week") or "") != latest:
        raise HTTPException(
            status_code=400,
            detail="이번 주차에 인정 처리된 회원만 취소할 수 있습니다.",
        )

    table.update_item(
        Key={"week": latest, "rank": int(member["rank"])},
        UpdateExpression="REMOVE spec_down_from_week, spec_down_prior_count",
    )

    _cache_clear()
    return {"name": name, "cancelled": True, "week": latest}


# 길드 단위 baseline 설정 기능은 제거했다.
#
# 단체 하락(보약 효과 종료 등)은 탐지 판정의 환경 보정이 매주 자동으로 상쇄한다.
#   env = median(전 회원 이번주 ÷ 직전 추세),  기대치 = 본인 추세 x env
# 실측상 전체 -7.7% 주차에서 보정 전 16명이던 탐지가 보정 후 4명으로,
# 평상시와 같은 수준을 유지했다.
#
# 반면 baseline 을 설정하면 그 이전 주차가 판정 구간에서 통째로 빠져
# 누적 탐지 이력이 소실된다(실측 31명 42건 -> 6명 6건). 얻는 것은 개인 평균의
# 변화뿐인데 그 값은 어느 화면에도 노출되지 않는다. 되돌리는 수단도 없었다.
#
# guild_baseline_from_week 읽기 경로는 남겨 둔다. 현재 미설정이라 무동작이며,
# 과거에 값이 설정된 환경에서도 거동이 달라지지 않도록 하기 위함이다.
# 개인 단위 예외는 spec_down_from_week 가 담당한다(이력 동결 + 주차 내 취소).




@app.post("/api/rename-member")
def rename_member(payload: dict = Body(...), _admin: str = Depends(require_admin)):
    """닉네임 변경(이관) — 기존 닉네임의 모든 주차 레코드를 신규 닉네임으로 변경.

    - 점수/벌금 등 모든 히스토리는 레코드에 그대로 붙어 있으므로 name 필드만
      바꾸면 신규 닉네임이 히스토리를 그대로 인수받는다.
    - 이름 변경으로 끊긴 벌금 carry-forward를 복원하기 위해, 통합된 회원의
      누적 상태(fine_count/last_fine_week/spec_down/면제주차)를 최신 주차 레코드에
      반영한다. (max/min/합집합 기반이라 재실행해도 안전)
    - 같은 주차에 두 닉네임이 모두 존재하면 자동 병합은 위험하므로 차단한다.
    """
    from boto3.dynamodb.conditions import Attr

    old = unicodedata.normalize("NFC", (payload.get("old_name") or "").strip())
    new = unicodedata.normalize("NFC", (payload.get("new_name") or "").strip())
    if not old or not new:
        raise HTTPException(status_code=400, detail="기존/신규 닉네임을 모두 입력하세요.")
    if old == new:
        raise HTTPException(status_code=400, detail="기존과 신규 닉네임이 동일합니다.")

    def scan_by_name(nm: str) -> list[dict]:
        items: list[dict] = []
        resp = table.scan(FilterExpression=Attr("name").eq(nm))
        items += resp.get("Items", [])
        while "LastEvaluatedKey" in resp:
            resp = table.scan(
                FilterExpression=Attr("name").eq(nm),
                ExclusiveStartKey=resp["LastEvaluatedKey"],
            )
            items += resp.get("Items", [])
        return [i for i in items if i.get("week") != "METADATA"]

    old_items = scan_by_name(old)
    new_items_before = scan_by_name(new)

    if not old_items:
        # 기존 닉네임 기록이 없고 신규 닉네임이 이미 존재하면 이미 변경 완료된 것으로 안내
        if new_items_before:
            raise HTTPException(
                status_code=409,
                detail={"code": "already_changed", "message": f"이미 '{new}'(으)로 수정한 닉네임입니다."},
            )
        # 둘 다 존재하지 않음 — 기존 닉네임 오입력
        raise HTTPException(
            status_code=404,
            detail={"code": "not_found", "message": f"'{old}' 닉네임의 존재 기록이 없습니다. 다시 확인해주세요."},
        )

    if not new_items_before:
        # 기존은 존재하나 신규 닉네임 기록이 없음 — 신규 닉네임 오입력
        raise HTTPException(
            status_code=404,
            detail={"code": "not_found", "message": f"'{new}' 닉네임의 존재 기록이 없습니다. 다시 확인해주세요."},
        )
    old_weeks = {i["week"] for i in old_items}
    new_weeks = {i["week"] for i in new_items_before}
    collision_weeks = sorted(old_weeks & new_weeks)
    if collision_weeks:
        pretty = ", ".join(get_week_display(w) for w in collision_weeks)
        raise HTTPException(
            status_code=409,
            detail=f"같은 주차에 두 닉네임이 모두 존재해 자동 변경할 수 없습니다 ({pretty}). 수동 확인이 필요합니다.",
        )

    # 1) 이름 변경 (조건부 업데이트로 경합 방지)
    for i in old_items:
        table.update_item(
            Key={"week": i["week"], "rank": int(i["rank"])},
            UpdateExpression="SET #n = :new",
            ConditionExpression="#n = :old",
            ExpressionAttributeNames={"#n": "name"},
            ExpressionAttributeValues={":new": new, ":old": old},
        )

    # 2) 벌금/상태 히스토리 인수 — 통합된 회원의 누적 상태를 최신 주차 레코드에 반영
    unified = scan_by_name(new)
    real = [i for i in unified if int(i.get("rank", 0)) > 0 and not i.get("is_ghost")]

    max_fine = max((int(i.get("fine_count", 0)) for i in unified), default=0)
    last_fines = [i.get("last_fine_week") for i in unified if i.get("last_fine_week")]
    last_fine = max(last_fines) if last_fines else None
    spec_downs = [i.get("spec_down_from_week") for i in unified if i.get("spec_down_from_week")]
    spec_down = min(spec_downs) if spec_downs else None
    exempt = sorted({w for i in unified for w in (i.get("fine_exempt_weeks") or [])})

    reconciled_week = None
    if real:
        latest_rec = max(real, key=lambda i: i["week"])
        expr_set, vals = [], {}
        if max_fine > int(latest_rec.get("fine_count", 0)):
            expr_set.append("fine_count = :fc"); vals[":fc"] = max_fine
        if last_fine and last_fine != latest_rec.get("last_fine_week"):
            expr_set.append("last_fine_week = :lfw"); vals[":lfw"] = last_fine
        if spec_down and not latest_rec.get("spec_down_from_week"):
            expr_set.append("spec_down_from_week = :sd"); vals[":sd"] = spec_down
        if exempt and exempt != sorted(latest_rec.get("fine_exempt_weeks") or []):
            expr_set.append("fine_exempt_weeks = :fe"); vals[":fe"] = exempt
        if expr_set:
            table.update_item(
                Key={"week": latest_rec["week"], "rank": int(latest_rec["rank"])},
                UpdateExpression="SET " + ", ".join(expr_set),
                ExpressionAttributeValues=vals,
            )
            reconciled_week = latest_rec["week"]

    _cache_clear()
    return {
        "old_name":      old,
        "new_name":      new,
        "updated_weeks": sorted(old_weeks),
        "updated_count": len(old_items),
        "fine_count":    max_fine,
        "reconciled_week": reconciled_week,
    }


@app.post("/api/fine-exempt/{week}/{name}")
def exempt_fine(week: str, name: str, _admin: str = Depends(require_admin)):
    """이번 주 벌금 면제 (신규 가입·부득이한 사유로 마감 임박 미참)
    - fine_exempt_weeks에 현재 주차 추가 (이번 주만 면제)
    - pending_weeks에서 현재 주차 제거 (다음 주 이월 방지)
    - fine_count 증가 없음
    """
    from boto3.dynamodb.conditions import Key, Attr

    resp = table.query(
        KeyConditionExpression=Key("week").eq(week) & Key("rank").gt(0),
        FilterExpression=Attr("name").eq(name)
    )
    items = resp.get("Items", [])
    if not items:
        raise HTTPException(status_code=404, detail="길드원을 찾을 수 없습니다.")

    member = items[0]
    exempt_weeks = list(member.get("fine_exempt_weeks") or [])
    if week not in exempt_weeks:
        exempt_weeks.append(week)
    pending = [p for p in (member.get("pending_weeks") or []) if p != week]

    table.update_item(
        Key={"week": week, "rank": int(member["rank"])},
        UpdateExpression=(
            "SET fine_exempt_weeks = :ew, pending_weeks = :pw, "
            "processed_by = :pb, processed_at = :pat, processed_action = :pact"
        ),
        ExpressionAttributeValues={
            ":ew": exempt_weeks,
            ":pw": pending,
            ":pb": _admin,
            ":pat": _kst_now_iso(),
            ":pact": "면제",
        },
    )
    _cache_clear()
    return {"name": name, "fine_exempt_weeks": exempt_weeks, "week": week}


@app.get("/api/leaderboards")
def get_leaderboards(naby_admin: str = Cookie(None)):
    """현 길드원 기준 리더보드
    - 공개: 평균 등수 TOP 5 + 점수 상승폭 TOP 5
    - 관리자 전용(인증 시 추가): 미참 횟수 / 점수 하락폭 / 불성실 의심 / 보약 효과 알림
    동률 시 닉네임 가나다순으로 정렬
    """
    from boto3.dynamodb.conditions import Attr

    admin = _verify_session(naby_admin) is not None
    cache_key = "leaderboards:admin" if admin else "leaderboards:public"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    latest = get_latest_week()

    # 모든 주차 레코드 scan (METADATA 제외, rank > 0)
    resp = table.scan(FilterExpression=Attr("rank").gt(0))
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=Attr("rank").gt(0),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    # 회원별 rank 누적 + 회원별 주차→score 매핑 + 최신 주차 회원 식별
    by_name: dict[str, list[int]] = defaultdict(list)
    score_by_name_week: dict[str, dict[str, int]] = defaultdict(dict)
    latest_members: dict[str, dict] = {}
    weeks_set: set[str] = set()
    for i in items:
        wk = i.get("week")
        if wk == "METADATA":
            continue
        if i.get("is_ghost"):
            continue  # 벌금 이월용 자리표시 — 리더보드/평균등수 계산 제외
        name = i.get("name")
        if not name:
            continue
        weeks_set.add(wk)
        by_name[name].append(int(i["rank"]))
        score_by_name_week[name][wk] = int(i.get("score", 0))
        if wk == latest:
            latest_members[name] = i

    # 직전 주차 식별 (정렬된 주차 목록의 latest 바로 앞)
    sorted_weeks = sorted(weeks_set)
    prev_week: str | None = None
    if latest in sorted_weeks:
        idx = sorted_weeks.index(latest)
        if idx > 0:
            prev_week = sorted_weeks[idx - 1]

    # 평균 등수 TOP 5 (현 길드원만 대상)
    avg_list: list[dict] = []
    for name, ranks in by_name.items():
        if name not in latest_members:
            continue
        avg = sum(ranks) / len(ranks)
        avg_list.append({
            "name": name,
            "job":  latest_members[name].get("job", ""),
            "avg_rank": round(avg, 2),
            "weeks": len(ranks),
        })
    avg_list.sort(key=lambda x: (x["avg_rank"], x["name"]))
    top_avg = avg_list[:10]

    # 누적 납부 횟수 TOP 5 (현 길드원, fine_count > 0)
    fine_list: list[dict] = []
    for name, m in latest_members.items():
        fc = int(m.get("fine_count", 0))
        if fc <= 0:
            continue
        fine_list.append({
            "name": name,
            "job":  m.get("job", ""),
            "fine_count": fc,
        })
    fine_list.sort(key=lambda x: (-x["fine_count"], x["name"]))
    top_fine = fine_list[:10]

    # 상승폭 / 하락폭 / 불성실 의심 — 공통 헬퍼 사용(detect_outliers_trailing,
    # find_latest_consecutive_outlier_group). 상수도 모듈 레벨에 통일.

    # METADATA 조회 — 길드 baseline (읽기 경로만 유지, 현재 미설정)
    meta_resp = table.get_item(Key={"week": "METADATA", "rank": 0}).get("Item") or {}
    guild_baseline_from = meta_resp.get("guild_baseline_from_week") or ""

    delta_list: list[dict] = []

    for name, m in latest_members.items():
        weekly = score_by_name_week.get(name, {})
        # 회원의 effective_from = max(spec_down, guild_baseline)
        member_sd = m.get("spec_down_from_week") or ""
        effective_from = max(member_sd, guild_baseline_from)

        # 참여 주차만 (score > 0) AND baseline 이상
        participating = [
            (w, s) for w, s in weekly.items()
            if s > 0 and w >= effective_from
        ]
        if len(participating) < MIN_PARTICIPATION:
            continue

        # 이상치를 제외한 본인 평균 — 상승/하락 리더보드의 '평균 대비' 표시에 쓴다
        outlier_weeks_set, _ = detect_outliers_trailing(participating)
        clean_pairs = [(w, s) for w, s in participating if w not in outlier_weeks_set]
        clean_mean = (
            sum(s for _, s in clean_pairs) / len(clean_pairs)
            if clean_pairs
            else sum(s for _, s in participating) / len(participating)
        )

        curr = int(m.get("score", 0))

        # 상승/하락 분석: 이번 주 참여 (이상치 회원도 포함 — 사용자 결정)
        #
        # 순위 기준은 wow_delta(전주 대비). 이전에는 avg_delta(본인 평균 대비)로
        # 정렬했는데, 꾸준히 성장해 평균이 낮게 깔린 회원이 이번 주에 거의 안 올라도
        # 계속 상위에 남는 문제가 있었다. (예: 이번 주 +4,407 인데 평균 대비로는
        # +49,560 이라 1위) avg_delta 는 참고용으로 함께 내려준다.
        if curr > 0:
            prev_score = score_by_name_week.get(name, {}).get(prev_week, 0) if prev_week else 0
            delta_list.append({
                "name":         name,
                "job":          m.get("job", ""),
                "curr_score":   curr,
                "prev_score":   int(prev_score),
                # 전주에 미참(0점)이면 상승폭을 계산할 수 없다 → 순위에서 제외
                "delta":        int(curr - prev_score) if prev_score > 0 else None,
                "avg_delta":    int(curr - clean_mean),
            })

    # delta 가 None(전주 미참)인 회원은 상승/하락 순위에서 제외
    ranked_delta = [d for d in delta_list if d["delta"] is not None]
    rise_top5 = sorted(ranked_delta, key=lambda x: (-x["delta"], x["name"]))[:10]
    drop_top5 = sorted(ranked_delta, key=lambda x: (x["delta"], x["name"]))[:10]

    # 무기 마크(weapon_tier) 일괄 주입 — 리더보드 각 행에 회원의 착용 무기 등급 부여
    _wtier = {name: m.get("weapon_tier") for name, m in latest_members.items()}
    for _row in (*top_avg, *top_fine, *rise_top5, *drop_top5):
        _row["weapon_tier"] = _wtier.get(_row["name"])
    # 공개 응답: 평균 등수 + 상승폭만
    result = {
        "avg_rank_top3": top_avg,
        "rise_top5":     rise_top5,
    }
    # 관리자 전용: 미참 횟수 / 하락폭 / 의심 명단
    if admin:
        result["fine_top3"]    = top_fine
        result["drop_top5"]    = drop_top5

    _cache_set(cache_key, result)
    return result


# ── 불성실 참여 탐지 (베타) ────────────────────────────────────────
# 버전 이력. 최신 항목이 현재 적용값이다.
# 새 패치를 낼 때는 이 목록 맨 앞에 항목을 추가한다.
# threshold_since 는 임계값이 마지막으로 산출된 시점. 임계값을 건드리지 않은
# 패치에서는 그대로 승계해, 배너가 재산출된 것처럼 보이지 않게 한다.
DETECTION_VERSIONS = [
    {
        "version": "v1.2",
        "date": "2026-08-07",
        "threshold": 0.15,
        "threshold_since": "2026-08-03",
        "trail_weeks": 3,
        "min_history": 3,
        "title": "스펙 환산 전면 개편 — 곱연산 데미지 지수·최강 프리셋 세트효과",
    },
    {
        "version": "v1.1",
        "date": "2026-08-04",
        "threshold": 0.15,
        "threshold_since": "2026-08-03",
        "trail_weeks": 3,
        "min_history": 3,
        "title": "장비 기반 스펙 다운 감지 도입 및 개인 기준선 인정",
    },
    {
        "version": "v1.0",
        "date": "2026-08-03",
        "threshold": 0.15,
        "threshold_since": "2026-08-03",
        "trail_weeks": 3,
        "min_history": 3,
        "title": "환경 보정 방식 도입 및 임계값 -15% 산출",
    },
]
DETECTION_CURRENT = DETECTION_VERSIONS[0]

# 데미지 지수가 이 비율 이상 하락하면 스펙 다운으로 본다.
# v1.2 곱연산 데미지 지수는 스펙 미변경 회원이 정확히 0%, 실제 하향은 -20~50%로
# 크게 벌어져(실측), -5%면 정상 변동과 확실히 구분된다.
SPEC_DROP_THRESHOLD = 0.05


@app.get("/api/detection")
def get_detection(_admin: str = Depends(require_admin)):
    """불성실 참여 탐지 (베타).

    길드 전체 변동(환경 요인)을 상쇄하고, 개인만 이탈한 정도를 본다.

        환경 변동률 = median(전 회원의 이번주 / 직전 추세)
        개인 기대치 = 본인 추세 x 환경 변동률
        이탈률      = (실제 - 기대치) / 기대치

    이탈률이 임계값 아래면 탐지한다. 상습성은 필터가 아니라 표시 정보로,
    1회여도 탐지하되 누적 횟수를 함께 보여준다.
    """
    from boto3.dynamodb.conditions import Attr

    cache_key = "detection"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached

    cfg = DETECTION_CURRENT
    THRESHOLD = cfg["threshold"]
    TRAIL = cfg["trail_weeks"]
    MIN_HIST = cfg["min_history"]

    resp = table.scan(FilterExpression=Attr("rank").gt(0))
    items = resp.get("Items", [])
    while "LastEvaluatedKey" in resp:
        resp = table.scan(
            FilterExpression=Attr("rank").gt(0),
            ExclusiveStartKey=resp["LastEvaluatedKey"],
        )
        items.extend(resp.get("Items", []))

    score_by_name: dict[str, dict[str, int]] = defaultdict(dict)
    spec_by_name: dict[str, dict[str, dict]] = defaultdict(dict)
    latest_members: dict[str, dict] = {}
    weeks_set: set[str] = set()
    latest = get_latest_week()
    for i in items:
        wk = i.get("week")
        if wk == "METADATA" or i.get("is_ghost"):
            continue
        name = i.get("name")
        if not name:
            continue
        weeks_set.add(wk)
        score_by_name[name][wk] = int(i.get("score", 0))
        if i.get("spec_score"):
            spec_by_name[name][wk] = {
                "score": int(i.get("spec_score") or 0),
                "items": int(i.get("spec_items") or 0),
            }
        if wk == latest:
            latest_members[name] = i

    weeks = sorted(weeks_set)

    meta_resp = table.get_item(Key={"week": "METADATA", "rank": 0}).get("Item") or {}
    guild_baseline = meta_resp.get("guild_baseline_from_week") or ""

    def effective_from(member: dict) -> str:
        return max(member.get("spec_down_from_week") or "", guild_baseline)

    def trend_at(name: str, wi: int, floor: str) -> float | None:
        """직전 TRAIL주 중 참여(>0)한 주차의 평균. 2주 미만이면 None."""
        vals = [
            score_by_name[name][weeks[j]]
            for j in range(max(0, wi - TRAIL), wi)
            if weeks[j] in score_by_name[name]
            and score_by_name[name][weeks[j]] > 0
            and weeks[j] >= floor
        ]
        return statistics.mean(vals) if len(vals) >= 2 else None

    def history_count(name: str, wi: int, floor: str) -> int:
        return sum(
            1
            for j in range(0, wi)
            if weeks[j] in score_by_name[name]
            and score_by_name[name][weeks[j]] > 0
            and weeks[j] >= floor
        )

    def detect_week(wi: int) -> tuple[float, list[dict]]:
        """해당 주차의 (환경 변동률, 탐지 목록)."""
        wk = weeks[wi]
        ratios: list[float] = []
        cands: list[tuple[str, int, float]] = []
        for name, weekly in score_by_name.items():
            score = weekly.get(wk, 0)
            if score <= 0:                      # 미참은 벌금 체계가 별도로 다룸
                continue
            member = latest_members.get(name) or {}
            floor = effective_from(member)
            if wk < floor:
                continue
            if history_count(name, wi, floor) < MIN_HIST:
                continue                        # 비교 기준이 없는 신규 등은 판정 보류
            tr = trend_at(name, wi, floor)
            if not tr:
                continue
            ratios.append(score / tr)
            cands.append((name, score, tr))

        if len(ratios) < 10:                    # 표본이 적으면 환경 추정 불가
            return 1.0, []

        env = statistics.median(ratios)
        hits: list[dict] = []
        for name, score, tr in cands:
            expected = tr * env
            if expected <= 0:
                continue
            dev = (score - expected) / expected
            if dev < -THRESHOLD:
                hits.append({
                    "name": name,
                    "score": score,
                    "trend": int(tr),
                    "expected": int(expected),
                    "deviation_pct": round(dev * 100, 1),
                })
        return env, hits

    def spec_tag(name: str, wi: int) -> dict | None:
        """장비 스펙 변화로 수로 점수 하락이 설명되는지 판정.

        스펙이 낮아졌다면 수로 점수 하락은 당연한 결과이므로 불성실로 보지 않는다.
        장비 수가 줄어든 경우는 강화를 위한 임시 탈착일 수 있어 판정을 보류한다.
        """
        wk = weeks[wi]
        cur = spec_by_name.get(name, {}).get(wk)
        if not cur:
            return None
        # 직전에 스펙이 기록된 주차와 비교
        prev = None
        for j in range(wi - 1, -1, -1):
            p = spec_by_name.get(name, {}).get(weeks[j])
            if p:
                prev = p
                break
        if not prev or prev["score"] <= 0:
            return None

        if cur["items"] < prev["items"]:
            return {
                "tag": "장비 미착용",
                "detail": f"장비 {prev['items']}개 → {cur['items']}개. "
                          f"강화를 위한 일시 탈착일 수 있어 판정을 보류합니다.",
                "spec_change_pct": round((cur["score"] / prev["score"] - 1) * 100, 1),
            }

        change = (cur["score"] - prev["score"]) / prev["score"]
        if change <= -SPEC_DROP_THRESHOLD:
            return {
                "tag": "스펙 다운 감지",
                "detail": f"데미지 지수 {prev['score']:,} → {cur['score']:,} "
                          f"({change*100:+.1f}%). 점수 하락이 스펙 변화로 설명됩니다.",
                "spec_change_pct": round(change * 100, 1),
            }
        return None

    latest_idx = weeks.index(latest)
    env_ratio, current_hits = detect_week(latest_idx)

    # 누적 탐지 횟수 — 과거 전 주차에 동일 규칙 적용.
    # 스펙 다운 인정으로 기준선이 올라간 회원은 그 이전 주차가 판정에서 빠지므로,
    # 인정 시점에 동결해 둔 횟수(spec_down_prior_count)를 더해 이력을 보존한다.
    prior_counts = {
        n: int(m.get("spec_down_prior_count") or 0) for n, m in latest_members.items()
    }
    total_counts: dict[str, int] = defaultdict(int)
    for wi in range(1, len(weeks)):
        _, hits = detect_week(wi)
        for h in hits:
            total_counts[h["name"]] += 1

    def cumulative(name: str) -> int:
        return total_counts.get(name, 0) + prior_counts.get(name, 0)

    # 태그 판정 — 스펙 변화로 설명되면 불성실 의심에서 제외한다
    for h in current_hits:
        m = latest_members.get(h["name"]) or {}
        h["job"] = m.get("job", "")
        h["detected_count"] = cumulative(h["name"])
        st = spec_tag(h["name"], latest_idx)
        if st:
            h["tag"] = st["tag"]
            h["tag_detail"] = st["detail"]
            h["spec_change_pct"] = st["spec_change_pct"]
        else:
            h["tag"] = "불성실 참여 의심"
            h["tag_detail"] = ""
            h["spec_change_pct"] = None
    current_hits.sort(key=lambda x: x["deviation_pct"])

    # 누적 탐지 횟수 TOP 10 — 이번 주 탐지 여부와 무관하게 현 길드원 전체를 본다.
    # 상습성은 탐지 필터가 아니라 참고 정보이므로 순위로만 제시한다.
    suspect_rank = sorted(
        (
            {
                "name": n,
                "job": m.get("job", ""),
                "count": cumulative(n),
                "detected_now": any(h["name"] == n for h in current_hits),
            }
            for n, m in latest_members.items()
            if cumulative(n) > 0
        ),
        key=lambda x: (-x["count"], x["name"]),
    )[:10]

    # 이번 주차에 스펙 다운으로 인정 처리된 회원 — 취소(되돌리기) 대상.
    # 인정과 동시에 탐지 목록에서 빠지므로 여기서 따로 노출해야 손댈 수 있다.
    confirmed: list[dict] = []
    for name, m in latest_members.items():
        if (m.get("spec_down_from_week") or "") != latest:
            continue
        st = spec_tag(name, latest_idx)
        confirmed.append({
            "name": name,
            "job": m.get("job", ""),
            "score": int(m.get("score", 0)),
            "detected_count": cumulative(name),
            "spec_change_pct": st["spec_change_pct"] if st else None,
        })
    confirmed.sort(key=lambda x: -x["score"])

    result = {
        "week": latest,
        "week_display": get_week_display(latest),
        "version": cfg["version"],
        "version_date": cfg["date"],
        "threshold_pct": round(THRESHOLD * 100, 1),
        "threshold_since": cfg["threshold_since"],
        "trail_weeks": TRAIL,
        "min_history": MIN_HIST,
        "env_shift_pct": round((env_ratio - 1) * 100, 1),
        "detected": current_hits,
        "detected_count": len(current_hits),
        "spec_down_confirmed": confirmed,
        "suspect_rank": suspect_rank,
        "evaluated_count": sum(
            1 for n, w in score_by_name.items() if w.get(latest, 0) > 0
        ),
        # 스펙 수집이 안 된 주차는 태그가 전부 '불성실 참여 의심'으로 떨어진다.
        # 조용히 성능이 낮아지는 상태이므로 화면에 노출해 확인 가능하게 한다.
        "spec_collected": sum(1 for n in spec_by_name if latest in spec_by_name[n]),
    }
    _cache_set(cache_key, result)
    return result


@app.get("/api/health")
def health():
    return {"status": "ok"}


@app.post("/api/login")
def login(response: Response, payload: dict = Body(...)):
    """관리자 로그인 — 닉네임(아이디) + 비밀번호 검증 후 세션 쿠키 발급"""
    username = unicodedata.normalize("NFC", (payload.get("username") or "").strip())
    password = payload.get("password") or ""
    if username not in ADMIN_NAMES or not _check_password(password):
        raise HTTPException(status_code=401, detail="아이디 또는 비밀번호가 올바르지 않습니다.")
    token = _make_session_token(username)
    response.set_cookie(
        key=SESSION_COOKIE,
        value=token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=COOKIE_SECURE,
    )
    return {"ok": True, "username": username}


@app.post("/api/logout")
def logout(response: Response):
    response.delete_cookie(SESSION_COOKIE)
    return {"ok": True}


@app.get("/api/auth-status")
def auth_status(naby_admin: str = Cookie(None)):
    """현재 세션의 관리자 여부 반환 (프론트엔드 분기용)"""
    username = _verify_session(naby_admin)
    return {"is_admin": username is not None, "username": username}


@app.get("/")
def root():
    return FileResponse("static/index.html")          # 공개 페이지

@app.get("/login")
def login_page():
    return FileResponse("static/login.html")          # 관리자 로그인

@app.get("/admin")
def admin_page():
    # 개요 페이지는 제거됨(공개 메인의 축소판이라 중복). 관리 페이지로 이동.
    return RedirectResponse("/manage")

@app.get("/manage")
def manage_page():
    return FileResponse("static/management.html")     # 관리자 관리

@app.get("/detection")
def detection_page():
    return FileResponse("static/detection.html")      # 불성실 참여 탐지 (베타)

@app.get("/member")
def member_page():
    return FileResponse("static/member.html")         # 회원 히스토리 (공개)


app.mount("/static", StaticFiles(directory="static"), name="static")
