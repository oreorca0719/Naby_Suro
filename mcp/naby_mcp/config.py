"""환경 설정 — 모든 값은 환경변수/.env 에서만 읽는다(하드코딩 금지).

이 MCP 서버는 어느 PC에서든 clone 후 .env 만 채우면 동작해야 하므로,
자격증명·길드명 등 환경 의존값을 여기 한 곳에 모은다.
"""
import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"


def _load_env_file(path: Path) -> None:
    """의존성 없이 .env 를 읽어 os.environ 에 주입 (기존 값 우선)."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


# mcp/.env → 리포 루트 .env 순으로 탐색
_load_env_file(BASE_DIR.parent / ".env")
_load_env_file(BASE_DIR.parent.parent / ".env")

# ── NEXON Open API ────────────────────────────────────────────────
NEXON_API_KEY = os.environ.get("NEXON_API_KEY", "")
NEXON_BASE = "https://open.api.nexon.com/maplestory/v1"

# ── 길드 식별 ─────────────────────────────────────────────────────
GUILD_NAME = os.environ.get("GUILD_NAME", "나비")
WORLD_NAME = os.environ.get("WORLD_NAME", "오로라")

# ── DynamoDB ──────────────────────────────────────────────────────
DYNAMODB_TABLE = os.environ.get("DYNAMODB_TABLE", "maple_guild")
AWS_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")

# ── 검증 임계값 ───────────────────────────────────────────────────
# 직전 주차 대비 이 비율 이상 변동하면 "확인 권장" 플래그.
# 자동 차단이 아니라 관리자의 시선을 유도하는 용도라 넉넉하게 잡는다.
SCORE_CHANGE_FLAG_RATIO = float(os.environ.get("SCORE_CHANGE_FLAG_RATIO", "0.5"))
# 지하수로 점수의 현실적 범위 — 벗어나면 OCR 자릿수 오인식 의심
SCORE_MIN_PLAUSIBLE = int(os.environ.get("SCORE_MIN_PLAUSIBLE", "5000"))
SCORE_MAX_PLAUSIBLE = int(os.environ.get("SCORE_MAX_PLAUSIBLE", "500000"))


def require_nexon_key() -> str:
    if not NEXON_API_KEY:
        raise RuntimeError(
            "NEXON_API_KEY 가 설정되지 않았습니다. mcp/.env 에 NEXON_API_KEY=... 를 넣으세요."
        )
    return NEXON_API_KEY
