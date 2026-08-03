# Naby Suro

메이플스토리 나비 길드(오로라 서버)의 주간 지하수로 점수 관리 시스템.

인게임 캡처에서 점수를 뽑아 적재하는 **데이터 반입 파이프라인(MCP)** 과,
그 데이터를 통계·리더보드·벌금 관리로 보여주는 **웹 대시보드** 로 이루어진다.

운영 대시보드: https://cd2pr5qdmm.ap-northeast-1.awsapprunner.com

```
인게임 캡처 ──> [MCP 파이프라인] ──> DynamoDB ──> [웹 대시보드]
              판독·검증·적재                    통계·리더보드·벌금
```

## 구성

| | 위치 | 역할 |
|---|---|---|
| 웹 대시보드 | `main.py`, `static/` | 통계·리더보드·벌금 관리 (App Runner 배포) |
| 반입 파이프라인 | `mcp/` | 캡처 판독 → 검증 → DynamoDB 적재 (로컬 MCP 서버) |

두 축은 DynamoDB만 공유하고 코드는 독립적이다.

## 주요 기능

**웹 대시보드**

- **통계 분석** — 주차별 평균/표준편차/사분위수, 점수 분포, 추세 차트
- **리더보드** — 평균 등수 / 누적 미참 / 상승폭 / 하락폭 TOP 10 (현 길드원 한정)
- **벌금 관리** — 미참(0점) 회원 누진 벌금, 미해결 이월 자동 처리, 예외 처리·탈퇴 처리
- **불성실 참여 탐지 (베타)** — 길드 전체 변동을 상쇄하고 개인만 이탈한 경우를 탐지 (아래 참조)
- **보약 효과 감지** — 100명 이상 동시 하락 시 길드 baseline 갱신 알림
- **회원 히스토리** — 주차별 점수 추이 + NEXON Open API 캐릭터 정보
- **무기 마크** — 제네시스/데스티니 해방 무기 착용자 표시
- **관리자 인증** — 벌금·탈퇴·스펙다운 등 관리 작업은 로그인 필요, 처리자 기록

**반입 파이프라인** (`mcp/` — 자세한 내용은 [mcp/README.md](mcp/README.md))

- **명단 대조** — NEXON 길드 API를 철자 정답지로 삼아 판독 오류 교정
- **본캐 환산** — 부캐 접속 행을 본캐로 환산하고 직업을 직전 주차에서 백필
- **검증** — 신규/이탈 대조, 자릿수·급변 플래그로 확인 대상만 좁힘
- **안전장치** — 규칙으로 확정 못 하는 건 추측하지 않고 사람에게 확인, 승인 없이는 적재하지 않음

## 기술 스택

- **백엔드** — Python / FastAPI (uvicorn), boto3, httpx, openpyxl
- **프론트** — HTML/CSS/JS (프레임워크 없음), Chart.js
- **인프라** — DynamoDB, AWS App Runner (ECR), GitHub Actions
- **파이프라인** — MCP (Model Context Protocol) 서버
- **외부 API** — NEXON Open API (캐릭터 정보, 길드 명단)

## 데이터 모델

DynamoDB 단일 테이블 `maple_guild`. PK=`week`, SK=`rank`.

```
{
  "week": "YYYYMMDD",  "rank": 1,  "name": "닉네임",  "job": "직업",  "score": 0,
  "fine_count": 0,                    # 누적 벌금 횟수
  "last_fine_week": "YYYYMMDD",       # 마지막 부과 주차
  "pending_weeks": ["YYYYMMDD"],      # 미해결 부과 대기 (이월)
  "left_guild": false,                # 탈퇴 (벌금 섹션에서만 제외)
  "spec_down_from_week": "YYYYMMDD",  # 개별 baseline (스펙 다운 인정 시)
  "fine_exempt_weeks": ["YYYYMMDD"],  # 벌금 면제 주차
  "weapon_tier": "genesis",           # 무기 등급 (genesis/destiny)
  "processed_by": "관리자 닉네임"        # 벌금 작업 처리자 (감사)
}
```

`week="METADATA"` 레코드에 `latest_week`, `guild_baseline_from_week`, `dismissed_baseline_alert_week`를 저장한다.

## 핵심 로직

**이상치 탐지** — 각 주차 점수를 직전 최대 3주 참여 점수의 평균과 비교해, 10% 이상 하락하면 이상치로 판정한다. 전 기간 평균이 아닌 직전 추세를 기준으로 삼아, 점수가 꾸준히 오르는 성장형 회원의 초기 저점이 이상치로 오판되지 않는다. 하락만 이상치로 보고 상승은 정상으로 인정한다.

**baseline** — 회원별 `spec_down_from_week`와 길드 전체 `guild_baseline_from_week` 중 더 최근 시점을 기준으로 평균을 산출한다. 그 이전 데이터는 평균에서 제외되지만 히스토리 표시에는 남는다.

**누진 벌금** — N회차 부과액 = `10 + (N-1)` 솔 에르다 조각. 미해결 N건은 한 번의 클릭으로 일괄 처리한다.

## 불성실 참여 탐지 (베타)

`/detection` (관리자 전용). 본인 스펙에 비해 낮은 점수로 참여한 회원을 찾는다.

보약 효과 종료처럼 길드 전체가 함께 떨어지는 주차에는 본인 과거 대비 하락만 보는 방식이
대부분을 탐지해 구별력을 잃는다. 그래서 개인의 변화를 길드 전체 변화로 나누어,
혼자만 이탈한 부분만 본다.

```
환경 변동률 = median(전 회원의 이번주 점수 ÷ 직전 3주 추세)
개인 기대치 = 본인 추세 × 환경 변동률
이탈률     = (실제 − 기대치) ÷ 기대치      →  -15% 미만이면 탐지
```

임계값 `-15%`는 16주 잔차 분포에서 산출했다(MAD 기반 -2.9σ, 상위 1.75%).
전체가 -7.7% 하락한 주차에서 보정 전 59명이던 탐지가 보정 후 4명으로,
평상시와 같은 수준을 유지하는 것을 확인했다.

참여 이력 3주 미만, 0점 미참, 스펙 다운 인정 이전 데이터는 판정에서 제외한다.
제외는 개인이 아니라 상태로만 정의한다.

임계값은 고정하되 데이터 누적·정책 변경·대형 패치 시 재산정한다. 산출 근거와 버전별
변경 이력은 페이지의 패치 노트에 기록한다.

## 로컬 실행

```bash
pip install -r requirements.txt openpyxl

# .env
DYNAMODB_TABLE=maple_guild
AWS_REGION=ap-northeast-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
NEXON_API_KEY=...

uvicorn main:app --host 127.0.0.1 --port 8001 --reload
```

관리자 페이지를 로컬 HTTP에서 테스트하려면 `COOKIE_SECURE=false`로 실행한다.

## 주간 운영

주차 키는 정산 종료일인 **수요일 날짜**(YYYYMMDD)를 쓴다.

**파이프라인 사용** (권장) — Claude에 `mcp/` 커넥터를 연결한 뒤,
캡처 이미지와 주차를 주면 판독·검증·적재가 이어진다.

```
캡처 이미지 + 주차  →  판독  →  명단 대조·본캐 환산  →  검증
                    →  확인 필요 건만 보고  →  승인  →  적재
```

**수동 업로드** — xlsx가 이미 있는 경우

```bash
python upload_to_dynamodb.py guild_members_20260604_0610.xlsx 20260610
```

어느 경로든 적재 시 직전 주차의 벌금·이월·스펙다운·탈퇴 상태를 닉네임 기준으로
자동 승계하고, 이번 주 미참(0점) 회원에게 pending을 등록한다.
이후 관리 페이지에서 벌금 납부 확인, 예외 처리, 스펙 다운 인정 등을 수행한다.

## 배포

`main` 브랜치에 push하면 GitHub Actions가 Docker 이미지를 ECR에 올리고 App Runner 배포를 트리거한다.

## 페이지

| 경로 | 설명 | 인증 |
|---|---|---|
| `/` | 개요 (통계·차트·TOP 5·전체 길드원) | 공개 |
| `/member?name=<닉>` | 회원 히스토리 | 공개 |
| `/admin` | 관리자 개요 | 필요 |
| `/manage` | 벌금·스펙다운·탈퇴 등 관리 작업 | 필요 |
| `/detection` | 불성실 참여 탐지 (베타) | 필요 |

## 구조

```
main.py                 # FastAPI 백엔드
upload_to_dynamodb.py   # 주간 xlsx 업로드 + 상태 승계
static/                 # index / admin / management / detection / member / login
Dockerfile
.github/workflows/deploy.yml

mcp/                    # 데이터 반입 파이프라인 (MCP 서버)
├── server.py           #   툴 7종
├── naby_mcp/           #   명단 조회 · 규칙 엔진 · 검증 · 적재
└── skill/              #   이미지 판독 지침
```
