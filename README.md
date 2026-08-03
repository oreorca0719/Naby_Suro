# Naby Suro

메이플스토리 나비 길드(오로라 서버)의 주간 지하수로 점수 관리 대시보드.

매주 길드원 점수표(xlsx)를 업로드하면 통계·리더보드·벌금 관리·이상치 탐지를 자동으로 처리한다.

운영 대시보드: https://cd2pr5qdmm.ap-northeast-1.awsapprunner.com

## 주요 기능

- **통계 분석** — 주차별 평균/표준편차/사분위수, 점수 분포, 추세 차트
- **리더보드** — 평균 등수 / 누적 미참 / 상승폭 / 하락폭 TOP 10 (현 길드원 한정)
- **벌금 관리** — 미참(0점) 회원 누진 벌금, 미해결 이월 자동 처리, 예외 처리·탈퇴 처리
- **이상치 탐지** — 직전 3주 추세 대비 급락 회원 감지, 2주 연속 시 스펙 다운 인정
- **보약 효과 감지** — 100명 이상 동시 하락 시 길드 baseline 갱신 알림
- **회원 히스토리** — 주차별 점수 추이 + NEXON Open API 캐릭터 정보
- **무기 마크** — 제네시스/데스티니 해방 무기 착용자 표시
- **관리자 인증** — 벌금·탈퇴·스펙다운 등 관리 작업은 로그인 필요, 처리자 기록

## 기술 스택

- **백엔드** — Python / FastAPI (uvicorn), boto3, httpx, openpyxl
- **프론트** — HTML/CSS/JS (프레임워크 없음), Chart.js
- **인프라** — DynamoDB, AWS App Runner (ECR), GitHub Actions
- **외부 API** — NEXON Open API (캐릭터 정보)

## 데이터 모델

DynamoDB 단일 테이블 `maple_guild`. PK=`week`, SK=`rank`.

```
{
  "week": "20260610", "rank": 154, "name": "훈플단", "job": "팔라딘", "score": 43030,
  "fine_count": 2,                    # 누적 벌금 횟수
  "last_fine_week": "20260603",       # 마지막 부과 주차
  "pending_weeks": ["20260603"],      # 미해결 부과 대기 (이월)
  "left_guild": false,                # 탈퇴 (벌금 섹션에서만 제외)
  "spec_down_from_week": "20260610",  # 개별 baseline (스펙 다운 인정 시)
  "fine_exempt_weeks": ["20260603"],  # 벌금 면제 주차
  "weapon_tier": "genesis",           # 무기 등급 (genesis/destiny)
  "processed_by": "볼땡글"             # 벌금 작업 처리자 (감사)
}
```

`week="METADATA"` 레코드에 `latest_week`, `guild_baseline_from_week`, `dismissed_baseline_alert_week`를 저장한다.

## 핵심 로직

**이상치 탐지** — 각 주차 점수를 직전 최대 3주 참여 점수의 평균과 비교해, 10% 이상 하락하면 이상치로 판정한다. 전 기간 평균이 아닌 직전 추세를 기준으로 삼아, 점수가 꾸준히 오르는 성장형 회원의 초기 저점이 이상치로 오판되지 않는다. 하락만 이상치로 보고 상승은 정상으로 인정한다.

**baseline** — 회원별 `spec_down_from_week`와 길드 전체 `guild_baseline_from_week` 중 더 최근 시점을 기준으로 평균을 산출한다. 그 이전 데이터는 평균에서 제외되지만 히스토리 표시에는 남는다.

**누진 벌금** — N회차 부과액 = `10 + (N-1)` 솔 에르다 조각. 미해결 N건은 한 번의 클릭으로 일괄 처리한다.

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

```bash
python upload_to_dynamodb.py guild_members_20260604_0610.xlsx 20260610
```

업로드 시 직전 주차의 벌금·이월·스펙다운·탈퇴 상태를 닉네임 기준으로 자동 승계하고, 이번 주 미참(0점) 회원에게 pending을 등록한다. 이후 관리 페이지에서 벌금 납부 확인, 예외 처리, 스펙 다운 인정 등을 수행한다.

## 배포

`main` 브랜치에 push하면 GitHub Actions가 Docker 이미지를 ECR에 올리고 App Runner 배포를 트리거한다.

## 페이지

| 경로 | 설명 | 인증 |
|---|---|---|
| `/` | 개요 (통계·차트·TOP 5·전체 길드원) | 공개 |
| `/member?name=<닉>` | 회원 히스토리 | 공개 |
| `/admin` | 관리자 개요 | 필요 |
| `/manage` | 벌금·스펙다운·탈퇴 등 관리 작업 | 필요 |

## 구조

```
main.py                 # FastAPI 백엔드
upload_to_dynamodb.py   # 주간 xlsx 업로드 + 상태 승계
requirements.txt
Dockerfile
static/                 # index / admin / management / member / login
.github/workflows/deploy.yml
```
