<div align="center">

# 🦋 Naby Suro

### MapleStory · 오로라 서버 · 나비 길드 지하수로 대시보드

길드원의 주간 지하수로 점수를 추적하고, 통계 분석 · 미참 벌금 관리 · 불성실 참여 의심 감지를 자동화하는 운영 대시보드.

![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.115-009688?logo=fastapi&logoColor=white)
![DynamoDB](https://img.shields.io/badge/DynamoDB-AWS-4053D6?logo=amazondynamodb&logoColor=white)
![App Runner](https://img.shields.io/badge/AWS-App%20Runner-FF9900?logo=amazonaws&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)
![License](https://img.shields.io/badge/license-Private-lightgrey)

[**🌐 운영 대시보드 →**](https://cd2pr5qdmm.ap-northeast-1.awsapprunner.com)

</div>

---

## 📑 목차

- [한눈에 보기](#-한눈에-보기)
- [주요 기능](#-주요-기능)
- [기술 스택](#-기술-스택)
- [아키텍처](#-아키텍처)
- [데이터 모델](#-데이터-모델)
- [분석 알고리즘](#-분석-알고리즘)
- [페이지 구조](#-페이지-구조)
- [API 엔드포인트](#-api-엔드포인트)
- [개발 환경 세팅](#-개발-환경-세팅)
- [주간 운영 워크플로](#-주간-운영-워크플로)
- [배포](#-배포)
- [디렉토리 구조](#-디렉토리-구조)

---

## 🎯 한눈에 보기

매주 길드 마스터가 길드원 200명의 지하수로 점수를 엑셀로 추출 → 본 시스템에 업로드하면 자동으로:

| 기능 | 결과 |
|---|---|
| 📊 **통계 분석** | 평균/표준편차/사분위수 자동 산출, 주차별 추세 시각화 |
| 🏆 **리더보드** | 평균 등수, 누적 미참, 본인 평균 대비 상승폭/하락폭 TOP 5 |
| 🚨 **이상치 자동 탐지** | 본인 평균 대비 −10% 이상 하락한 회원을 의심 명단에 자동 등록 |
| 💸 **벌금 관리** | 0점 회원에게 누진 벌금 산식 적용 + 미해결 이월 자동 처리 |
| 🔬 **스펙 다운 인정** | 2주 연속 하락 패턴 감지 → 관리자 확인 시 평균 재산출 |
| 🩺 **보약 효과 감지** | 100명 이상 동시 감소 시 길드 전체 baseline 갱신 알림 |
| 📈 **회원별 히스토리** | 주차별 점수 추이 + NEXON Open API 연동으로 캐릭터 스펙 표시 |

---

## ✨ 주요 기능

### 🏆 리더보드 (TOP 5)

회원 본인의 historical 평균을 기준점으로 산출되는 4종 리더보드.

| 리더보드 | 정렬 기준 | 의미 |
|---|---|---|
| **수로 점수 평균 등수** | 모든 주차 rank 평균 (오름차순) | 길드 내 평균적으로 상위에 있는 회원 |
| **수로 미참 횟수** | 누적 `fine_count` 내림차순 | 벌금 누적 많은 회원 |
| **점수 상승폭** | (이번 주 점수 − 본인 평균) 내림차순 | 평소보다 잘한 회원 |
| **점수 하락폭** | (이번 주 점수 − 본인 평균) 오름차순 | 평소보다 못한 회원 |

> 모두 **현재 길드원 한정**. 이미 탈퇴한 회원은 자동 제외됩니다.

### 🚨 불성실 참여 의심 자동 탐지

각 회원의 점수 변동을 **Leave-One-Out 평균** 기준으로 분석하여, **이번 주 본인 평균 대비 -10% 이상 하락**한 회원을 자동으로 의심 명단에 등재.

**어뷰징 방지 메커니즘**:
- 이상치 주차의 점수는 회원 평균 계산에서 자동 제외
- "한 주 일부러 대충 해서 평균을 낮추고 다음 주에 큰 상승으로 보이게" 시도 차단

**스펙 다운 인정**:
- 이상치 주차가 **2주 이상 연속** 발생하면 "스펙 다운 추정" 라벨
- 관리자가 "스펙 다운 확인" 버튼 클릭 → 회원의 baseline 갱신
- 아이템 매각 등으로 정당히 점수 낮아진 경우 영구 의심 처리 방지

### 💸 벌금 관리 시스템

지하수로 미참(점수=0) 회원에게 누진 벌금 부과.

```
벌금 액수 N회차 = 솔 에르다 조각 (9 + N) 개
```

- 1회차: 10개
- 2회차: 11개
- N회차: (9 + N)개

**미해결 이월 처리**: 미참 후 부과 확인을 안 한 채로 다음 주가 되면, 그 회원은 다음 주 벌금 섹션에 계속 표시됩니다. 한 번의 "벌금 납부 확인" 클릭으로 미해결 N건 일괄 처리.

**길드 탈퇴 처리**: "길드 탈퇴" 버튼 클릭 시 해당 회원을 벌금 섹션에서만 영구 제외 (다른 화면 영향 없음).

### 🩺 보약 효과 종료 자동 감지

메이플스토리 보약 이벤트 종료 시 길드 전체 점수가 일제히 떨어지는 시즌 효과를 감지.

- **트리거**: 한 주차에 전주 대비 점수 감소 회원이 **100명 이상**
- **알림**: 메인 페이지 최상단에 배너 표시
- **확인 시**: 길드 전체의 평균 기준선(`guild_baseline_from_week`) 갱신 → 모든 회원의 평균이 그 주차 이후 데이터로 자동 재산출

### 📈 회원 히스토리 + 캐릭터 분석

각 회원의 상세 페이지에서:
- 주차별 점수 추이 (선형 차트)
- 사분위수 / 평균 / 표준편차
- 직업 / 레벨 / 전투력 / HEXA 코어 (NEXON Open API)
- **MapleScouter 환산 사이트** 외부 링크 (스펙 환산 교차 검증)
- 누적 납부 횟수 배지 (0회 초록 / 1회 이상 빨강)

---

## 🛠 기술 스택

### Backend
- **Python 3.12** + **FastAPI** (uvicorn ASGI server)
- **boto3** — AWS SDK (DynamoDB 연동)
- **httpx** — NEXON Open API 비동기 호출
- **openpyxl** — xlsx 파일 파싱

### Frontend
- **순수 HTML/CSS/JS** (프레임워크 없음)
- **Chart.js** — 시계열 차트, 분포 히스토그램
- **Pretendard** 한글 폰트 페어링 (옵션)

### Infrastructure
- **DynamoDB** — 회원 데이터 + 메타데이터 저장 (서버리스 NoSQL)
- **AWS App Runner** — 컨테이너 자동 배포
- **AWS ECR** — Docker 이미지 레지스트리
- **GitHub Actions** — CI/CD 파이프라인 (master 푸시 시 자동 배포)

### External Services
- **NEXON Open API** — 캐릭터 정보 조회 (basic, stat, hexamatrix)

---

## 🏗 아키텍처

```
┌─────────────────────────────────────────────────────────────────┐
│                       Browser (관리자)                            │
└─────────────────────────────────────────────────────────────────┘
                              │ HTTPS
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    AWS App Runner (Container)                    │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  FastAPI (main.py)                                        │   │
│  │  - /api/data, /api/history, /api/leaderboards            │   │
│  │  - /api/zero-score, /api/fine, /api/leave-guild          │   │
│  │  - /api/spec-down, /api/guild-baseline                   │   │
│  │  - /api/member/{name}, /api/member/{name}/profile        │   │
│  │  - StaticFiles → static/*.html                            │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
              │                              │
              ▼                              ▼
   ┌─────────────────────┐       ┌──────────────────────────┐
   │  AWS DynamoDB        │       │  NEXON Open API           │
   │  Table: maple_guild  │       │  - /character/id          │
   │  - 회원 주차별 점수    │       │  - /character/basic       │
   │  - 벌금/이상치 메타    │       │  - /character/stat        │
   │  - METADATA 레코드    │       │  - /character/hexamatrix  │
   └─────────────────────┘       └──────────────────────────┘
              ▲
              │ batch_writer
              │
   ┌─────────────────────┐
   │  매주 xlsx 업로드     │
   │  upload_to_dynamodb  │
   │  .py                 │
   └─────────────────────┘
```

---

## 🗃 데이터 모델

DynamoDB는 schema-less이지만, 실제 사용 중인 회원 레코드 구조:

```python
{
  "week":                 "20260513",    # PK (string)
  "rank":                 154,            # SK (int)
  "name":                 "훈플단",        # 닉네임
  "job":                  "팔라딘",
  "score":                43030,
  "fine_count":           2,              # 누적 벌금 횟수
  "last_fine_week":       "20260506",     # 마지막 부과 주차
  "pending_weeks":        ["20260506"],   # 미해결 부과 대기 주차 (이월)
  "left_guild":           False,          # 길드 탈퇴 (벌금 섹션 제외)
  "spec_down_from_week":  "20260513"      # 개별 baseline (스펙 다운 인정 시)
}
```

**METADATA 레코드** (week="METADATA", rank=0):
```python
{
  "latest_week":                     "20260513",
  "guild_baseline_from_week":        "20260513",   # 길드 전체 baseline (보약 효과 종료 시)
  "dismissed_baseline_alert_week":   "20260506"    # 알림 무시 처리 주차
}
```

---

## 🧮 분석 알고리즘

### Leave-One-Out 평균

각 주차 점수가 이상치인지 판정할 때, **그 주차를 제외한 나머지 평균**과 비교합니다.

```python
total = sum(participating_scores)
n = len(participating_scores)

for (week_i, score_i) in participating:
    loo_mean_i = (total − score_i) / (n − 1)
    if score_i < loo_mean_i × 0.9:    # -10% 이상 하락
        outlier_weeks.add(week_i)
```

**왜 LOO인가**: 자기 자신을 평균 계산에 포함하면 이상치가 평균을 끌어내려 판정이 둔감해지는 **자기 참조 문제**가 발생. LOO는 이 문제를 해결.

### 평균 산출 우선순위

회원의 effective baseline 결정:
```python
effective_from_week = max(
  member.spec_down_from_week,      # 개별 스펙 다운 baseline
  METADATA.guild_baseline_from_week # 길드 전체 baseline (보약 효과 종료)
)
```

→ 둘 중 더 최근 시점을 기준으로 평균 산출. 그 시점 이전 데이터는 평균에서 제외 (히스토리 표시는 유지).

### 단방향 이상치 처리 (비대칭)

- **하락 −10% 이상**: 이상치 → 평균 제외
- **상승 +N% (모든 범위)**: 정상 점수로 인정 → 평균 포함

> **근거**: 메이플 도메인에서 의도적으로 점수를 일시적으로 상승시키는 어뷰징 방법은 존재하지 않음. 따라서 상승 방향 이상치는 진정한 노력의 결과로 인정.

### 누진 벌금 산식

```python
# N회차 부과 시 금액
amount_N = 10 + (N − 1)

# 미해결 N건 일괄 처리 시 총액
total = Σ amount_k  for k in [current+1 .. current+N]
```

---

## 🖥 페이지 구조

### `/` — 개요 페이지
- 🔔 보약 효과 알림 배너 (조건부)
- 📊 통계 카드 (총 길드원 / 수로 참여자 / 총 점수)
- 📝 주간 브리핑 보드
- 📈 주간 길드 총점수 추이 차트
- 📉 점수 분포 히스토그램
- 🔬 길드원 점수 통계 분석 (Q1/Q2/Q3, 표준편차, 주간 통계 추이)

### `/static/management.html` — 관리 페이지
- 🏆 리더보드 4열 (평균 등수 / 미참 횟수 / 상승폭 / 하락폭)
- 🚨 불성실 참여 의심 (조건부, 스펙 다운 확인 버튼 포함)
- 💸 벌금 납부 섹션 (조건부)
- 👥 전체 길드원 현황 (검색 + 히스토리 진입)

### `/static/member.html?name=<닉네임>` — 회원 히스토리
- 캐릭터 정보 (NEXON API) + MapleScouter 환산 사이트 링크
- 주차별 점수 추이 차트
- 상세 기록 테이블 + 누적 납부 횟수 배지
- HEXA 코어 정보

---

## 🔌 API 엔드포인트

### 조회
| Method | Path | 설명 |
|---|---|---|
| `GET` | `/api/health` | 헬스 체크 |
| `GET` | `/api/data` | 최신 주차 통계 + 회원 목록 |
| `GET` | `/api/history` | 주차별 분위수/평균/표준편차 |
| `GET` | `/api/leaderboards` | 리더보드 4종 + 의심 명단 + 보약 알림 |
| `GET` | `/api/zero-score/{week}` | 0점 회원 + 이월 미해결 + 연속 미참 카운트 |
| `GET` | `/api/member/{name}` | 회원의 모든 주차 점수 |
| `GET` | `/api/member/{name}/profile` | NEXON API 캐릭터 정보 |
| `GET` | `/api/week/{week}` | 특정 주차 전체 회원 데이터 |

### 액션 (관리자용)
| Method | Path | 설명 |
|---|---|---|
| `POST` | `/api/fine/{week}/{name}` | 벌금 부과 (미해결 일괄 처리) |
| `DELETE` | `/api/fine/{week}/{name}` | 벌금 1회 취소 |
| `POST` | `/api/leave-guild/{name}` | 회원 길드 탈퇴 처리 |
| `POST` | `/api/spec-down/{name}` | 스펙 다운 인정 → baseline 갱신 |
| `POST` | `/api/guild-baseline` | 보약 효과 종료 인정 → 길드 baseline 갱신 |
| `POST` | `/api/dismiss-baseline-alert` | 보약 효과 알림 무시 |

---

## 🚀 개발 환경 세팅

### 사전 요구사항
- Python 3.12+
- AWS 자격 증명 (DynamoDB 접근)
- NEXON Open API Key (회원 프로필 조회용)

### 로컬 실행

```bash
# 1. 저장소 클론
git clone https://github.com/oreorca0719/Naby_Suro.git
cd Naby_Suro

# 2. 의존성 설치
pip install -r requirements.txt

# 3. .env 파일 작성
cat > .env <<EOF
DYNAMODB_TABLE=maple_guild
AWS_REGION=ap-northeast-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
NEXON_API_KEY=...
EOF

# 4. 서버 기동
uvicorn main:app --host 127.0.0.1 --port 8001 --reload

# 5. 브라우저 접속
open http://localhost:8001
```

---

## 📅 주간 운영 워크플로

```
┌─────────────────────────────────────────────────────────┐
│  1. 매주 길드 마스터가 인게임 길드 점수표를 xlsx로 추출   │
└─────────────────────────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  2. python upload_to_dynamodb.py <파일명> [yyyymmdd]    │
│     → 직전 주차 fine_count / pending_weeks /             │
│       spec_down_from_week / left_guild 자동 이월         │
│     → 새 주차 score=0 회원에게 pending_weeks 자동 등록   │
└─────────────────────────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  3. /api/data → /api/leaderboards 자동 갱신             │
│     → 보약 효과 종료 의심 시 메인 페이지에 알림 자동 표시 │
└─────────────────────────────────────────────────────────┘
                       ▼
┌─────────────────────────────────────────────────────────┐
│  4. 관리자는 [관리] 페이지에서:                          │
│     - 리더보드 / 의심 명단 확인                          │
│     - 벌금 납부 확인 / 취소                              │
│     - 스펙 다운 인정 (해당 시)                           │
│     - 보약 효과 종료 인정 (해당 시)                      │
│     - 길드 탈퇴 처리 (해당 시)                           │
└─────────────────────────────────────────────────────────┘
```

---

## ☁️ 배포

GitHub Actions가 `master` 브랜치 push 시 자동 배포.

```
[git push origin master]
       ▼
[GitHub Actions]
  - actions/checkout
  - AWS Credentials
  - Login to ECR
  - Build & tag Docker image
  - Push to ECR (latest + SHA tag)
  - aws apprunner start-deployment
       ▼
[AWS App Runner]
  - Pull new ECR image
  - Spin up new container
  - Health check
  - Traffic switch
       ▼
[Live: https://cd2pr5qdmm.ap-northeast-1.awsapprunner.com]
```

**총 소요 시간**: 약 3분 30초 (Actions 30초 + App Runner 3분)

---

## 📂 디렉토리 구조

```
Naby_Suro/
├── main.py                    # FastAPI 백엔드 (전체 API 로직)
├── upload_to_dynamodb.py      # 주간 xlsx → DynamoDB 업로드 + 이월 처리
├── create_table.py            # DynamoDB 테이블 초기 생성
├── seed_fine_counts.py        # 기존 누적 벌금 일괄 시드 (1회성)
├── requirements.txt
├── Dockerfile
├── static/
│   ├── index.html             # 개요 페이지
│   ├── management.html        # 관리 페이지 (리더보드 + 의심 + 벌금 + 길드원)
│   └── member.html            # 회원 히스토리 페이지
└── .github/
    └── workflows/
        └── deploy.yml         # CI/CD 파이프라인
```

---

<div align="center">

**🦋 Naby Suro** · 나비 길드 운영자를 위한 개인 도구

[![Made with FastAPI](https://img.shields.io/badge/Made%20with-FastAPI-009688?logo=fastapi)](https://fastapi.tiangolo.com/)
[![Powered by DynamoDB](https://img.shields.io/badge/Powered%20by-DynamoDB-4053D6?logo=amazondynamodb)](https://aws.amazon.com/dynamodb/)
[![Deployed on App Runner](https://img.shields.io/badge/Deployed%20on-App%20Runner-FF9900?logo=amazonaws)](https://aws.amazon.com/apprunner/)

</div>
