# Naby Suro MCP

나비 길드 주간 지하수로 데이터 반입 파이프라인용 MCP 서버.

인게임 캡처 이미지에서 점수를 뽑아 DynamoDB에 넣기까지를, Claude(판독·판단)와
이 서버(정답지 조회·규칙 적용·적재)가 나눠 처리한다.

## 역할 분담

| | 담당 |
|---|---|
| 이미지 판독(OCR), 애매 건 판단, 관리자 보고 | Claude |
| 길드 명단 조회, 본캐 환산·정규화·중복 정리, 검증, 적재 | MCP 서버 |

규칙으로 확정할 수 없는 항목은 추측하지 않고 `ambiguous`로 올린다.
Claude가 풀지 못하면 관리자에게 확인받는다. 이 흐름이 잘못된 데이터의 유입을 막는다.

## 설치

```bash
cd mcp
pip install -r requirements.txt
cp .env.example .env      # 값 입력
```

`.env`에 최소한 `NEXON_API_KEY`가 필요하다. AWS 자격증명은 `.env` 또는
`aws configure`로 설정한다.

## Claude Desktop 연결

설정 파일(`claude_desktop_config.json`)에 추가한다.

```json
{
  "mcpServers": {
    "naby-suro": {
      "command": "python",
      "args": ["C:/path/to/Naby_Suro/mcp/server.py"]
    }
  }
}
```

Claude Code는 다음 명령으로 등록한다.

```bash
claude mcp add naby-suro -- python /path/to/Naby_Suro/mcp/server.py
```

## 제공 툴

| 툴 | 설명 |
|---|---|
| `fetch_roster(week)` | NEXON 길드 명단 — 닉네임 철자 정답지 |
| `get_previous_week(week)` | 직전 주차 확정 데이터 — 직업 백필·급변 판정 기준 |
| `resolve_rows(raw_rows, roster, prev_week_rows)` | 본캐 환산·정규화·백필·중복 정리 |
| `cross_validate(resolved, prev_week_rows, prev_week)` | 신규/이탈, 점수 이상 플래그 |
| `check_week_key(week)` | 주차 키 점검(수요일 여부·기존 데이터) |
| `generate_xlsx(rows, out_path)` | 업로드용 XLSX 생성 |
| `upload_week(xlsx_path, week, approved, overwrite)` | DynamoDB 적재 |

## 처리 흐름

```
관리자: 캡처 이미지 + 주차(수요일 날짜)
  ↓
Claude   이미지 판독 → raw rows (닉네임은 괄호 포함 원문 그대로)
MCP      fetch_roster / get_previous_week
MCP      resolve_rows        → 확정분 + 확인 필요분
Claude   확인 필요분 처리(크롭 재판독 또는 관리자 문의)
MCP      cross_validate      → 신규·이탈·점수 이상
Claude   "확정 N명 / 확인 M명" 보고
관리자:  승인
MCP      generate_xlsx → upload_week(approved=True)
```

## 규칙

**본캐 환산** — 인게임 표기 기준

| 표기 | 본캐 | 직업 |
|---|---|---|
| `A` | A | 화면 그대로 |
| `A(A)` | A | 화면 그대로 |
| `A(B)` | **B** | 직전 주차의 B 직업으로 백필 |

`A(..)`처럼 괄호 안이 잘린 경우는 확정하지 않고 명단 인접 후보와 함께 확인 요청한다.

**중복** — 길드 컨텐츠는 본캐 1개만 참여 가능하므로, 한 회원이 서로 다른 점수로
두 번 잡히면 정상 상황이 아니다. 임의 병합 없이 확인 요청한다.

**점수** — 외부 정답지가 없어 자동 검증이 불가능하다. 자릿수 이상과 직전 대비
급변만 플래그해 확인 대상을 좁히고, 최종 판단은 관리자가 한다.

## 정규화 표 갱신

`naby_mcp/data/normalize.json`을 수정한다. 코드 변경은 필요 없다.

```json
{
  "job_corrections":      { "워드브레이커": "윈드브레이커" },
  "nickname_corrections": { "츄주": "츄쭈" },
  "renames":              { "냠순": "미니냥이" }
}
```

`renames`는 개명 시 히스토리 연속성을 위해 옛 닉네임으로 기록하기 위한 것이다.

## 주차 키

정산 종료일인 **수요일 날짜(YYYYMMDD)** 를 쓴다. 예: `20260729`.
NEXON 명단 조회와 DynamoDB 적재 키가 이 값을 공유한다.
