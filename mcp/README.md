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

## 다른 PC에서 사용하기

```bash
git clone https://github.com/oreorca0719/Naby_Suro.git
cd Naby_Suro/mcp
pip install -r requirements.txt
cp .env.example .env      # 값 입력
```

`.env`는 저장소에 포함되지 않으므로 clone 후 직접 만든다.
최소한 `NEXON_API_KEY`가 필요하다. AWS 자격증명은 `.env`에 넣거나
그 PC에 `aws configure`로 설정한다.

### 커넥터 등록

**Claude Desktop** — `claude_desktop_config.json`에 추가한다.

| OS | 설정 파일 위치 |
|---|---|
| Windows | `%APPDATA%\Claude\claude_desktop_config.json` |
| macOS | `~/Library/Application Support/Claude/claude_desktop_config.json` |

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

**Claude Code**

```bash
claude mcp add naby-suro -- python /path/to/Naby_Suro/mcp/server.py
```

등록 후 Claude를 완전히 종료했다가 다시 실행한다.

주의할 점:

- 경로는 그 PC 기준 절대경로로 바꾼다.
- `python`으로 실행되지 않으면 `python3` 또는 파이썬 실행 파일 전체 경로를 쓴다.
- PowerShell로 JSON을 편집하면 BOM이 붙어 파싱이 깨질 수 있다.
  에디터로 편집하거나 BOM 없는 UTF-8인지 확인한다.

### 판독 스킬 (선택)

`skill/` 에 이미지 판독 지침이 함께 들어 있다. 설치하면 매번 설명 없이
동일한 절차로 판독한다. 자세한 내용은 `skill/README.md` 참고.

```bash
cp -r skill ~/.claude/skills/guild-suro-tracker
```

설치하지 않아도 파이프라인은 동작한다.

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

## 구성

```
mcp/
├── server.py              # MCP 진입점 (툴 7종)
├── naby_mcp/
│   ├── config.py          # 환경변수 설정
│   ├── data/
│   │   └── normalize.json # 정규화·개명 표 (코드 밖 데이터)
│   └── tools/
│       ├── roster.py      # NEXON 길드 명단
│       ├── db.py          # DynamoDB 조회
│       ├── resolve.py     # 규칙 엔진
│       ├── validate.py    # 교차검증
│       ├── xlsx.py        # XLSX 생성
│       └── upload.py      # 적재 (승인 게이트)
├── skill/                 # 이미지 판독 지침 (선택 설치)
├── .env.example
└── requirements.txt
```

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

## 닉네임 오독 교정

판독 결과가 길드 명단에 없으면 오독으로 보고, 명단에서 가장 유사한 닉네임을 찾는다.
한글은 자모로 분해해 비교한다. 글자 단위로 재면 한 글자만 달라도 유사도가 급락해
실제 유사성이 반영되지 않기 때문이다.

충분히 유사하면 자동 교정하고, 애매하면 후보와 함께 확인을 요청한다.
길드 명단이 정답지 역할을 하므로 오독 목록을 따로 관리할 필요가 없고,
처음 보는 오독도 대응된다.

## 정규화 표 갱신

`naby_mcp/data/normalize.json`을 수정한다. 코드 변경은 필요 없다.

| 항목 | 용도 |
|---|---|
| `job_corrections` | 직업명 교정. 직업은 길드 명단에 없어 대조할 정답지가 없으므로 표로 관리한다. |
| `nickname_corrections` | 유사도 매칭이 듣지 않는 예외만 넣는다. 평소에는 비워둔다. |
| `renames` | 개명 회원을 옛 닉네임으로 기록해 히스토리를 잇는다. 비워두면 새 닉네임으로 적재되며, 관리 페이지의 닉네임 변경 기능으로 사후 통합할 수 있다. |

## 주차 키

정산 종료일인 **수요일 날짜(YYYYMMDD)** 를 쓴다. 예: `20260729`.
NEXON 명단 조회와 DynamoDB 적재 키가 이 값을 공유한다.
