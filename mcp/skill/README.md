# guild-suro-tracker 스킬

인게임 캡처에서 무엇을 어떻게 읽을지에 대한 판독 지침. MCP 커넥터와 짝을 이룬다.

| | 담당 |
|---|---|
| 이 스킬 | 이미지 판독 방법 — 컬럼 위치, 본캐 환산 규칙, 저해상도 확대 팁, 어조 |
| MCP 커넥터 | 명단 조회, 규칙 적용, 검증, 적재 |

## 설치

Claude 스킬 디렉토리에 이 폴더를 통째로 복사한다.

```bash
# Claude Code
cp -r mcp/skill ~/.claude/skills/guild-suro-tracker
```

Claude Desktop은 설정의 스킬 경로에 같은 방식으로 배치한다.
경로는 환경마다 다르므로 Claude 설정 화면에서 확인한다.

설치하지 않아도 파이프라인은 동작한다. 그 경우 `SKILL.md` 내용을
대화에 직접 붙여넣어 판독 지침으로 쓰면 된다.

## 구성

| 파일 | 내용 |
|---|---|
| `SKILL.md` | 발동 조건과 처리 절차 요약 |
| `references/pipeline.md` | 상세 규칙 — 컬럼 레이아웃, 본캐 환산, 정규화·개명 대장, 확대 팁 |
| `scripts/generate_xlsx.py` | 독립 실행용 XLSX 생성기 (커넥터의 `generate_xlsx` 툴과 동일 서식) |

## MCP 커넥터와의 관계

`scripts/generate_xlsx.py` 와 `references/pipeline.md` 의 정규화·개명 표는
커넥터에도 이식되어 있다(`naby_mcp/tools/xlsx.py`, `naby_mcp/data/normalize.json`).

커넥터를 쓰는 경우 정규화 표는 `naby_mcp/data/normalize.json` 을 갱신한다.
이 스킬의 마크다운 표는 사람이 읽는 참고용이다.
