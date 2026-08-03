#!/usr/bin/env python3
"""나비 길드 수로 파이프라인 MCP 서버.

역할 분담:
  Claude — 이미지 OCR(비결정론적 인지), 애매 건 판단, 관리자 보고
  MCP    — 정답지 조회, 규칙 적용(본캐 환산/정규화/백필/중복), 검증, 적재

무결성 원칙: 규칙으로 확정할 수 없는 것은 추측하지 않고 ambiguous 로 올린다.
Claude 가 못 풀면 관리자에게 묻는다. 이 defer 체인이 오염 유입을 막는다.

실행: python server.py   (stdio 전송)
"""
from __future__ import annotations

import json
from typing import Any

from mcp.server.fastmcp import FastMCP

from naby_mcp.tools import roster as roster_tool
from naby_mcp.tools import db as db_tool
from naby_mcp.tools import resolve as resolve_tool
from naby_mcp.tools import validate as validate_tool
from naby_mcp.tools import xlsx as xlsx_tool
from naby_mcp.tools import upload as upload_tool

mcp = FastMCP("naby-suro")


@mcp.tool()
def fetch_roster(week: str) -> dict:
    """해당 주차(정산 종료일=수요일)의 NEXON 길드원 명단을 가져온다.

    닉네임 철자의 정답지로 쓴다. 배열 순서는 소유자별 부캐 그룹이
    인접 배치되어 있어 본캐 추정의 힌트가 된다.

    Args:
        week: 주차 키 YYYYMMDD (예: "20260729")
    """
    return roster_tool.fetch_roster(week)


@mcp.tool()
def get_previous_week(week: str) -> dict:
    """주어진 주차의 직전 주차 확정 데이터를 가져온다.

    본캐 직업 백필과 점수 급변 판정의 기준으로 쓴다.

    Args:
        week: 이번 주차 키 YYYYMMDD
    """
    return db_tool.get_prev_week(week)


@mcp.tool()
def resolve_rows(
    raw_rows: list[dict],
    roster: list[str],
    prev_week_rows: list[dict] | None = None,
) -> dict:
    """OCR 원본 행을 규칙에 따라 확정 데이터로 변환한다.

    적용 규칙: 괄호 기반 본캐 환산, 철자/직업 정규화, 개명 반영,
    부캐 접속 시 직업 백필, 중복 병합.
    확신할 수 없는 건은 고치지 않고 ambiguous 로 반환한다.

    Args:
        raw_rows: [{"name": "A(B)", "job": "비숍", "score": 43030}, ...]
                  name 은 괄호를 포함한 화면 표기 그대로 넣을 것.
        roster:   fetch_roster 로 받은 members 배열
        prev_week_rows: get_previous_week 의 rows (직업 백필용)
    """
    return resolve_tool.resolve(raw_rows, roster, prev_week_rows)


@mcp.tool()
def cross_validate(
    resolved: list[dict],
    prev_week_rows: list[dict] | None = None,
    prev_week: str | None = None,
) -> dict:
    """확정 데이터를 직전 주차와 대조해 확인 권장 항목을 뽑는다.

    신규/이탈 명단과, 점수 자릿수 이상·직전 대비 급변 플래그를 낸다.
    자동으로 차단하거나 수정하지 않으며, 관리자의 확인 대상을 좁히는 용도다.
    """
    return validate_tool.cross_validate(resolved, prev_week_rows, prev_week)


@mcp.tool()
def check_week_key(week: str) -> dict:
    """적재 전 주차 키를 점검한다 (쓰기 없음).

    수요일 여부와 기존 데이터 존재 여부를 확인한다.
    """
    return upload_tool.check_week(week)


@mcp.tool()
def generate_xlsx(rows: list[dict], out_path: str) -> dict:
    """확정 데이터로 업로드용 XLSX 를 만든다.

    Args:
        rows: [{"name","job","score"}, ...]
        out_path: 저장 경로 (예: "guild_members_20260723_0729.xlsx")
    """
    return xlsx_tool.build_xlsx(rows, out_path)


@mcp.tool()
def upload_week(
    xlsx_path: str,
    week: str,
    approved: bool = False,
    overwrite: bool = False,
) -> dict:
    """검증·승인이 끝난 XLSX 를 DynamoDB 에 적재한다.

    벌금 이월·자리표시·재가입 처리가 자동 수행된다.
    approved=True 없이는 절대 쓰지 않는다.

    Args:
        xlsx_path: generate_xlsx 결과 파일
        week: 주차 키 YYYYMMDD (정산 종료일=수요일)
        approved: 관리자 승인 여부
        overwrite: 기존 주차 덮어쓰기 허용 여부
    """
    return upload_tool.upload_week(xlsx_path, week, approved, overwrite)


if __name__ == "__main__":
    mcp.run()
