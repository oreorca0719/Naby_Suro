#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""나비 길드 지하수로 XLSX 생성기 + 교차검증기.

사용법:
  python generate_xlsx.py --data data.json --out out.xlsx [--week YYYYMMDD] [--last 지난주.xlsx]

data.json 형식:
  {"rows": [["닉네임","직업", 점수, "직위"], ...]}   # 순위는 정렬 후 자동 부여
  직위(4번째 값)는 0점 규칙용. 0점이 아닌 행은 생략해도 된다.

0점 규칙 (--week 가 ZERO_RULE_FROM 이상일 때):
  0점 + 직위 '나방'  -> 미참으로 포함
  0점 + 그 밖 직위   -> 제외 (XLSX 에 넣지 않음)
  0점 + 직위 없음    -> 중단 (직위를 확인해 넣을 것)

동작:
  - 지하수로(점수) 내림차순 정렬 → 순위/닉네임/직업/지하수로 4컬럼 XLSX
  - TOP3 금·은·동 음영, 헤더 고정, '맑은 고딕', 천단위 콤마
  - '워드브레이커'(오표기) 잔존 여부 자동 점검 → 있으면 경고
  - --last 지정 시 지난주 파일과 대조: 신규 / 이탈 / 중복 리포트
의존성: openpyxl  (pip install openpyxl --break-system-packages)
"""
import argparse, json, sys
from collections import Counter
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side

FONT = "맑은 고딕"
ZERO_RULE_FROM = "20261104"
MISS_RANK = "나방"


def apply_zero_rule(rows, week):
    """0점 규칙 적용. (남길 행[닉,직업,점수], 제외 행[(닉,직위)], 직위 누락[닉])"""
    keep, excluded, missing = [], [], []
    active = bool(week) and week >= ZERO_RULE_FROM
    for r in rows:
        nick, job, score = r[0], r[1], int(r[2])
        rank = (r[3] if len(r) > 3 else "") or ""
        if active and score == 0:
            if not rank.strip():
                missing.append(nick); continue
            if rank.strip() != MISS_RANK:
                excluded.append((nick, rank.strip())); continue
        keep.append([nick, job, score])
    return keep, excluded, missing


def build(rows, out):
    rows = sorted(rows, key=lambda r: -r[2])
    wb = Workbook(); ws = wb.active; ws.title = "지하수로"
    hf = PatternFill("solid", fgColor="2B2B2B")
    hfont = Font(name=FONT, bold=True, color="FFFFFF", size=11)
    gold = PatternFill("solid", fgColor="FFF3C4")
    silver = PatternFill("solid", fgColor="ECECEC")
    bronze = PatternFill("solid", fgColor="F6E0C8")
    thin = Side(style="thin", color="D9D9D9")
    bd = Border(left=thin, right=thin, top=thin, bottom=thin)
    C = Alignment(horizontal="center", vertical="center")
    L = Alignment(horizontal="left", vertical="center")
    R = Alignment(horizontal="right", vertical="center")

    ws.append(["순위", "닉네임", "직업", "지하수로"])
    for c in ws[1]:
        c.fill = hf; c.font = hfont; c.alignment = C; c.border = bd

    for i, (nick, job, score) in enumerate(rows, 1):
        ws.append([i, nick, job, int(score)])
        r = ws[i + 1]
        r[0].alignment = C; r[1].alignment = L; r[2].alignment = L
        r[3].alignment = R; r[3].number_format = "#,##0"
        for c in r:
            c.font = Font(name=FONT, size=11); c.border = bd
        if i == 1:
            for c in r: c.fill = gold; c.font = Font(name=FONT, size=11, bold=True)
        elif i == 2:
            for c in r: c.fill = silver
        elif i == 3:
            for c in r: c.fill = bronze

    ws.column_dimensions["A"].width = 7
    ws.column_dimensions["B"].width = 18
    ws.column_dimensions["C"].width = 18
    ws.column_dimensions["D"].width = 12
    ws.freeze_panes = "A2"
    wb.save(out)
    return rows


def load_names(xlsx):
    ws = load_workbook(xlsx).active
    return [r[1].value for r in ws.iter_rows(min_row=2)]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--last", default=None, help="직전 주차 xlsx (교차검증용)")
    ap.add_argument("--week", default=None, help="주차 키 YYYYMMDD (0점 규칙 적용 판단)")
    a = ap.parse_args()

    data = json.load(open(a.data, encoding="utf-8"))
    rows, excluded, missing = apply_zero_rule(data["rows"], a.week)
    if missing:
        print("⛔ 0점인데 직위가 없는 행:", missing)
        print("   직위를 확인해 4번째 값으로 넣은 뒤 다시 실행하세요. (파일 생성 안 함)")
        sys.exit(1)

    names = [r[0] for r in rows]
    dup = [k for k, v in Counter(names).items() if v > 1]
    bad_job = sorted({r[1] for r in rows if r[1] == "워드브레이커"})

    rows = build(rows, a.out)

    total = sum(r[2] for r in rows)
    zero = sum(1 for r in rows if r[2] == 0)
    print(f"총원: {len(rows)} | 0점: {zero} | 합계: {total:,}")
    print("TOP3:", [(rows[i][0], f"{rows[i][2]:,}") for i in range(min(3, len(rows)))])
    print("중복 닉:", dup if dup else "없음")
    if bad_job:
        print("⚠️  '워드브레이커' 오표기 잔존 → '윈드브레이커'로 고칠 것")
    if excluded:
        print(f"0점 제외({len(excluded)}명, 직위 {MISS_RANK} 아님):",
              ", ".join(f"{n}({k})" for n, k in excluded))
    print("저장:", a.out)

    if a.last:
        last = set(load_names(a.last)); cur = set(names)
        print("\n[교차검증 vs", a.last.split("/")[-1], "]")
        print("  신규(직전주 없음):", sorted(cur - last) or "없음")
        print("  이탈(이번주 없음):", sorted(last - cur) or "없음")


if __name__ == "__main__":
    main()
