"""
일회성 복구 스크립트: 특정 회원의 최신 주차 레코드에 pending_weeks 주입.

벌금 미납자가 이월되지 않고 사라진 경우, 최신 주차 DB 레코드에 누락된
pending_weeks 를 강제로 보정한다. 실행 즉시 벌금 납부 섹션에 표시된다
(서버 응답 캐시 TTL 60초 이내).

사용법:
  # 후보 스캔 (변경 없음): score==0 인데 pending_weeks 비어있는 최신 주차 회원 목록
  python fix_pending.py --scan

  # 단일 회원 보정: 닉네임 + 미납 주차(들)
  python fix_pending.py "OrOI폰X" 20260507
  python fix_pending.py "OrOI폰X" 20260507 20260430   # 여러 주차 누적

옵션:
  --yes   확인 프롬프트 건너뛰기 (스크립트 자동화용)
"""
import sys
import unicodedata
import boto3
from boto3.dynamodb.conditions import Key

TABLE_NAME = "maple_guild"
AWS_REGION = "ap-northeast-1"


def _norm(s: str) -> str:
    return unicodedata.normalize("NFC", (s or "").strip())


def _get_latest_week(table) -> str:
    meta = table.get_item(Key={"week": "METADATA", "rank": 0}).get("Item")
    if not meta or "latest_week" not in meta:
        print("ERROR: METADATA 없음 — 업로드 먼저 필요", file=sys.stderr)
        sys.exit(1)
    return meta["latest_week"]


def _scan(table):
    latest = _get_latest_week(table)
    print(f"최신 주차: {latest}\n")
    resp = table.query(
        KeyConditionExpression=Key("week").eq(latest) & Key("rank").gt(0)
    )
    items = resp.get("Items", [])

    suspicious = []
    for i in items:
        if bool(i.get("left_guild")):
            continue
        if bool(i.get("is_ghost")):
            continue
        score = int(i.get("score", 0))
        pending = list(i.get("pending_weeks") or [])
        fine_count = int(i.get("fine_count", 0))
        last_fine_week = i.get("last_fine_week")
        # 의심 케이스: 이번 주 0점인데 pending 도 없고 이번 주 결제도 안 했음
        if score == 0 and not pending and last_fine_week != latest:
            suspicious.append({
                "rank": int(i["rank"]),
                "name": i.get("name"),
                "fine_count": fine_count,
                "last_fine_week": last_fine_week,
            })

    if not suspicious:
        print("의심 케이스 없음 — 모든 0점 회원이 pending_weeks 를 가짐.")
        return

    print(f"의심 케이스 {len(suspicious)}건 (score=0 이면서 pending_weeks 비어있음):\n")
    print(f"  {'rank':>5}  {'name':<20}  {'fine_count':>10}  {'last_fine_week':<14}")
    print(f"  {'-'*5}  {'-'*20}  {'-'*10}  {'-'*14}")
    for s in suspicious:
        last = s["last_fine_week"] or "-"
        print(f"  {s['rank']:>5}  {str(s['name']):<20}  {s['fine_count']:>10}  {last:<14}")
    print()
    print("→ 보정하려면:  python fix_pending.py \"<닉네임>\" <누락된주차>")


def _fix(table, target_name: str, missed_weeks: list[str], assume_yes: bool):
    latest = _get_latest_week(table)
    target_key = _norm(target_name)
    print(f"최신 주차: {latest}")
    print(f"대상 회원: {target_name!r} (NFC: {target_key!r})")
    print(f"주입할 미납 주차: {missed_weeks}\n")

    resp = table.query(
        KeyConditionExpression=Key("week").eq(latest) & Key("rank").gt(0)
    )
    candidates = [
        i for i in resp.get("Items", [])
        if _norm(str(i.get("name") or "")) == target_key
    ]

    if not candidates:
        print(f"ERROR: {latest} 주차에 '{target_name}' 없음 (NFC 매칭 실패)", file=sys.stderr)
        # 비슷한 이름 힌트
        names = sorted({str(i.get("name") or "") for i in resp.get("Items", [])})
        partial = [n for n in names if target_name[:2] in n or target_key[:2] in _norm(n)]
        if partial:
            print(f"  비슷한 닉네임 후보: {partial[:10]}", file=sys.stderr)
        sys.exit(1)
    if len(candidates) > 1:
        print(f"ERROR: 동명 {len(candidates)}명 발견 — 수동 처리 필요", file=sys.stderr)
        for c in candidates:
            print(f"  rank={int(c['rank'])}, name={c.get('name')!r}", file=sys.stderr)
        sys.exit(1)

    member = candidates[0]
    rank = int(member["rank"])
    current_pending = list(member.get("pending_weeks") or [])
    last_fine_week = member.get("last_fine_week")

    print("현재 상태:")
    print(f"  rank:           {rank}")
    print(f"  score:          {int(member.get('score', 0))}")
    print(f"  fine_count:     {int(member.get('fine_count', 0))}")
    print(f"  pending_weeks:  {current_pending}")
    print(f"  last_fine_week: {last_fine_week}")
    print(f"  left_guild:     {bool(member.get('left_guild'))}")
    print(f"  is_ghost:       {bool(member.get('is_ghost'))}\n")

    if last_fine_week == latest:
        print(f"WARN: last_fine_week == {latest} — 이미 이번 주 결제 처리됨.")
        print(f"      이대로 pending 만 추가하면 [납부확인] 버튼이 [취소]로 보이고 즉시 청구 불가.")
        print(f"      먼저 UI 에서 결제 취소 후 재실행을 권장.")
        if not assume_yes:
            ans = input("그래도 강행할까요? (yes/no): ").strip().lower()
            if ans != "yes":
                print("취소됨.")
                sys.exit(0)

    new_pending = sorted(set(current_pending) | set(missed_weeks))
    if new_pending == sorted(current_pending):
        print("변경 없음 — pending_weeks 가 이미 동일.")
        sys.exit(0)

    print("변경 예정:")
    print(f"  pending_weeks: {current_pending} → {new_pending}\n")

    if not assume_yes:
        ans = input("진행하시겠습니까? (yes/no): ").strip().lower()
        if ans != "yes":
            print("취소됨.")
            sys.exit(0)

    table.update_item(
        Key={"week": latest, "rank": rank},
        UpdateExpression="SET pending_weeks = :pw",
        ExpressionAttributeValues={":pw": new_pending},
    )
    print("OK 완료. 서버 응답 캐시 TTL 60초 이내에 벌금 목록에 표시됩니다.")


def main():
    args = sys.argv[1:]
    assume_yes = False
    if "--yes" in args:
        assume_yes = True
        args.remove("--yes")

    db = boto3.resource("dynamodb", region_name=AWS_REGION)
    table = db.Table(TABLE_NAME)

    if not args:
        print(__doc__)
        sys.exit(1)

    if args[0] == "--scan":
        _scan(table)
        return

    if len(args) < 2:
        print("ERROR: 닉네임 + 미납주차(들) 이 필요합니다.", file=sys.stderr)
        print("       예: python fix_pending.py \"OrOI폰X\" 20260507", file=sys.stderr)
        sys.exit(1)

    name = args[0]
    weeks = args[1:]
    # 주차 형식 간단 검증
    for w in weeks:
        if not (len(w) == 8 and w.isdigit()):
            print(f"ERROR: 주차 형식 오류 '{w}' — YYYYMMDD 8자리 숫자", file=sys.stderr)
            sys.exit(1)

    _fix(table, name, weeks, assume_yes)


if __name__ == "__main__":
    main()
