"""SRT 자동 예매 루프.

사용 예:
    uv run python book.py --dep 수서 --arr 부산 --date 20260523 \
        --time-start 160000 --time-end 190000 --adults 2

매진 상태에서 취소표가 풀리는 순간 가장 빠른 시간의 열차를 자동 예매한다.
예약(결제 대기) 단계까지만 진행하고, 결제는 사용자가 SRT 앱/웹에서 직접 처리.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime

from dotenv import load_dotenv

from SRT import SRT
from SRT.errors import SRTError, SRTResponseError
from SRT.passenger import Adult
from SRT.seat_type import SeatType


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SRT auto-booking loop")
    p.add_argument("--dep", required=True, help="출발역 (예: 수서)")
    p.add_argument("--arr", required=True, help="도착역 (예: 부산)")
    p.add_argument("--date", required=True, help="출발일 YYYYMMDD (예: 20260523)")
    p.add_argument(
        "--time-start",
        default="000000",
        help="검색 시작 시각 HHMMSS (포함). 기본 000000",
    )
    p.add_argument(
        "--time-end",
        default="235959",
        help="검색 종료 시각 HHMMSS (포함). 기본 235959",
    )
    p.add_argument("--adults", type=int, default=1, help="성인 인원 (기본 1)")
    p.add_argument(
        "--interval",
        type=float,
        default=4.0,
        help="폴링 간격(초). 너무 짧으면 차단 위험. 기본 4초",
    )
    p.add_argument(
        "--standby",
        action="store_true",
        help="일반 예매 실패 시 예약대기까지 시도",
    )
    return p.parse_args()


def in_window(train, time_start: str, time_end: str) -> bool:
    return time_start <= train.dep_time <= time_end


def attempt_reserve(
    srt: SRT,
    train,
    passengers,
    standby: bool,
    seat_type=SeatType.GENERAL_FIRST,
):
    """일반 예매 → 실패 시 옵션에 따라 예약대기."""
    try:
        return srt.reserve(train, passengers=passengers, special_seat=seat_type), "reserve"
    except SRTResponseError as e:
        msg = str(e)
        # 좌석 매진/이미 끊긴 경우 등은 다음 후보로 넘어감
        if standby and train.reserve_standby_available():
            try:
                return (
                    srt.reserve_standby(
                        train,
                        passengers=passengers,
                        special_seat=SeatType.GENERAL_FIRST,
                    ),
                    "standby",
                )
            except SRTResponseError as e2:
                print(f"  standby 실패: {e2}", flush=True)
        else:
            # 디버깅용 1줄
            pass
        return None, msg


def main() -> int:
    args = parse_args()
    load_dotenv()
    srt_id = os.getenv("SRT_ID")
    srt_pw = os.getenv("SRT_PW")
    if not srt_id or not srt_pw:
        print("ERROR: .env 에 SRT_ID, SRT_PW 를 설정하세요.", file=sys.stderr)
        return 2

    passengers = [Adult(args.adults)] if args.adults else None

    print(
        f"[start] {args.dep}→{args.arr} {args.date} "
        f"{args.time_start[:2]}:{args.time_start[2:4]}~"
        f"{args.time_end[:2]}:{args.time_end[2:4]} "
        f"성인 {args.adults}명, 폴링 {args.interval}s"
    )

    srt = SRT(srt_id, srt_pw, verbose=False)
    print(f"[login] ok ({datetime.now():%H:%M:%S})", flush=True)

    attempt = 0
    while True:
        attempt += 1
        try:
            trains = srt.search_train(
                args.dep,
                args.arr,
                date=args.date,
                time=args.time_start,
                available_only=False,
            )
        except SRTError as e:
            print(f"[{attempt:>4}] search 실패: {e} — 재로그인 시도", flush=True)
            try:
                srt.login(srt_id, srt_pw)
            except SRTError as e2:
                print(f"  재로그인 실패: {e2}", flush=True)
            time.sleep(args.interval)
            continue

        # time-end 이내, 출발시간 빠른 순
        candidates = sorted(
            [t for t in trains if in_window(t, args.time_start, args.time_end)],
            key=lambda t: t.dep_time,
        )

        general_available = [t for t in candidates if t.general_seat_available()]
        special_only_available = [
            t for t in candidates
            if not t.general_seat_available() and t.special_seat_available()
        ]
        available = general_available + special_only_available
        standby_only = [
            t
            for t in candidates
            if not t.seat_available() and t.reserve_standby_available()
        ]

        if attempt % 15 == 1:
            print(
                f"[{attempt:>4} {datetime.now():%H:%M:%S}] "
                f"후보 {len(candidates)}개 / 잔여 {len(available)} / "
                f"대기가능 {len(standby_only)}",
                flush=True,
            )

        # 1순위: 일반석 가능 열차(빠른 순) → 2순위: 특실만 가능 열차(빠른 순)
        for t, seat_label, seat_type in [
            *[(t, "일반석", SeatType.GENERAL_ONLY) for t in general_available],
            *[(t, "특실", SeatType.SPECIAL_ONLY) for t in special_only_available],
        ]:
            print(
                f"  → {seat_label} 예매 시도 {t.dep_station_name}→{t.arr_station_name} "
                f"{t.dep_time[:2]}:{t.dep_time[2:4]} ({t.train_number})",
                flush=True,
            )
            r, info = attempt_reserve(
                srt, t, passengers, standby=False, seat_type=seat_type
            )
            if r is not None:
                print("\n[SUCCESS] 예매 완료")
                print(r)
                print(
                    "\n결제 마감 전에 SRT 앱/웹에서 결제 진행하세요. "
                    "예약번호를 확인해 마이페이지에서 결제 완료 처리."
                )
                return 0
            print(f"    실패: {info}", flush=True)

        # 2순위: 예약대기 (옵션)
        if args.standby and not available:
            for t in standby_only:
                print(
                    f"  → 예약대기 시도 {t.dep_time[:2]}:{t.dep_time[2:4]} "
                    f"({t.train_number})",
                    flush=True,
                )
                r, info = attempt_reserve(srt, t, passengers, standby=True)
                if r is not None:
                    print("\n[STANDBY] 예약대기 등록 완료")
                    print(r)
                    return 0
                print(f"    실패: {info}", flush=True)

        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\n[abort] 사용자 중단")
        sys.exit(130)
