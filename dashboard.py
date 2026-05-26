"""SRT 왕복 자동 예매 + 로컬 대시보드 + 이메일 알림.

실행:
    uv run python dashboard.py
    → http://127.0.0.1:8765

ENV (.env):
    SRT_ID, SRT_PW                 (필수)
    DASH_PORT=8765
    SRT_INTERVAL=4                 폴링 간격(초)
    SRT_STANDBY=0                  1이면 일반 예매 안 잡힐 때 예약대기까지

    # 알림 (쉼표로 여러 수신자 지정 가능)
    NOTIFY_EMAIL=eksska12@naver.com
    SMTP_HOST=smtp.gmail.com
    SMTP_PORT=587
    SMTP_USER=<발신용 Gmail>
    SMTP_PASSWORD=<Gmail 앱 비밀번호>   (https://myaccount.google.com/apppasswords)
    SMTP_FROM=<발신주소, 보통 SMTP_USER>
"""

from __future__ import annotations

import json
import logging
import os
import smtplib
import threading
import time
from collections import deque
from datetime import datetime, timedelta
from email.message import EmailMessage
from pathlib import Path
from typing import Any
from uuid import uuid4

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse

from SRT import SRT
from SRT.errors import SRTError, SRTLoginError, SRTResponseError
from SRT.passenger import Adult
from SRT.seat_type import SeatType


load_dotenv()
logging.basicConfig(level=logging.WARNING)

# 사용자가 UI에서 추가/삭제하는 감시(task) 목록 영속 파일.
# 첫 실행 시 SEED_WATCHES 가 자동 시드됨.
WATCHES_PATH = Path(os.getenv("SRT_WATCHES_PATH", "watches.json"))

# 수서 출발 운임표 (SRT 운임요금표 2026-05-15 기준, 단위: 원, 성인 1인)
# 키는 SRT API 표기. 사용자는 수서 출발만 사용.
SRT_FARES_FROM_SUSEO: dict[str, dict[str, int]] = {
    "동탄":       {"general":  7500, "special": 10900},
    "평택지제":   {"general":  7700, "special": 11200},
    "천안아산":   {"general": 11300, "special": 16400},
    "오송":       {"general": 15400, "special": 22300},
    "대전":       {"general": 20100, "special": 29100},
    "김천(구미)": {"general": 30300, "special": 43900},
    "서대구":     {"general": 36400, "special": 52800},
    "동대구":     {"general": 37400, "special": 54200},
    "경주":       {"general": 42700, "special": 61900},
    "울산(통도사)": {"general": 46800, "special": 67900},
    "부산":       {"general": 52600, "special": 76300},
    "공주":       {"general": 21600, "special": 31300},
    "익산":       {"general": 28000, "special": 40600},
    "정읍":       {"general": 33900, "special": 49200},
    "광주송정":   {"general": 40700, "special": 59000},
    "나주":       {"general": 42100, "special": 61000},
    "목포":       {"general": 46500, "special": 67400},
    "전주":       {"general": 30300, "special": 43900},
    "남원":       {"general": 35200, "special": 51000},
    "곡성":       {"general": 36700, "special": 53200},
    "구례구":     {"general": 38400, "special": 55700},
    "순천":       {"general": 41000, "special": 59500},
    "여천":       {"general": 43100, "special": 62500},
    "여수EXPO":   {"general": 43800, "special": 63500},
    "포항":       {"general": 47200, "special": 68400},
    "밀양":       {"general": 42300, "special": 61300},
    "진영":       {"general": 44400, "special": 64400},
    "창원중앙":   {"general": 45600, "special": 66100},
    "창원":       {"general": 46500, "special": 67400},
    "마산":       {"general": 46900, "special": 68000},
    "진주":       {"general": 51100, "special": 74100},
}

# SRT 정차역 (srtrain.constants.STATION_CODE 기준)
SRT_STATIONS: list[str] = [
    "수서", "동탄", "평택지제",
    "천안아산", "오송", "대전", "김천(구미)", "서대구", "동대구",
    "경주", "울산(통도사)", "부산",
    "공주", "익산", "정읍", "광주송정", "나주", "목포",
    "전주", "남원", "곡성", "구례구", "순천", "여천", "여수EXPO",
    "마산", "창원", "진영", "진주", "밀양",
]

SEED_WATCHES: list[dict[str, Any]] = [
    {
        "label": "하행 (수서 → 부산)",
        "dep": "수서",
        "arr": "부산",
        "date": "20260523",
        "time_start": "140000",
        "time_end": "170000",
        "adults": 2,
    },
    {
        "label": "상행 (부산 → 수서)",
        "dep": "부산",
        "arr": "수서",
        "date": "20260525",
        "time_start": "140000",
        "time_end": "190000",
        "adults": 2,
    },
]

INTERVAL = float(os.getenv("SRT_INTERVAL", "4"))
STANDBY = os.getenv("SRT_STANDBY", "0") == "1"
PORT = int(os.getenv("DASH_PORT", "8765"))
HOST = os.getenv("DASH_HOST", "127.0.0.1")
# SRT_AUTO_BOOKING=0 이면 SRT 로그인/자동 예매 워커를 시작하지 않음 (조회 전용 모드).
AUTO_BOOKING = os.getenv("SRT_AUTO_BOOKING", "1") == "1"
# 결제 확인 동작: 예매 후 60초부터 시작 → 30초 간격으로 SRT 실제 결제 마감 + grace까지 체크
PAYMENT_CHECK_START_SECONDS = int(os.getenv("SRT_PAYMENT_CHECK_START_SECONDS", "60"))
PAYMENT_CHECK_RETRY_SECONDS = int(os.getenv("SRT_PAYMENT_CHECK_RETRY_SECONDS", "30"))
# SRT 결제 마감(보통 예매 후 20분) 이후 추가로 N초 동안 한 번 더 확인
PAYMENT_DEADLINE_GRACE_SECONDS = int(os.getenv("SRT_PAYMENT_DEADLINE_GRACE_SECONDS", "120"))
# 결제 마감 파싱 실패시 하드 캡 (분 단위 X 60 = 초)
PAYMENT_CHECK_HARD_LIMIT_SECONDS = int(os.getenv("SRT_PAYMENT_CHECK_HARD_LIMIT_SECONDS", "1800"))
# 결제 마감 이후 자동 재예매 방지 - 사용자가 명시적으로 reset 호출해야 다시 감시
BOOKING_RETRY_AFTER_SECONDS = int(os.getenv("SRT_BOOKING_RETRY_AFTER_SECONDS", "1800"))
# 기존 예약 idempotency 캐시 TTL
RESERVATION_CACHE_TTL_SECONDS = int(os.getenv("SRT_RESERVATION_CACHE_TTL", "30"))
# 출발일이 오늘이면 검색 시작 시각 = max(설정값, 현재 + N분)
SAME_DAY_BUFFER_MINUTES = int(os.getenv("SRT_SAME_DAY_BUFFER_MINUTES", "10"))
TIME_OPTIONS_PATH = Path(os.getenv("SRT_TIME_OPTIONS_PATH", "time_options.json"))
SETTINGS_PATH = Path(os.getenv("SRT_SETTINGS_PATH", "settings.json"))
TIME_OPTIONS_ROUTE = {"dep": "수서", "arr": "부산"}


def _empty_time_options() -> dict[str, Any]:
    return {
        "schema_version": 1,
        "enabled": False,
        "route": dict(TIME_OPTIONS_ROUTE),
        "windows": [],
    }


def _normalize_time(raw: Any) -> str:
    value = str(raw).strip().replace(":", "")
    if len(value) == 4:
        value += "00"
    if len(value) != 6 or not value.isdigit():
        raise ValueError("time must be HHMM, HH:MM, or HHMMSS")
    hour = int(value[:2])
    minute = int(value[2:4])
    second = int(value[4:6])
    if hour > 23 or minute > 59 or second > 59:
        raise ValueError("time is out of range")
    return value


def _normalize_date(raw: Any) -> str:
    value = str(raw).strip().replace("-", "")
    try:
        datetime.strptime(value, "%Y%m%d")
    except ValueError as exc:
        raise ValueError("date must be YYYYMMDD or YYYY-MM-DD") from exc
    return value


def normalize_time_window(raw: dict[str, Any]) -> dict[str, Any]:
    date = _normalize_date(raw.get("date"))
    time_start = _normalize_time(raw.get("time_start"))
    time_end = _normalize_time(raw.get("time_end"))
    if time_start > time_end:
        raise ValueError("time_start must be earlier than or equal to time_end")

    label = str(raw.get("label") or "").strip()
    if not label:
        label = f"{date} {time_start[:2]}:{time_start[2:4]}~{time_end[:2]}:{time_end[2:4]}"

    return {
        "id": str(raw.get("id") or uuid4().hex[:8]),
        "label": label,
        "date": date,
        "time_start": time_start,
        "time_end": time_end,
        "active": bool(raw.get("active", True)),
    }


def load_time_options() -> dict[str, Any]:
    if not TIME_OPTIONS_PATH.exists():
        return _empty_time_options()

    with TIME_OPTIONS_PATH.open("r", encoding="utf-8") as f:
        raw = json.load(f)

    config = _empty_time_options()
    config["enabled"] = bool(raw.get("enabled", False))
    config["windows"] = [
        normalize_time_window(w)
        for w in raw.get("windows", [])
        if isinstance(w, dict)
    ]
    return config


def save_time_options(config: dict[str, Any]) -> None:
    TIME_OPTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
    TIME_OPTIONS_PATH.write_text(
        json.dumps(config, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def leg_route(cfg: dict[str, Any]) -> dict[str, str]:
    return {
        "dep": str(cfg.get("dep") or TIME_OPTIONS_ROUTE["dep"]),
        "arr": str(cfg.get("arr") or TIME_OPTIONS_ROUTE["arr"]),
    }


def booking_window_open_at(date_yyyymmdd: str) -> datetime:
    """SRT 예매 오픈 시각: 출발일 정확히 1개월 전 07:00 KST.

    같은 일자가 전월에 없으면 (예: 3/31 → 2/28) 가장 가까운 말일로 조정.
    """
    dep = datetime.strptime(date_yyyymmdd, "%Y%m%d")
    year, month = dep.year, dep.month
    if month == 1:
        prev_year, prev_month = year - 1, 12
    else:
        prev_year, prev_month = year, month - 1
    day = dep.day
    while day >= 1:
        try:
            return datetime(prev_year, prev_month, day, 7, 0, 0)
        except ValueError:
            day -= 1
    raise ValueError(f"cannot compute booking window for {date_yyyymmdd}")


def _normalize_adults(raw: Any) -> int:
    try:
        n = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("adults must be a positive integer") from exc
    if n < 1 or n > 9:
        raise ValueError("adults must be between 1 and 9")
    return n


def _normalize_station(raw: Any, field: str) -> str:
    value = str(raw or "").strip()
    if not value:
        raise ValueError(f"{field} is required")
    if value not in SRT_STATIONS:
        raise ValueError(f"{field} '{value}' is not an SRT station")
    return value


def normalize_watch(
    raw: dict[str, Any],
    *,
    existing_id: str | None = None,
) -> dict[str, Any]:
    """폼/요청에서 받은 watch 항목을 검증하고 정규화."""
    dep = _normalize_station(raw.get("dep"), "dep")
    arr = _normalize_station(raw.get("arr"), "arr")
    if dep == arr:
        raise ValueError("dep and arr must be different")

    date = _normalize_date(raw.get("date"))
    time_start = _normalize_time(raw.get("time_start"))
    time_end = _normalize_time(raw.get("time_end"))
    if time_start > time_end:
        raise ValueError("time_start must be earlier than or equal to time_end")

    adults = _normalize_adults(raw.get("adults", 1))

    label = str(raw.get("label") or "").strip() or (
        f"{dep} → {arr} {date[:4]}-{date[4:6]}-{date[6:8]} "
        f"{time_start[:2]}:{time_start[2:4]}~{time_end[:2]}:{time_end[2:4]}"
    )

    active = bool(raw.get("active", True))
    created_at = str(raw.get("created_at") or datetime.now().isoformat(timespec="seconds"))
    result = raw.get("result") if isinstance(raw.get("result"), dict) else None

    # 좌석 전략:
    #  - "together_only" : Adult(N) 한 번에만 시도 (= 사실상 연석 보장)
    #  - "split_ok"      : Adult(N) 실패시 Adult(1) × N 분할 폴백 (기본, 2인 이상에서만 의미)
    raw_strategy = str(raw.get("seat_strategy") or "").strip().lower()
    if raw_strategy not in {"together_only", "split_ok"}:
        raw_strategy = "split_ok"
    seat_strategy = raw_strategy if adults >= 2 else "together_only"

    return {
        "id": existing_id or str(raw.get("id") or uuid4().hex[:8]),
        "label": label,
        "dep": dep,
        "arr": arr,
        "date": date,
        "time_start": time_start,
        "time_end": time_end,
        "adults": adults,
        "seat_strategy": seat_strategy,
        "active": active,
        "created_at": created_at,
        "result": result,
    }


def _seed_watches_payload() -> list[dict[str, Any]]:
    return [normalize_watch(dict(w)) for w in SEED_WATCHES]


def load_watches() -> list[dict[str, Any]]:
    """디스크에서 watches 목록을 읽어 정규화. 없으면 seed 생성."""
    if not WATCHES_PATH.exists():
        watches = _seed_watches_payload()
        save_watches(watches)
        return watches

    try:
        with WATCHES_PATH.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _seed_watches_payload()

    items = raw.get("watches", []) if isinstance(raw, dict) else []
    out: list[dict[str, Any]] = []
    for entry in items:
        if not isinstance(entry, dict):
            continue
        try:
            out.append(normalize_watch(entry, existing_id=entry.get("id")))
        except ValueError:
            continue
    return out


def _atomic_write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def save_watches(watches: list[dict[str, Any]]) -> None:
    payload = {"schema_version": 1, "watches": watches}
    _atomic_write_json(WATCHES_PATH, payload)


def find_watch(watches: list[dict[str, Any]], watch_id: str) -> dict[str, Any] | None:
    for w in watches:
        if w.get("id") == watch_id:
            return w
    return None


def retry_at_from(captured_at: datetime) -> datetime:
    return captured_at + timedelta(seconds=BOOKING_RETRY_AFTER_SECONDS)


def payment_check_start_at_from(captured_at: datetime) -> datetime:
    return captured_at + timedelta(seconds=PAYMENT_CHECK_START_SECONDS)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _parse_payment_deadline(reserve_obj) -> datetime | None:
    """예약 객체의 payment_date / payment_time (YYYYMMDD / HHMMSS) → datetime."""
    pd = str(getattr(reserve_obj, "payment_date", "") or "").strip()
    pt = str(getattr(reserve_obj, "payment_time", "") or "").strip()
    if len(pd) != 8 or len(pt) < 4:
        return None
    pt = (pt + "00")[:6]
    try:
        return datetime.strptime(pd + pt, "%Y%m%d%H%M%S")
    except ValueError:
        return None


def _serialize_reservation(reserve_obj, train_dict: dict[str, Any]) -> dict[str, Any]:
    """한 건의 예약을 result.reservations 항목 형태로 직렬화."""
    return {
        "reservation_number": getattr(reserve_obj, "reservation_number", None),
        "summary": str(reserve_obj),
        "payment_deadline": (
            f"{getattr(reserve_obj, 'payment_date', '')} "
            f"{getattr(reserve_obj, 'payment_time', '')}"
        ).strip(),
        "paid": bool(getattr(reserve_obj, "paid", False)),
        "train": train_dict,
    }


def reservation_result(
    result_type: str,
    reserve_objs: list,
    trains: list,
    captured_at: datetime,
) -> dict[str, Any]:
    """예매 결과를 result dict 로 직렬화. 분할 예매 지원 (reservations 리스트).

    reserve_objs / trains 는 1개 이상의 예약. 첫 항목이 대표.
    """
    if not isinstance(reserve_objs, list):
        reserve_objs = [reserve_objs]
    if not isinstance(trains, list):
        trains = [trains]

    serialized = [
        _serialize_reservation(r, serialize_train(t) if not isinstance(t, dict) else t)
        for r, t in zip(reserve_objs, trains)
    ]

    primary = serialized[0]
    primary_obj = reserve_objs[0]

    # 결제 마감 시각: 첫 예약의 마감 또는 캡처 + hard limit 중 빠른 쪽
    deadline_dt = _parse_payment_deadline(primary_obj)
    if deadline_dt is None:
        deadline_dt = captured_at + timedelta(seconds=PAYMENT_CHECK_HARD_LIMIT_SECONDS)
    final_check_at = deadline_dt + timedelta(seconds=PAYMENT_DEADLINE_GRACE_SECONDS)

    next_check_at = captured_at + timedelta(seconds=PAYMENT_CHECK_START_SECONDS)

    return {
        "type": result_type,
        "summary": primary["summary"],
        "reservation_number": primary["reservation_number"],
        "payment_deadline": primary["payment_deadline"],
        "paid": all(r["paid"] for r in serialized),
        "train": primary["train"],
        "captured_at": captured_at.isoformat(timespec="seconds"),
        "payment_status": "waiting",
        "payment_check_attempts": 0,
        "payment_check_start_at": next_check_at.isoformat(timespec="seconds"),
        "next_payment_check_at": next_check_at.isoformat(timespec="seconds"),
        # 결제 확인 종료 시각(SRT 실제 마감 + grace). 이 시각 지나면 expired 처리.
        "payment_check_until": final_check_at.isoformat(timespec="seconds"),
        "retry_after_at": (captured_at + timedelta(seconds=BOOKING_RETRY_AFTER_SECONDS)).isoformat(timespec="seconds"),
        "reservations": serialized,
        "split": len(serialized) > 1,
    }


# === 기존 예약 조회 캐시 (idempotency 가드용) ===
_RESERVATION_CACHE: dict[str, Any] = {"at": None, "data": None}


def get_reservations_cached(srt: SRT, force: bool = False) -> list:
    """SRT 전체 예약 목록을 캐싱. 기본 TTL=30초."""
    now = datetime.now()
    at = _RESERVATION_CACHE.get("at")
    if not force and at and (now - at).total_seconds() < RESERVATION_CACHE_TTL_SECONDS:
        return _RESERVATION_CACHE.get("data") or []
    try:
        data = srt.get_reservations()
    except SRTError:
        return _RESERVATION_CACHE.get("data") or []
    _RESERVATION_CACHE["at"] = now
    _RESERVATION_CACHE["data"] = data
    return data


def invalidate_reservation_cache() -> None:
    _RESERVATION_CACHE["at"] = None
    _RESERVATION_CACHE["data"] = None


def reservation_matches_cfg(r, cfg: dict[str, Any]) -> bool:
    """예약 r 이 watch cfg(dep/arr/date/time_start~time_end) 와 매칭되는지."""
    route = leg_route(cfg)
    try:
        if r.dep_station_name != route["dep"]:
            return False
        if r.arr_station_name != route["arr"]:
            return False
        if r.dep_date != cfg["date"]:
            return False
        # 출발 시각이 윈도우 안에 있는지
        if not (cfg["time_start"] <= r.dep_time <= cfg["time_end"]):
            return False
    except AttributeError:
        return False
    return True


def find_matching_reservations(srt: SRT, cfg: dict[str, Any]) -> list:
    """해당 watch 설정과 매칭되는 기존 예약(결제 여부 무관) 리스트."""
    return [r for r in get_reservations_cached(srt) if reservation_matches_cfg(r, cfg)]


def restore_result_from_reservations(reservations: list, captured_at: datetime | None = None) -> dict[str, Any]:
    """기존 예약 리스트 → result dict (idempotency 복원용)."""
    captured_at = captured_at or datetime.now()
    return reservation_result(
        "reserve",
        list(reservations),
        [serialize_train_from_reservation(r) for r in reservations],
        captured_at,
    )


def serialize_train_from_reservation(r) -> dict[str, Any]:
    """SRTReservation → train_dict (검색 결과 train과 호환)."""
    return {
        "train_number": getattr(r, "train_number", None),
        "dep_time": f"{r.dep_time[:2]}:{r.dep_time[2:4]}",
        "arr_time": f"{r.arr_time[:2]}:{r.arr_time[2:4]}",
        "dep": r.dep_station_name,
        "arr": r.arr_station_name,
        "general": "예약됨",
        "special": "-",
        "seat_available": False,
        "standby_available": False,
    }


def _parse_emails(raw: str) -> list[str]:
    return [e.strip() for e in raw.split(",") if e.strip()]


def _default_settings() -> dict[str, Any]:
    env_emails = _parse_emails(os.getenv("NOTIFY_EMAIL", ""))
    return {"schema_version": 1, "notify_emails": env_emails}


def load_settings() -> dict[str, Any]:
    if not SETTINGS_PATH.exists():
        s = _default_settings()
        save_settings(s)
        return s
    try:
        with SETTINGS_PATH.open("r", encoding="utf-8") as f:
            raw = json.load(f)
    except (OSError, json.JSONDecodeError):
        return _default_settings()
    return {
        "schema_version": 1,
        "notify_emails": [e for e in (raw.get("notify_emails") or []) if isinstance(e, str) and "@" in e],
    }


def save_settings(s: dict[str, Any]) -> None:
    _atomic_write_json(SETTINGS_PATH, s)


def get_notify_emails() -> list[str]:
    return list(load_settings().get("notify_emails") or [])


class LegState:
    def __init__(self, name: str, cfg: dict[str, Any]) -> None:
        self.name = name
        self.cfg = cfg
        self.attempts = 0
        self.last_poll_at: str | None = None
        self.last_search_ok = False
        self.candidates: list[dict[str, Any]] = []
        self.available_count = 0
        self.standby_count = 0
        self.result: dict[str, Any] | None = None
        self.awaiting_window_open_at: str | None = None


class State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started_at = datetime.now().isoformat(timespec="seconds")
        self.worker_status = "starting"   # starting / running / done / error / stopped
        self.login_status = "pending"
        self.last_error: str | None = None
        self.email_status = "not-sent"
        self.legs: dict[str, LegState] = {}
        self.logs: deque[str] = deque(maxlen=300)
        # 최초 1회 디스크에서 동기화
        self.sync_legs(load_watches())

    def log(self, msg: str) -> None:
        line = f"{datetime.now():%H:%M:%S} {msg}"
        with self.lock:
            self.logs.appendleft(line)
        print(line, flush=True)

    def sync_legs(self, watches: list[dict[str, Any]]) -> None:
        """디스크 watches 목록과 메모리 legs 를 동기화.

        - 신규 id: LegState 생성 (디스크에 저장된 result 가 있으면 복원)
        - 기존 id: cfg 만 갱신 (런타임 통계는 유지)
        - 사라진 id: 제거
        """
        with self.lock:
            existing_ids = set(self.legs.keys())
            incoming_ids = {w["id"] for w in watches}

            for wid in existing_ids - incoming_ids:
                del self.legs[wid]

            for w in watches:
                wid = w["id"]
                if wid in self.legs:
                    self.legs[wid].cfg = w
                else:
                    leg = LegState(wid, w)
                    if isinstance(w.get("result"), dict):
                        leg.result = w["result"]
                    self.legs[wid] = leg

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "started_at": self.started_at,
                "worker_status": self.worker_status,
                "login_status": self.login_status,
                "last_error": self.last_error,
                "email_status": self.email_status,
                "notify_emails": get_notify_emails(),
                "interval": INTERVAL,
                "standby": STANDBY,
                "stations": SRT_STATIONS,
                "fares_from_suseo": SRT_FARES_FROM_SUSEO,
                "legs": {
                    wid: {
                        "id": wid,
                        "label": leg.cfg["label"],
                        "dep": leg_route(leg.cfg)["dep"],
                        "arr": leg_route(leg.cfg)["arr"],
                        "date": leg.cfg["date"],
                        "time_start": leg.cfg["time_start"],
                        "time_end": leg.cfg["time_end"],
                        "adults": leg.cfg["adults"],
                        "seat_strategy": leg.cfg.get("seat_strategy", "together_only" if leg.cfg.get("adults", 1) < 2 else "split_ok"),
                        "active": bool(leg.cfg.get("active", True)),
                        "created_at": leg.cfg.get("created_at"),
                        "attempts": leg.attempts,
                        "last_poll_at": leg.last_poll_at,
                        "last_search_ok": leg.last_search_ok,
                        "candidates": list(leg.candidates),
                        "available_count": leg.available_count,
                        "standby_count": leg.standby_count,
                        "result": leg.result,
                        "awaiting_window_open_at": leg.awaiting_window_open_at,
                        "fare": SRT_FARES_FROM_SUSEO.get(leg_route(leg.cfg)["arr"]) if leg_route(leg.cfg)["dep"] == "수서" else None,
                    }
                    for wid, leg in self.legs.items()
                },
                "logs": list(self.logs),
            }


STATE = State()
STOP = threading.Event()


def in_window(train, cfg) -> bool:
    return cfg["time_start"] <= train.dep_time <= cfg["time_end"]


def serialize_train(t) -> dict[str, Any]:
    return {
        "train_number": t.train_number,
        "dep_time": f"{t.dep_time[:2]}:{t.dep_time[2:4]}",
        "arr_time": f"{t.arr_time[:2]}:{t.arr_time[2:4]}",
        "dep": t.dep_station_name,
        "arr": t.arr_station_name,
        "general": t.general_seat_state,
        "special": t.special_seat_state,
        "seat_available": t.seat_available(),
        "standby_available": t.reserve_standby_available(),
    }


def send_email(subject: str, body: str) -> tuple[bool, str]:
    user = os.getenv("SMTP_USER")
    pw = os.getenv("SMTP_PASSWORD")
    host = os.getenv("SMTP_HOST", "smtp.gmail.com")
    port = int(os.getenv("SMTP_PORT", "587"))
    sender = os.getenv("SMTP_FROM") or user
    notify_emails = get_notify_emails()
    if not user or not pw:
        return False, "SMTP_USER/SMTP_PASSWORD 미설정"
    if not sender:
        return False, "SMTP_FROM 또는 SMTP_USER 없음"
    if not notify_emails:
        return False, "알림 이메일 미설정 (대시보드에서 추가)"
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(notify_emails)
    msg.set_content(body)
    try:
        with smtplib.SMTP(host, port, timeout=15) as s:
            s.ehlo()
            s.starttls()
            s.login(user, pw)
            s.send_message(msg, to_addrs=notify_emails)
        return True, f"sent → {len(notify_emails)}명"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def notify_booking(leg: LegState) -> None:
    cfg = leg.cfg
    route = leg_route(cfg)
    result = leg.result or {}
    reservations = result.get("reservations") or []
    is_partial = bool(result.get("partial"))
    is_split = bool(result.get("split"))

    head = "예매 완료"
    if is_partial:
        head = f"부분 예매({len(reservations)}/{result.get('requested_adults', cfg['adults'])}좌석)"
    elif is_split:
        head = f"분할 예매({len(reservations)}건)"

    subject = f"[SRT] {cfg['label']} {head} — 결제 필요"

    res_blocks = []
    for idx, r in enumerate(reservations, 1):
        res_blocks.append(
            f"예약 #{idx}\n"
            f"  예약번호    : {r.get('reservation_number')}\n"
            f"  열차/시각   : {r.get('train', {}).get('train_number', '')} "
            f"{r.get('train', {}).get('dep_time', '')} → {r.get('train', {}).get('arr_time', '')}\n"
            f"  결제 마감   : {r.get('payment_deadline', '-')}\n"
            f"  내역       : {r.get('summary', '')}"
        )

    body = (
        f"SRT 자동 예매 결과입니다. 결제 마감 전 SRT 앱에서 결제하세요.\n\n"
        f"구간       : {route['dep']} → {route['arr']}\n"
        f"일자       : {cfg['date']}\n"
        f"인원       : 성인 {cfg['adults']}명\n"
        f"좌석 전략  : {cfg.get('seat_strategy', '-')}\n"
        f"잡힌 시각  : {datetime.now():%Y-%m-%d %H:%M:%S}\n\n"
        + ("\n\n".join(res_blocks) if res_blocks else "(예약 상세 없음)")
        + f"\n\nSRT 앱: https://etk.srail.kr/cmc/01/selectLoginForm.do\n"
    )
    ok, info = send_email(subject, body)
    with STATE.lock:
        STATE.email_status = (
            f"{datetime.now():%H:%M:%S} {leg.name}: " + ("OK" if ok else f"FAIL ({info})")
        )
    STATE.log(
        f"email → {', '.join(get_notify_emails())}: "
        f"{'OK' if ok else 'FAIL ' + info}"
    )


def _effective_search_start(cfg: dict[str, Any]) -> str:
    """출발일이 오늘이면 (현재 + 버퍼) 이후만 검색 - 이미 출발한 열차 제외."""
    today = datetime.now().strftime("%Y%m%d")
    if cfg.get("date") != today:
        return cfg["time_start"]
    threshold = (datetime.now() + timedelta(minutes=SAME_DAY_BUFFER_MINUTES)).strftime("%H%M%S")
    return max(cfg["time_start"], threshold)


def _attempt_split_booking(
    srt: SRT, train, adults: int, seat_type
) -> tuple[list, str | None]:
    """Adult(1) × adults 회 분할 예매. 같은 열차에서 1좌석씩.

    성공한 예약 리스트와 실패시 마지막 에러 메시지를 반환.
    부분 성공이면 (1건 리스트, 에러 메시지) 형태.
    """
    succeeded: list = []
    last_error: str | None = None
    for i in range(adults):
        try:
            r = srt.reserve(train, passengers=[Adult(1)], special_seat=seat_type)
            succeeded.append(r)
        except SRTResponseError as e:
            last_error = str(e)
            break
    return succeeded, last_error


def try_leg(srt: SRT, leg: LegState) -> bool:
    """Returns True 이면 leg 종료(예매 성공/기존 예약 복원)."""
    cfg = leg.cfg
    route = leg_route(cfg)
    leg.attempts += 1

    # ===== 0) Idempotency 가드 — 같은 구간/날짜/시간대에 이미 예약 있나? =====
    try:
        existing = find_matching_reservations(srt, cfg)
    except SRTError as e:
        existing = []
        STATE.log(f"[{leg.name}] 기존 예약 조회 실패: {e}")
    if existing:
        with STATE.lock:
            leg.result = restore_result_from_reservations(existing)
            leg.candidates = [serialize_train_from_reservation(r) for r in existing]
            leg.available_count = 0
            leg.standby_count = 0
        STATE.log(
            f"[{leg.name}] 기존 예약 {len(existing)}건 감지 → 재예매 중단 "
            f"(이미 {'결제됨' if leg.result['paid'] else '결제 대기 중'})"
        )
        return True

    # ===== 1) 검색 (당일자 시간 필터 적용) =====
    search_start = _effective_search_start(cfg)
    try:
        trains = srt.search_train(
            route["dep"], route["arr"],
            date=cfg["date"], time=search_start,
            available_only=False,
        )
        leg.last_search_ok = True
    except SRTError as e:
        leg.last_search_ok = False
        STATE.last_error = f"{leg.name} search: {e}"
        STATE.log(f"[{leg.name}] search 실패: {e}")
        return False

    leg.last_poll_at = datetime.now().isoformat(timespec="seconds")
    # 윈도우 필터링 (당일 보정된 시작시각 기준)
    effective_cfg = dict(cfg, time_start=search_start)
    candidates = sorted(
        [t for t in trains if in_window(t, effective_cfg)], key=lambda t: t.dep_time
    )
    general_available = [t for t in candidates if t.general_seat_available()]
    special_only_available = [
        t for t in candidates
        if not t.general_seat_available() and t.special_seat_available()
    ]
    available = general_available + special_only_available
    standby_only = [
        t for t in candidates
        if not t.seat_available() and t.reserve_standby_available()
    ]
    with STATE.lock:
        leg.candidates = [serialize_train(t) for t in candidates]
        leg.available_count = len(available)
        leg.standby_count = len(standby_only)

    if leg.attempts % 15 == 1:
        STATE.log(
            f"[{leg.name}] 후보 {len(candidates)} / 잔여 {len(available)} / "
            f"대기가능 {len(standby_only)}"
        )

    adults = cfg["adults"]
    strategy = cfg.get("seat_strategy", "together_only" if adults < 2 else "split_ok")
    passengers = [Adult(adults)]

    for t, seat_label, seat_type in [
        *[(t, "일반석", SeatType.GENERAL_ONLY) for t in general_available],
        *[(t, "특실", SeatType.SPECIAL_ONLY) for t in special_only_available],
    ]:
        STATE.log(
            f"[{leg.name}] {seat_label} 예매 시도 {t.dep_time[:2]}:{t.dep_time[2:4]} "
            f"{t.train_number} ({adults}인 / {strategy})"
        )
        # 1차 - Adult(N) 한 번에 (연석 우선 / 기본)
        try:
            r = srt.reserve(t, passengers=passengers, special_seat=seat_type)
            with STATE.lock:
                leg.result = reservation_result("reserve", [r], [t], datetime.now())
            invalidate_reservation_cache()
            STATE.log(f"[{leg.name}] [SUCCESS] 예매 완료 (단일/연석)")
            notify_booking(leg)
            return True
        except SRTResponseError as e:
            STATE.log(f"[{leg.name}]   {adults}인 일괄 실패: {e}")

        # 2차 - split_ok 이고 2인 이상이면 1좌석씩 분할 시도
        if strategy == "split_ok" and adults >= 2:
            STATE.log(f"[{leg.name}] {adults}인 분할 예매 시도 (같은 열차 {t.train_number})")
            succeeded, err = _attempt_split_booking(srt, t, adults, seat_type)
            if len(succeeded) == adults:
                with STATE.lock:
                    leg.result = reservation_result(
                        "reserve", succeeded, [t] * adults, datetime.now()
                    )
                invalidate_reservation_cache()
                STATE.log(f"[{leg.name}] [SUCCESS] 분할 예매 완료 ({adults}건)")
                notify_booking(leg)
                return True
            if succeeded:
                # 부분 성공 → 부분 예매도 확정 처리 (마감 시간 안에 user가 결정)
                with STATE.lock:
                    leg.result = reservation_result(
                        "reserve", succeeded, [t] * len(succeeded), datetime.now()
                    )
                    leg.result["partial"] = True
                    leg.result["requested_adults"] = adults
                invalidate_reservation_cache()
                STATE.log(
                    f"[{leg.name}] [PARTIAL] {len(succeeded)}/{adults}좌석만 확보 "
                    f"({err}). 다음 폴링에서 추가 시도 안 함."
                )
                notify_booking(leg)
                return True
            STATE.log(f"[{leg.name}]   분할 1좌석도 실패: {err}")

    if STANDBY and not available:
        for t in standby_only:
            STATE.log(
                f"[{leg.name}] 예약대기 시도 {t.dep_time[:2]}:{t.dep_time[2:4]} "
                f"{t.train_number}"
            )
            try:
                r = srt.reserve_standby(
                    t, passengers=passengers, special_seat=SeatType.GENERAL_FIRST
                )
                with STATE.lock:
                    leg.result = reservation_result("standby", [r], [t], datetime.now())
                invalidate_reservation_cache()
                STATE.log(f"[{leg.name}] [STANDBY] 예약대기 등록")
                notify_booking(leg)
                return True
            except SRTResponseError as e:
                STATE.log(f"[{leg.name}]   대기 실패: {e}")

    return False


def paid_reservation_numbers(srt: SRT) -> set[str]:
    """캐시된 예약 목록에서 결제 완료된 예약번호만 추출."""
    return {
        str(r.reservation_number)
        for r in get_reservations_cached(srt)
        if getattr(r, "reservation_number", None) and getattr(r, "paid", False)
    }


def all_reservation_numbers(srt: SRT) -> set[str]:
    """캐시된 예약 목록의 모든 예약번호."""
    return {
        str(r.reservation_number)
        for r in get_reservations_cached(srt)
        if getattr(r, "reservation_number", None)
    }


def refresh_booking_results(srt: SRT) -> None:
    """예매된 leg 들의 결제/만료 상태를 갱신.

    데드라인 기반:
    - 예매 후 PAYMENT_CHECK_START_SECONDS 부터 PAYMENT_CHECK_RETRY_SECONDS 간격으로 체크
    - SRT 결제 마감(payment_check_until) 이후로는 expired 처리, 자동 재예매 안 함
    """
    now = datetime.now()
    due_legs: list[LegState] = []
    with STATE.lock:
        for leg in STATE.legs.values():
            result = leg.result
            if not result or result.get("type") not in {"reserve", "standby"}:
                continue
            if result.get("paid"):
                continue
            if result.get("payment_status") == "expired":
                continue

            next_check = parse_iso_datetime(result.get("next_payment_check_at"))
            if next_check is None:
                captured_at = parse_iso_datetime(result.get("captured_at")) or now
                next_check = captured_at + timedelta(seconds=PAYMENT_CHECK_START_SECONDS)
                result["next_payment_check_at"] = next_check.isoformat(timespec="seconds")

            if now >= next_check:
                due_legs.append(leg)

    if not due_legs:
        return

    try:
        paid_numbers = paid_reservation_numbers(srt)
        live_numbers = all_reservation_numbers(srt)
    except SRTError as e:
        paid_numbers = None
        live_numbers = None
        STATE.last_error = f"payment check: {e}"
        STATE.log(f"결제 확인 실패: {e} — 재확인 예정")

    for leg in due_legs:
        with STATE.lock:
            result = leg.result
            if not result:
                continue

            reservations = result.get("reservations") or []
            attempt_count = int(result.get("payment_check_attempts") or 0) + 1
            result["payment_check_attempts"] = attempt_count
            result["payment_checked_at"] = now.isoformat(timespec="seconds")

            # 각 sub-reservation 별 paid 갱신
            paid_count = 0
            live_count = 0
            if paid_numbers is not None:
                for r in reservations:
                    rn = str(r.get("reservation_number") or "")
                    if rn and rn in paid_numbers:
                        r["paid"] = True
                        paid_count += 1
                    if rn and live_numbers and rn in live_numbers:
                        live_count += 1

            all_paid = bool(reservations) and all(r.get("paid") for r in reservations)
            result["paid"] = all_paid

            check_until = parse_iso_datetime(result.get("payment_check_until"))
            if check_until is None:
                captured_at = parse_iso_datetime(result.get("captured_at")) or now
                check_until = captured_at + timedelta(seconds=PAYMENT_CHECK_HARD_LIMIT_SECONDS)

            if all_paid:
                result["payment_status"] = "paid"
                STATE.log(f"[{leg.name}] 결제 완료 확인 ({paid_count}/{len(reservations)}건) — 감시 유지")
                continue

            if now >= check_until:
                # 결제 마감 + grace 지났는데 미결제 → expired (자동 재예매 하지 않음)
                if paid_count > 0:
                    result["payment_status"] = "partially_paid_expired"
                    STATE.log(
                        f"[{leg.name}] 결제 마감 — 일부 결제 ({paid_count}/{len(reservations)}). "
                        f"자동 재예매 안 함, 필요시 '결과 초기화'."
                    )
                elif live_numbers is not None and live_count == 0:
                    # 예약 자체가 사라짐(취소 등) → 마감으로 처리 후 자동 재감시 가능
                    result["payment_status"] = "expired"
                    STATE.log(
                        f"[{leg.name}] 결제 마감 + 예약 없음 — expired 처리, "
                        f"필요시 '결과 초기화'로 재감시 시작."
                    )
                else:
                    result["payment_status"] = "expired"
                    STATE.log(
                        f"[{leg.name}] 결제 마감 — 미결제로 expired 처리, "
                        f"자동 재예매 안 함. 필요시 '결과 초기화'."
                    )
                continue

            # 아직 마감 전 — 다음 체크 시각 잡기
            next_check = min(
                now + timedelta(seconds=PAYMENT_CHECK_RETRY_SECONDS),
                check_until,
            )
            result["payment_status"] = "waiting"
            result["next_payment_check_at"] = next_check.isoformat(timespec="seconds")
            STATE.log(
                f"[{leg.name}] 결제 미확인 ({paid_count}/{len(reservations)}건). "
                f"다음 확인 {next_check:%H:%M:%S}"
            )


def sync_results_to_disk() -> None:
    """메모리상 leg.result 변화를 watches.json 에 일괄 반영."""
    watches = load_watches()
    changed = False
    with STATE.lock:
        for w in watches:
            leg = STATE.legs.get(w["id"])
            if leg is None:
                continue
            if w.get("result") != leg.result:
                w["result"] = leg.result
                changed = True
    if changed:
        try:
            save_watches(watches)
        except OSError as exc:
            STATE.last_error = f"persist watches: {exc}"


def leg_is_final(leg: LegState) -> bool:
    """leg 가 더 이상 자동 폴링/예매 대상이 아님."""
    if not leg.result:
        return False
    result = leg.result
    if result.get("type") == "standby":
        return True
    if result.get("paid"):
        return True
    if result.get("payment_status") in {"expired", "partially_paid_expired"}:
        return True
    return False


def worker_loop() -> None:
    srt_id = os.getenv("SRT_ID")
    srt_pw = os.getenv("SRT_PW")
    if not srt_id or not srt_pw:
        STATE.worker_status = "error"
        STATE.last_error = ".env SRT_ID/SRT_PW 누락"
        STATE.log("ENV missing — 워커 중단")
        return

    STATE.log(f"login attempt ({srt_id})")
    try:
        srt = SRT(srt_id, srt_pw, verbose=False)
    except SRTLoginError as e:
        STATE.worker_status = "error"
        STATE.login_status = f"fail: {e}"
        STATE.last_error = str(e)
        STATE.log(f"login fail: {e} — 워커 중단 (잠금 회피)")
        return
    except SRTError as e:
        STATE.worker_status = "error"
        STATE.login_status = f"error: {e}"
        STATE.last_error = str(e)
        STATE.log(f"login error: {e}")
        return

    STATE.login_status = "ok"
    STATE.worker_status = "running"
    STATE.log("login ok — 왕복 폴링 시작")

    # ENV 진단 메시지 (메일 발송 가능 여부)
    if not os.getenv("SMTP_USER") or not os.getenv("SMTP_PASSWORD"):
        STATE.log(
            "주의: SMTP_USER/SMTP_PASSWORD 미설정 — 부킹 성공해도 이메일 미발송. "
            "Gmail 앱 비밀번호를 .env 에 추가하세요."
        )

    while not STOP.is_set():
        # 매 사이클마다 디스크 → 메모리 동기화 (UI 에서 추가/삭제 반영)
        STATE.sync_legs(load_watches())
        refresh_booking_results(srt)
        sync_results_to_disk()

        # 예매 오픈 대기 체크
        now = datetime.now()
        with STATE.lock:
            for leg in STATE.legs.values():
                open_at = booking_window_open_at(leg.cfg["date"])
                if open_at > now:
                    leg.awaiting_window_open_at = open_at.isoformat(timespec="seconds")
                else:
                    leg.awaiting_window_open_at = None

        all_legs = list(STATE.legs.values())
        active_legs = [leg for leg in all_legs if leg.cfg.get("active", True)]
        pending_legs = [
            leg for leg in active_legs
            if leg.result is None and leg.awaiting_window_open_at is None
        ]

        awaiting_count = sum(1 for leg in all_legs if leg.awaiting_window_open_at)

        if not all_legs:
            STATE.worker_status = "idle"
            time.sleep(min(INTERVAL, 10))
            continue

        if not pending_legs:
            # 활성 leg 중 결제 대기 단계가 있으면 그 진행을 보여줌
            has_waiting_payment = any(
                leg.result and not leg.result.get("paid")
                and leg.result.get("type") == "reserve"
                for leg in active_legs
            )
            if has_waiting_payment:
                STATE.worker_status = "waiting_payment"
            elif awaiting_count > 0 and awaiting_count == len(all_legs):
                STATE.worker_status = "awaiting_window"
            else:
                STATE.worker_status = "idle"
            time.sleep(min(INTERVAL, 10))
            continue

        STATE.worker_status = "running"

        for leg in pending_legs:
            if STOP.is_set():
                break
            done = try_leg(srt, leg)
            if done:
                sync_results_to_disk()
                continue
            time.sleep(0.5)   # leg 간 짧은 간격

        if any(leg.cfg.get("active", True) and leg.result is None for leg in STATE.legs.values()):
            time.sleep(INTERVAL)

    STATE.worker_status = "stopped"


# --- FastAPI ---

app = FastAPI(title="SRT 왕복 자동 예매")


@app.get("/api/state")
def api_state() -> JSONResponse:
    return JSONResponse(STATE.snapshot())


@app.post("/api/test-email")
def api_test_email() -> JSONResponse:
    ok, info = send_email(
        subject="[SRT] 알림 테스트",
        body=f"이 메일이 보이면 SMTP 설정 OK.\n시각: {datetime.now():%Y-%m-%d %H:%M:%S}\n",
    )
    return JSONResponse({"ok": ok, "info": info, "to": get_notify_emails()})


@app.get("/api/settings")
def api_get_settings() -> JSONResponse:
    return JSONResponse(load_settings())


@app.patch("/api/settings")
def api_update_settings(payload: dict[str, Any]) -> JSONResponse:
    current = load_settings()
    raw = payload.get("notify_emails")
    if raw is None:
        raise HTTPException(status_code=400, detail="notify_emails required")
    if not isinstance(raw, list):
        raise HTTPException(status_code=400, detail="notify_emails must be a list")
    cleaned: list[str] = []
    seen: set[str] = set()
    for e in raw:
        if not isinstance(e, str):
            continue
        e = e.strip()
        if not e or "@" not in e:
            continue
        if e in seen:
            continue
        seen.add(e)
        cleaned.append(e)
    current["notify_emails"] = cleaned
    try:
        save_settings(current)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    STATE.log(f"알림 이메일 변경: {len(cleaned)}명 — {', '.join(cleaned) or '(없음)'}")
    return JSONResponse({"ok": True, "settings": current})


@app.get("/api/watches")
def api_list_watches() -> JSONResponse:
    return JSONResponse({"watches": load_watches(), "stations": SRT_STATIONS})


@app.post("/api/watches")
def api_create_watch(payload: dict[str, Any]) -> JSONResponse:
    try:
        item = normalize_watch(payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    watches = load_watches()
    watches.append(item)
    try:
        save_watches(watches)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    STATE.sync_legs(watches)
    STATE.log(
        f"감시 추가 [{item['id']}] {item['dep']}→{item['arr']} {item['date']} "
        f"{item['time_start'][:4]}~{item['time_end'][:4]}"
    )
    return JSONResponse({"ok": True, "watch": item})


@app.patch("/api/watches/{watch_id}")
def api_update_watch(watch_id: str, payload: dict[str, Any]) -> JSONResponse:
    watches = load_watches()
    target = find_watch(watches, watch_id)
    if target is None:
        raise HTTPException(status_code=404, detail="watch not found")

    merged = dict(target)
    for key in ("label", "dep", "arr", "date", "time_start", "time_end", "adults", "active", "seat_strategy"):
        if key in payload:
            merged[key] = payload[key]

    try:
        updated = normalize_watch(merged, existing_id=watch_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    # result 와 created_at 보존
    updated["result"] = target.get("result")
    updated["created_at"] = target.get("created_at", updated["created_at"])

    new_watches = [updated if w["id"] == watch_id else w for w in watches]
    try:
        save_watches(new_watches)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    STATE.sync_legs(new_watches)
    STATE.log(f"감시 수정 [{watch_id}] active={updated['active']}")
    return JSONResponse({"ok": True, "watch": updated})


@app.delete("/api/watches/{watch_id}")
def api_delete_watch(watch_id: str) -> JSONResponse:
    watches = load_watches()
    target = find_watch(watches, watch_id)
    if target is None:
        raise HTTPException(status_code=404, detail="watch not found")

    new_watches = [w for w in watches if w["id"] != watch_id]
    try:
        save_watches(new_watches)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    STATE.sync_legs(new_watches)
    STATE.log(f"감시 삭제 [{watch_id}]")
    return JSONResponse({"ok": True, "removed_id": watch_id})


@app.post("/api/watches/{watch_id}/reset")
def api_reset_watch(watch_id: str) -> JSONResponse:
    """예매 결과를 초기화해서 다시 감시 상태로 돌림."""
    watches = load_watches()
    target = find_watch(watches, watch_id)
    if target is None:
        raise HTTPException(status_code=404, detail="watch not found")

    target["result"] = None
    try:
        save_watches(watches)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    with STATE.lock:
        leg = STATE.legs.get(watch_id)
        if leg is not None:
            leg.result = None
            leg.candidates = []
            leg.available_count = 0
            leg.standby_count = 0

    STATE.log(f"감시 초기화 [{watch_id}] — 재감시 시작")
    return JSONResponse({"ok": True, "watch": target})


@app.get("/api/time-options")
def api_time_options() -> JSONResponse:
    """수서→부산 시간대 옵션 껍데기. 현재 워커에는 적용하지 않는다."""
    return JSONResponse(
        {
            "applied_to_worker": False,
            "storage": str(TIME_OPTIONS_PATH),
            "config": load_time_options(),
        }
    )


@app.post("/api/time-options/windows")
def api_add_time_option_window(window: dict[str, Any]) -> JSONResponse:
    """시간대 옵션 추가. 저장만 하고 현재 실행 중인 예매 조건에는 반영하지 않는다."""
    try:
        item = normalize_time_window(window)
        config = load_time_options()
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    config["windows"].append(item)
    try:
        save_time_options(config)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return JSONResponse(
        {
            "ok": True,
            "applied_to_worker": False,
            "window": item,
            "config": config,
        }
    )


@app.delete("/api/time-options/windows/{window_id}")
def api_delete_time_option_window(window_id: str) -> JSONResponse:
    """시간대 옵션 삭제. 저장만 하고 현재 실행 중인 예매 조건에는 반영하지 않는다."""
    try:
        config = load_time_options()
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    before = len(config["windows"])
    config["windows"] = [w for w in config["windows"] if w["id"] != window_id]
    if len(config["windows"]) == before:
        raise HTTPException(status_code=404, detail="window not found")

    try:
        save_time_options(config)
    except OSError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    return JSONResponse(
        {
            "ok": True,
            "applied_to_worker": False,
            "removed_id": window_id,
            "config": config,
        }
    )


HTML = """<!doctype html>
<html lang="ko">
<head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>SRT Smart Booking Dashboard</title>
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@400;500;600;700;800;900&family=JetBrains+Mono:wght@500;700&display=swap" rel="stylesheet">
    <style>
        :root {
            /* SRT brand: deep navy primary + signature red accent */
            --primary: #1F2D5A;
            --primary-light: #2C3F7A;
            --primary-soft: #eef1f7;
            --primary-deep: #131E3F;
            --accent: #D6131E;
            --accent-deep: #A30C25;
            --accent-soft: #fef2f4;
            --secondary: #475569;
            --success: #16a34a;
            --success-soft: #ecfdf5;
            --warning: #d97706;
            --warning-soft: #fffbeb;
            --danger: #dc2626;
            --bg: #f4f6fa;
            --bg-deep: #e7ebf2;
            --card-bg: #ffffff;
            --text-main: #131E3F;
            --text-muted: #5d6783;
            --text-faint: #94a0b9;
            --border: #e2e8f0;
            --border-strong: #c7d0de;
            --shadow-sm: 0 1px 2px rgba(19, 30, 63, 0.05);
            --shadow-md: 0 4px 12px rgba(19, 30, 63, 0.07), 0 1px 2px rgba(19, 30, 63, 0.04);
            --shadow-lg: 0 14px 32px rgba(19, 30, 63, 0.10), 0 2px 4px rgba(19, 30, 63, 0.04);
            --radius-sm: 0.5rem;
            --radius-md: 0.75rem;
            --radius-lg: 1rem;
        }

        * { box-sizing: border-box; }
        html, body { height: 100%; }
        body {
            font-family: 'Noto Sans KR', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            margin: 0;
            padding: 0;
            background:
                radial-gradient(1200px 600px at 0% -10%, rgba(31, 45, 90, 0.06), transparent 60%),
                radial-gradient(900px 500px at 100% 0%, rgba(214, 19, 30, 0.04), transparent 55%),
                var(--bg);
            color: var(--text-main);
            line-height: 1.55;
            -webkit-font-smoothing: antialiased;
            font-feature-settings: "tnum" 1, "kern" 1;
            letter-spacing: -0.01em;
        }

        .container {
            max-width: 1080px;
            margin: 0 auto;
            padding: 2.25rem 1.25rem 4rem;
        }

        header.hero {
            display: flex;
            justify-content: space-between;
            align-items: stretch;
            gap: 1.5rem;
            margin-bottom: 2rem;
            padding: 1.625rem 1.875rem;
            background:
                radial-gradient(circle at 100% 0%, rgba(214, 19, 30, 0.22), transparent 45%),
                linear-gradient(135deg, var(--primary-deep) 0%, var(--primary) 70%, var(--primary-light) 100%);
            color: white;
            border-radius: var(--radius-lg);
            box-shadow: var(--shadow-lg);
            position: relative;
            overflow: hidden;
        }
        header.hero::before {
            content: "";
            position: absolute;
            inset: -2px -2px auto auto;
            width: 280px; height: 280px;
            background: radial-gradient(circle, rgba(255,255,255,0.12), transparent 60%);
            pointer-events: none;
        }
        header.hero::after {
            content: "";
            position: absolute;
            left: 0; right: 0; bottom: 0;
            height: 3px;
            background: linear-gradient(90deg, var(--accent) 0%, var(--accent-deep) 100%);
            pointer-events: none;
        }
        .hero-brand-row {
            display: flex; align-items: center; gap: 0.625rem;
            font-size: 0.75rem; font-weight: 700; letter-spacing: 0.12em;
            text-transform: uppercase; opacity: 0.92;
        }
        .hero-dot {
            width: 8px; height: 8px; border-radius: 50%;
            background: #ffffff;
            box-shadow: 0 0 0 4px rgba(255,255,255,0.18);
        }
        .header-title h1 {
            font-size: 2rem;
            font-weight: 800;
            margin: 0.25rem 0 0.35rem;
            color: white;
            letter-spacing: -0.025em;
            line-height: 1.15;
        }
        .header-info {
            font-size: 0.8125rem;
            color: rgba(255,255,255,0.78);
        }
        .header-info .sep { opacity: 0.45; margin: 0 0.5rem; }

        .hero-actions {
            display: flex; flex-direction: column; gap: 0.5rem; align-items: flex-end;
            justify-content: flex-end;
            position: relative; z-index: 1;
        }
        .hero-actions .btn {
            background: rgba(255,255,255,0.14);
            color: white;
            border: 1px solid rgba(255,255,255,0.25);
            backdrop-filter: blur(4px);
        }
        .hero-actions .btn:hover {
            background: rgba(255,255,255,0.22);
            border-color: rgba(255,255,255,0.4);
        }
        .hero-meta {
            font-size: 0.6875rem;
            color: rgba(255,255,255,0.6);
        }

        .btn {
            background: white;
            border: 1px solid var(--border);
            padding: 0.5rem 0.875rem;
            border-radius: var(--radius-sm);
            font-size: 0.8125rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.15s ease;
            color: var(--secondary);
            font-family: inherit;
            display: inline-flex; align-items: center; gap: 0.375rem;
            white-space: nowrap;
        }
        .btn:hover {
            background: var(--bg);
            border-color: var(--border-strong);
            color: var(--text-main);
        }
        .btn:active { transform: translateY(1px); }
        .btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
        .btn-primary {
            background: linear-gradient(180deg, var(--primary-light), var(--primary));
            color: white;
            border: 1px solid var(--primary-deep);
            box-shadow: 0 1px 0 rgba(255,255,255,0.15) inset, 0 1px 2px rgba(31,45,90,0.25);
        }
        .btn-primary:hover {
            background: linear-gradient(180deg, var(--primary), var(--primary-deep));
            color: white;
            border-color: var(--primary-deep);
        }

        .status-banner {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 0.875rem;
            margin-bottom: 2rem;
        }

        .status-card {
            background: var(--card-bg);
            padding: 1rem 1.125rem;
            border-radius: var(--radius-md);
            border: 1px solid var(--border);
            box-shadow: var(--shadow-sm);
            transition: box-shadow 0.15s ease, transform 0.15s ease;
        }
        .status-card:hover { box-shadow: var(--shadow-md); transform: translateY(-1px); }

        .status-card h4 {
            margin: 0;
            font-size: 0.6875rem;
            text-transform: uppercase;
            color: var(--text-faint);
            letter-spacing: 0.08em;
            font-weight: 700;
        }

        .status-card .value {
            font-size: 1.0625rem;
            font-weight: 700;
            margin-top: 0.35rem;
            color: var(--text-main);
            display: flex; align-items: center; gap: 0.5rem;
        }

        .leg-container {
            display: flex;
            flex-direction: column;
            gap: 1.25rem;
        }

        .leg-card {
            background: var(--card-bg);
            border-radius: var(--radius-lg);
            border: 1px solid var(--border);
            box-shadow: var(--shadow-md);
            overflow: hidden;
            transition: box-shadow 0.2s ease, transform 0.2s ease;
        }
        .leg-card:hover { box-shadow: var(--shadow-lg); }

        .leg-header {
            padding: 1.25rem 1.5rem;
            background: linear-gradient(135deg, #ffffff 0%, #faf6f7 100%);
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            gap: 1rem;
        }

        .leg-title h2 {
            margin: 0;
            font-size: 1.125rem;
            font-weight: 700;
            color: var(--text-main);
            letter-spacing: -0.01em;
        }

        .leg-subtitle {
            font-size: 0.8125rem;
            color: var(--text-muted);
            margin-top: 0.35rem;
            display: flex; flex-wrap: wrap; align-items: center; gap: 0.5rem;
        }
        .leg-subtitle .sep {
            color: var(--text-faint); font-weight: 400;
        }
        .leg-route-arrow {
            display: inline-flex; align-items: center; gap: 0.4rem;
            background: var(--primary-soft);
            color: var(--primary);
            font-weight: 700;
            padding: 0.2rem 0.65rem;
            border-radius: 9999px;
            font-size: 0.8125rem;
        }

        .leg-stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 0.875rem;
            padding: 1.25rem 1.5rem;
            background: #fcfcfd;
            border-top: 1px dashed var(--border);
        }

        .leg-body {
            padding: 0 1.5rem 1.5rem;
        }

        .train-table-wrapper {
            overflow-x: auto;
            border: 1px solid var(--border);
            border-radius: 0.5rem;
        }

        table {
            width: 100%;
            border-collapse: collapse;
            font-size: 0.875rem;
        }

        th {
            background: #f8fafc;
            padding: 0.75rem 1rem;
            text-align: left;
            font-weight: 600;
            color: var(--secondary);
            border-bottom: 1px solid var(--border);
        }

        td {
            padding: 0.75rem 1rem;
            border-bottom: 1px solid var(--border);
        }

        tr:last-child td {
            border-bottom: none;
        }

        .badge {
            display: inline-flex;
            align-items: center;
            padding: 0.125rem 0.625rem;
            border-radius: 9999px;
            font-size: 0.75rem;
            font-weight: 600;
        }

        .badge-success { background: #d1fae5; color: #065f46; }
        .badge-warning { background: #fef3c7; color: #92400e; }
        .badge-danger { background: #fee2e2; color: #991b1b; }
        .badge-muted { background: #f1f5f9; color: #475569; }

        .result-banner {
            margin: 1.5rem;
            padding: 1.25rem;
            border-radius: 0.75rem;
            border-left: 4px solid var(--success);
            background: #f0fdf4;
            display: flex;
            flex-direction: column;
            gap: 0.5rem;
        }

        .result-banner.standby {
            border-left-color: var(--warning);
            background: #fffbeb;
        }
        .result-banner.expired {
            border-left-color: var(--danger);
            background: #fef2f2;
        }
        .result-banner.expired .result-header { color: #991b1b; }

        .result-header {
            display: flex;
            align-items: center;
            gap: 0.5rem;
            font-weight: 700;
            color: #166534;
        }
        .standby .result-header { color: #92400e; }

        .log-section {
            margin-top: 3rem;
        }

        .log-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1rem;
        }

        #logs {
            background: #0f172a;
            color: #e2e8f0;
            padding: 1.5rem;
            border-radius: 0.75rem;
            font-family: 'JetBrains Mono', 'Fira Code', monospace;
            font-size: 0.8125rem;
            line-height: 1.6;
            max-height: 400px;
            overflow-y: auto;
            white-space: pre-wrap;
            border: 1px solid #1e293b;
        }

        .pulse {
            display: inline-block;
            width: 8px;
            height: 8px;
            border-radius: 50%;
            background: var(--success);
            margin-right: 6px;
            box-shadow: 0 0 0 rgba(16, 185, 129, 0.4);
            animation: pulse-animation 2s infinite;
        }

        @keyframes pulse-animation {
            0% { box-shadow: 0 0 0 0px rgba(16, 185, 129, 0.7); }
            70% { box-shadow: 0 0 0 10px rgba(16, 185, 129, 0); }
            100% { box-shadow: 0 0 0 0px rgba(16, 185, 129, 0); }
        }

        .payment-timer {
            display: inline-flex; align-items: center; gap: 0.4rem;
            padding: 0.3rem 0.65rem;
            background: var(--accent-soft);
            border: 1px solid #fbcbd1;
            border-radius: var(--radius-sm);
            color: var(--accent-deep);
            font-weight: 800;
            font-size: 0.8125rem;
            font-variant-numeric: tabular-nums;
        }
        .payment-timer.urgent {
            background: var(--accent);
            color: white;
            border-color: var(--accent-deep);
            animation: pulse-soft 1.4s infinite;
        }
        @keyframes pulse-soft {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.78; }
        }

        .error-banner {
            background: #fef2f2;
            border: 1px solid #fee2e2;
            color: #b91c1c;
            padding: 1rem 1.5rem;
            border-radius: 0.75rem;
            margin-bottom: 2rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.75rem;
        }

        .watch-form-section {
            background: var(--card-bg);
            border-radius: var(--radius-lg);
            border: 1px solid var(--border);
            box-shadow: var(--shadow-md);
            margin-bottom: 1.75rem;
            padding: 1.5rem 1.75rem 1.75rem;
            position: relative;
        }
        .watch-form-section::before {
            content: ""; position: absolute; left: 0; top: 1.25rem; bottom: 1.25rem;
            width: 3px; border-radius: 0 3px 3px 0;
            background: linear-gradient(180deg, var(--primary), var(--accent));
        }
        .watch-form-section.editing {
            border-color: var(--primary);
            box-shadow: 0 0 0 3px rgba(31,45,90,0.08), var(--shadow-md);
        }
        .watch-form-header {
            display: flex; justify-content: space-between; align-items: flex-start;
            margin-bottom: 1.125rem; gap: 1rem;
        }
        .watch-form-header h3 {
            margin: 0; font-size: 1.0625rem; font-weight: 800;
            color: var(--text-main); letter-spacing: -0.01em;
        }
        .form-subtitle {
            font-size: 0.8125rem; color: var(--text-muted); margin-top: 0.2rem;
        }
        .watch-form { display: flex; flex-direction: column; gap: 0.875rem; }
        .watch-form.collapsed { display: none; }

        .form-leg-card {
            background: linear-gradient(180deg, #fbfbfd, #f6f7fa);
            border: 1px solid var(--border);
            border-radius: var(--radius-md);
            padding: 1rem 1.125rem 1.125rem;
            display: flex; flex-direction: column; gap: 0.75rem;
        }
        .form-leg-card.return-block {
            background: linear-gradient(180deg, #fefaf3, #fdf3e3);
            border-color: #f6e3c1;
        }
        .form-leg-title {
            display: flex; align-items: center; gap: 0.625rem;
            font-size: 0.8125rem;
        }
        .form-leg-tag {
            display: inline-flex; align-items: center; padding: 0.2rem 0.625rem;
            background: var(--primary-soft); color: var(--primary);
            border-radius: 9999px; font-weight: 700; font-size: 0.6875rem;
            letter-spacing: 0.05em; text-transform: uppercase;
        }
        .form-leg-tag.return {
            background: #fff1d6; color: #92400e;
        }
        .form-leg-help { color: var(--text-muted); font-size: 0.75rem; }

        .form-row { display: flex; gap: 0.75rem; }
        .form-row-grid {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 0.75rem;
        }
        .form-row label {
            display: flex; flex-direction: column; gap: 0.3rem;
            font-size: 0.75rem; color: var(--text-muted); font-weight: 700;
            text-transform: uppercase; letter-spacing: 0.04em;
            flex: 1;
        }
        .form-row label.form-field-full { width: 100%; }
        .form-row input, .form-row select {
            padding: 0.55rem 0.75rem; border: 1px solid var(--border-strong);
            border-radius: var(--radius-sm); font-size: 0.9375rem; background: white;
            color: var(--text-main); font-family: inherit;
            font-weight: 500; letter-spacing: -0.005em;
            transition: border-color 0.15s ease, box-shadow 0.15s ease;
        }
        .form-row input::placeholder { color: var(--text-faint); font-weight: 400; }
        .form-row input:focus, .form-row select:focus {
            outline: 2px solid transparent;
            outline-offset: 2px;
            border-color: var(--primary);
            box-shadow: 0 0 0 3px rgba(31,45,90,0.18);
        }
        .form-row input:invalid:not(:placeholder-shown) {
            border-color: var(--danger);
        }

        .seat-strategy-block { width: 100%; }
        .seat-strategy-title {
            font-size: 0.75rem; color: var(--text-muted); font-weight: 700;
            text-transform: uppercase; letter-spacing: 0.04em;
            margin-bottom: 0.5rem;
            display: flex; align-items: center; gap: 0.5rem;
        }
        .seat-strategy-hint {
            font-size: 0.6875rem; color: var(--text-faint);
            text-transform: none; letter-spacing: 0; font-weight: 500;
        }
        .seat-strategy-options {
            display: grid; grid-template-columns: 1fr 1fr; gap: 0.5rem;
        }
        .radio-option {
            display: flex; align-items: flex-start; gap: 0.5rem;
            background: white; border: 1px solid var(--border-strong);
            border-radius: var(--radius-sm);
            padding: 0.65rem 0.75rem;
            cursor: pointer;
            transition: background 0.15s ease, border-color 0.15s ease;
            font-size: 0.8125rem;
        }
        .radio-option:hover {
            background: var(--primary-soft);
            border-color: var(--primary-light);
        }
        .radio-option input[type="radio"] {
            margin-top: 0.2rem;
            accent-color: var(--primary);
        }
        .radio-option:has(input:checked) {
            background: var(--primary-soft);
            border-color: var(--primary);
            box-shadow: 0 0 0 2px rgba(31,45,90,0.08);
        }
        .radio-title {
            font-weight: 700; color: var(--text-main); font-size: 0.875rem;
            line-height: 1.3;
        }
        .radio-help { color: var(--text-muted); font-size: 0.75rem; margin-top: 0.15rem; }
        .seat-strategy-row.hidden { display: none; }

        .leg-status-pill.expired { background: #fee2e2; color: #991b1b; }
        .leg-status-pill.partial { background: #fef3c7; color: #92400e; }
        .leg-status-pill.split   { background: #ede9fe; color: #5b21b6; }
        .leg-card.expired { opacity: 0.7; }
        .leg-card.expired .leg-header {
            background: linear-gradient(135deg, #ffffff 0%, #fef2f2 100%);
        }

        .reservation-list {
            display: flex; flex-direction: column; gap: 0.625rem;
            margin: 0 1.5rem 1rem;
        }
        .reservation-item {
            border: 1px solid var(--border);
            border-radius: var(--radius-sm);
            padding: 0.75rem 1rem;
            background: white;
            display: grid;
            grid-template-columns: auto 1fr auto;
            gap: 0.75rem;
            align-items: center;
        }
        .reservation-item.paid { background: var(--success-soft); border-color: #a7f3d0; }
        .reservation-item .res-num {
            font-family: 'JetBrains Mono', 'Fira Code', monospace;
            font-size: 0.8125rem; font-weight: 700; color: var(--text-main);
        }
        .reservation-item .res-meta {
            font-size: 0.75rem; color: var(--text-muted);
        }
        .reservation-item .res-paid {
            font-size: 0.6875rem; font-weight: 700; padding: 0.2rem 0.5rem;
            border-radius: 9999px;
            background: #f1f5f9; color: var(--text-muted);
            text-transform: uppercase; letter-spacing: 0.04em;
        }
        .reservation-item.paid .res-paid {
            background: #dcfce7; color: #15803d;
        }

        .form-toggle-row {
            display: flex; align-items: center; gap: 0.625rem;
            padding: 0.75rem 1rem;
            background: white;
            border: 1px dashed var(--border-strong);
            border-radius: var(--radius-sm);
            font-size: 0.8125rem;
            color: var(--secondary);
            cursor: pointer;
            transition: background 0.15s ease, border-color 0.15s ease;
        }
        .form-toggle-row:hover { background: var(--primary-soft); border-color: var(--primary-light); }
        .form-toggle-row input[type="checkbox"] {
            width: 1rem; height: 1rem; accent-color: var(--primary);
            cursor: pointer;
        }
        .form-toggle-row strong { color: var(--text-main); font-weight: 700; }
        .form-toggle-row.hidden { display: none; }

        .form-actions {
            justify-content: flex-end; align-items: center; gap: 0.75rem;
            margin-top: 0.25rem;
            flex-wrap: wrap;
        }
        .form-result { font-size: 0.8125rem; color: var(--text-muted); margin-right: auto; }
        .form-result.error { color: var(--danger); font-weight: 600; }
        .form-result.success { color: var(--success); font-weight: 600; }

        .leg-actions {
            display: flex; gap: 0.4rem; align-items: center; flex-wrap: wrap;
            justify-content: flex-end;
        }
        .btn-sm {
            padding: 0.35rem 0.7rem; font-size: 0.75rem;
        }
        .btn-danger {
            background: white; color: var(--danger);
            border: 1px solid #fecaca;
        }
        .btn-danger:hover { background: #fef2f2; color: var(--danger); border-color: #fca5a5; }
        .btn-edit {
            background: var(--primary-soft); color: var(--primary);
            border-color: #f5cbcb;
        }
        .btn-edit:hover { background: #fbe3e3; color: var(--primary-deep); border-color: var(--primary-light); }
        .leg-card.inactive { opacity: 0.65; }
        .leg-card.inactive .leg-header { background: #f1f5f9; }
        .leg-card.booked .leg-header { background: linear-gradient(135deg, #ffffff 0%, #ecfdf5 100%); }

        .leg-status-pill {
            display: inline-flex; align-items: center; gap: 0.35rem;
            padding: 0.25rem 0.625rem;
            border-radius: 9999px;
            font-size: 0.6875rem;
            font-weight: 700;
            text-transform: uppercase; letter-spacing: 0.05em;
        }
        .leg-status-pill.active { background: var(--success-soft); color: #065f46; }
        .leg-status-pill.paused { background: #f1f5f9; color: var(--text-muted); }
        .leg-status-pill.booked { background: #dcfce7; color: #14532d; }
        .leg-status-pill .pulse-mini {
            width: 6px; height: 6px; border-radius: 50%;
            background: currentColor;
            animation: pulse-animation 1.6s infinite;
        }

        .empty-state {
            text-align: center; padding: 3rem 1.5rem;
            color: var(--text-muted);
            background: var(--card-bg);
            border-radius: var(--radius-lg);
            border: 1px dashed var(--border-strong);
        }
        .empty-state .empty-emoji {
            font-size: 2.25rem; line-height: 1; margin-bottom: 0.75rem;
            opacity: 0.7;
        }
        .empty-state .empty-title {
            font-size: 1rem; font-weight: 700; color: var(--text-main);
            margin-bottom: 0.25rem;
        }

        @media (max-width: 640px) {
            .header-bar { flex-direction: column; align-items: flex-start; }
            .leg-header { flex-direction: column; align-items: flex-start; gap: 1rem; }
            .form-row-grid { grid-template-columns: 1fr; }
        }

        .form-rules-note {
            background: #fffbeb; border: 1px solid #fde68a;
            border-radius: 0.5rem; padding: 0.5rem 0.75rem;
            font-size: 0.75rem; color: #92400e;
            margin-bottom: 0.875rem;
        }
        .form-rules-note strong { color: #78350f; }

        .arr-fare-hint {
            font-size: 0.75rem; color: var(--text-muted);
            margin-top: 0.25rem; display: block;
            font-weight: 500;
        }
        .arr-fare-hint.active { color: var(--primary); font-weight: 700; }

        .leg-fare { color: var(--primary); font-weight: 700; }

        .modal-overlay {
            position: fixed; inset: 0; background: rgba(15,23,42,0.45);
            display: flex; align-items: center; justify-content: center;
            z-index: 100; padding: 1rem;
        }
        .modal-overlay[hidden] { display: none; }
        .modal-card {
            background: var(--card-bg); border-radius: var(--radius-lg);
            box-shadow: var(--shadow-lg);
            max-width: 480px; width: 100%; max-height: 85vh; display: flex; flex-direction: column;
            overflow: hidden;
        }
        .modal-header {
            padding: 1.125rem 1.5rem; border-bottom: 1px solid var(--border);
            display: flex; justify-content: space-between; align-items: center;
        }
        .modal-header h3 { margin: 0; font-size: 1.0625rem; font-weight: 800; }
        .modal-body { padding: 1.25rem 1.5rem; flex: 1; overflow-y: auto; }
        .modal-help { margin: 0 0 0.75rem; font-size: 0.8125rem; color: var(--text-muted); }
        .modal-body textarea {
            width: 100%; min-height: 120px;
            padding: 0.75rem; border: 1px solid var(--border-strong);
            border-radius: var(--radius-sm); font-size: 0.875rem;
            font-family: 'JetBrains Mono', 'Fira Code', monospace;
            resize: vertical;
        }
        .modal-body textarea:focus { outline: none; border-color: var(--primary); box-shadow: 0 0 0 3px rgba(31,45,90,0.12); }
        .modal-footer {
            padding: 1rem 1.5rem; border-top: 1px solid var(--border);
            display: flex; justify-content: flex-end; gap: 0.5rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <header class="hero">
            <div class="header-title">
                <div class="hero-brand-row"><span class="hero-dot"></span> SRT · Auto-Booking</div>
                <h1>Smart Booking Dashboard</h1>
                <div class="header-info" id="global-status">연결 중...</div>
            </div>
            <div class="hero-actions">
                <button class="btn" onclick="testEmail()">알림 테스트</button>
                <span id="email-result" class="hero-meta"></span>
            </div>
        </header>

        <div id="banner-container"></div>

        <div class="status-banner">
            <div class="status-card">
                <h4>워커 상태</h4>
                <div class="value" id="worker-status">-</div>
            </div>
            <div class="status-card">
                <h4>로그인</h4>
                <div class="value" id="login-status">-</div>
            </div>
            <div class="status-card">
                <h4>폴링 간격</h4>
                <div class="value" id="polling-info">-</div>
            </div>
            <div class="status-card notify-card">
                <h4>알림 대상</h4>
                <div class="value" id="notify-info" style="font-size: 0.875rem;">-</div>
                <button class="btn btn-sm" id="notify-edit-btn" onclick="openNotifyEditor()" style="margin-top: 0.5rem;">수정</button>
            </div>
        </div>

        <section class="watch-form-section">
            <div class="watch-form-header">
                <div>
                    <h3 id="form-title">신규 감시 추가</h3>
                    <div class="form-subtitle" id="form-subtitle">출발 · 도착 · 날짜 · 시간대를 지정하면 잔여석 발생 시 자동 예매됩니다.</div>
                </div>
                <button type="button" class="btn" id="form-toggle" onclick="toggleForm()" aria-expanded="true" aria-controls="watch-form">접기</button>
            </div>
            <form id="watch-form" class="watch-form" onsubmit="return submitWatch(event)">
                <input type="hidden" name="id" id="form-id">
                <div class="form-rules-note">
                    <strong>예매 가능 기간</strong> · 열차 출발 1개월 전 07:00 ~ 출발 직전까지. 그 이전 날짜로 등록하면 자동으로 오픈 시각까지 대기합니다.
                </div>
                <div class="form-leg-card">
                    <div class="form-leg-title">
                        <span class="form-leg-tag" id="leg-tag-outbound">가는편</span>
                        <span class="form-leg-help">출발지 · 인원 · 날짜 · 시간대</span>
                    </div>
                    <div class="form-row">
                        <label class="form-field-full">라벨 <input type="text" name="label" placeholder="(선택) 예: 5/23 본가행"></label>
                    </div>
                    <div class="form-row form-row-grid">
                        <label>출발역
                            <select name="dep" id="dep-select" required></select>
                        </label>
                        <label>도착역
                            <select name="arr" id="arr-select" required></select>
                            <span class="arr-fare-hint" id="arr-fare-hint"></span>
                        </label>
                        <label>인원
                            <input type="number" name="adults" min="1" max="9" value="1" required>
                        </label>
                    </div>
                    <div class="form-row form-row-grid">
                        <label>날짜
                            <input type="date" name="date" required>
                        </label>
                        <label>시작 시간
                            <input type="time" name="time_start" value="06:00" required>
                        </label>
                        <label>종료 시간
                            <input type="time" name="time_end" value="23:00" required>
                        </label>
                    </div>
                    <div class="form-row" id="seat-strategy-row">
                        <div class="seat-strategy-block">
                            <div class="seat-strategy-title">좌석 전략 <span class="seat-strategy-hint">2인 이상에만 적용</span></div>
                            <div class="seat-strategy-options">
                                <label class="radio-option">
                                    <input type="radio" name="seat_strategy" value="split_ok" checked>
                                    <div>
                                        <div class="radio-title">연석 우선 + 분할 허용</div>
                                        <div class="radio-help">Adult(N) 한번에 시도 → 실패시 같은 열차에서 1좌석씩 빠르게 분할 (권장)</div>
                                    </div>
                                </label>
                                <label class="radio-option">
                                    <input type="radio" name="seat_strategy" value="together_only">
                                    <div>
                                        <div class="radio-title">반드시 같이</div>
                                        <div class="radio-help">Adult(N) 한번에만 시도 (분할 금지). 못 잡으면 계속 대기.</div>
                                    </div>
                                </label>
                            </div>
                        </div>
                    </div>
                </div>

                <label class="form-toggle-row" id="round-trip-row">
                    <input type="checkbox" id="round-trip-toggle" onchange="toggleRoundTrip()">
                    <span><strong>왕복으로 추가</strong> · 오는편 감시도 동시에 등록 (출발/도착 자동 반전)</span>
                </label>

                <div class="form-leg-card return-block" id="return-block" hidden>
                    <div class="form-leg-title">
                        <span class="form-leg-tag return">오는편</span>
                        <span class="form-leg-help">자동 반전된 구간 · 날짜와 시간대만 지정</span>
                    </div>
                    <div class="form-row form-row-grid">
                        <label>날짜
                            <input type="date" name="return_date">
                        </label>
                        <label>시작 시간
                            <input type="time" name="return_time_start" value="14:00">
                        </label>
                        <label>종료 시간
                            <input type="time" name="return_time_end" value="22:00">
                        </label>
                    </div>
                </div>

                <div class="form-row form-actions">
                    <span id="form-result" class="form-result"></span>
                    <button type="button" class="btn" id="form-cancel" onclick="cancelEdit()" hidden>취소</button>
                    <button type="submit" class="btn btn-primary" id="form-submit">감시 추가</button>
                </div>
            </form>
        </section>

        <div id="legs" class="leg-container"></div>

        <div class="log-section">
            <div class="log-header">
                <h3 style="margin:0; font-size: 1.125rem; font-weight: 700;">실시간 로그</h3>
                <span style="font-size: 0.75rem; color: var(--text-muted);">최근 300개 항목</span>
            </div>
            <pre id="logs"></pre>
        </div>
    </div>

    <div class="modal-overlay" id="notify-modal" hidden onclick="closeNotifyEditor(event)" role="dialog" aria-modal="true" aria-labelledby="notify-modal-title">
        <div class="modal-card" onclick="event.stopPropagation()">
            <div class="modal-header">
                <h3 id="notify-modal-title">알림 이메일</h3>
                <button class="btn btn-sm" onclick="closeNotifyEditor()">닫기</button>
            </div>
            <div class="modal-body">
                <p class="modal-help">예매 완료 시 알림을 받을 이메일. 여러 개면 줄바꿈 또는 쉼표로 구분.</p>
                <textarea id="notify-emails-input" rows="6" placeholder="example@gmail.com&#10;another@example.com"></textarea>
                <div id="notify-result" class="form-result"></div>
            </div>
            <div class="modal-footer">
                <button class="btn" onclick="closeNotifyEditor()">취소</button>
                <button class="btn btn-primary" onclick="saveNotifyEmails()">저장</button>
            </div>
        </div>
    </div>

    <script>
        document.addEventListener('keydown', (e) => {
            if (e.key === 'Escape') {
                const modal = document.getElementById('notify-modal');
                if (modal && !modal.hidden) {
                    modal.hidden = true;
                }
            }
        });

        function formatDateTimeShort(iso) {
            const d = new Date(iso);
            if (Number.isNaN(d.getTime())) return iso;
            const mm = String(d.getMonth()+1).padStart(2,'0');
            const dd = String(d.getDate()).padStart(2,'0');
            const hh = String(d.getHours()).padStart(2,'0');
            const mi = String(d.getMinutes()).padStart(2,'0');
            return `${mm}/${dd} ${hh}:${mi}`;
        }

        function formatTime(value) {
            if (!value) return '-';
            const date = new Date(value);
            if (Number.isNaN(date.getTime())) return '-';
            return date.toLocaleTimeString();
        }

        function secondsUntil(value) {
            if (!value) return null;
            const date = new Date(value);
            if (Number.isNaN(date.getTime())) return null;
            return Math.max(0, Math.floor((date.getTime() - Date.now()) / 1000));
        }

        function formatRemaining(seconds) {
            if (seconds === null) return '-';
            const m = String(Math.floor(seconds / 60)).padStart(2, '0');
            const s = String(seconds % 60).padStart(2, '0');
            return `${m}:${s}`;
        }

        async function updateDashboard() {
            try {
                const response = await fetch('/api/state');
                const state = await response.json();
                
                // Update Global Status
                const globalStatus = document.getElementById('global-status');
                const lastUpdate = new Date().toLocaleTimeString();
                globalStatus.innerHTML = `<span class="pulse"></span> 마지막 업데이트: ${lastUpdate} | 시작: ${state.started_at?.slice(11, 19)}`;

                // Update Status Cards
                document.getElementById('worker-status').innerHTML = 
                    `<span class="badge ${state.worker_status === 'running' ? 'badge-success' : 'badge-warning'}">${state.worker_status.toUpperCase()}</span>`;
                document.getElementById('login-status').innerHTML = 
                    `<span class="badge ${state.login_status === 'ok' ? 'badge-success' : 'badge-danger'}">${state.login_status.toUpperCase()}</span>`;
                document.getElementById('polling-info').textContent = `${state.interval}초`;
                document.getElementById('notify-info').textContent = (state.notify_emails || []).join(', ') || '(설정 안 됨)';

                // Error Banner
                const bannerContainer = document.getElementById('banner-container');
                bannerContainer.innerHTML = '';
                if (state.worker_status === 'error') {
                    const div = document.createElement('div');
                    div.className = 'error-banner';
                    div.innerHTML = `<span>⚠️</span> <div><strong>워커 오류:</strong> ${state.last_error || '알 수 없는 오류'}</div>`;
                    bannerContainer.appendChild(div);
                }

                // Populate station dropdowns (once)
                if (state.stations) {
                    window.__lastStations = state.stations;
                    populateStations(state.stations);
                }
                if (state.fares_from_suseo) window.__fares = state.fares_from_suseo;

                // Render Legs
                const legsRoot = document.getElementById('legs');
                legsRoot.innerHTML = '';

                let legEntries = Object.entries(state.legs);
                // 정렬: 활성+미예매 → 활성+예매중 → 활성+완료 → 일시정지, 같은 그룹은 날짜순
                legEntries.sort(([, a], [, b]) => {
                    const rank = (l) => {
                        if (!l.active) return 3;
                        if (l.result && (l.result.paid || l.result.type === 'standby')) return 2;
                        if (l.result) return 1;
                        return 0;
                    };
                    const ra = rank(a), rb = rank(b);
                    if (ra !== rb) return ra - rb;
                    return (a.date + a.time_start).localeCompare(b.date + b.time_start);
                });

                if (!legEntries.length) {
                    const empty = document.createElement('div');
                    empty.className = 'empty-state';
                    empty.innerHTML = `
                        <div class="empty-emoji">🚄</div>
                        <div class="empty-title">등록된 감시가 없습니다</div>
                        <div style="margin-bottom: 1.25rem;">출발 · 도착 · 날짜를 지정하면 자동으로 잔여석을 잡습니다.</div>
                        <button class="btn btn-primary" onclick="document.querySelector('.watch-form-section').scrollIntoView({behavior:'smooth'}); document.querySelector('#watch-form input[name=label]').focus();">첫 감시 추가하기</button>`;
                    legsRoot.appendChild(empty);
                }

                for (const [id, leg] of legEntries) {
                    const card = document.createElement('div');
                    card.className = 'leg-card';
                    
                    let resultHtml = '';
                    if (leg.result) {
                        const r = leg.result;
                        const isStandby = r.type === 'standby';
                        const isPaid = r.paid === true || r.payment_status === 'paid';
                        const isExpired = r.payment_status === 'expired';
                        const isPartialExpired = r.payment_status === 'partially_paid_expired';
                        const isPartial = !!r.partial;
                        const isSplit = !!r.split;
                        const reservations = r.reservations || [];

                        const untilRemaining = formatRemaining(secondsUntil(r.payment_check_until));
                        const nextCheckRemaining = formatRemaining(secondsUntil(r.next_payment_check_at));

                        let statusText, bannerKind;
                        if (isStandby)              { statusText = '예약대기 등록됨'; bannerKind = 'standby'; }
                        else if (isPaid)            { statusText = '결제 확인됨'; bannerKind = ''; }
                        else if (isExpired)         { statusText = '결제 마감 — 미결제 (자동 재예매 안 함)'; bannerKind = 'expired'; }
                        else if (isPartialExpired)  { statusText = '결제 마감 — 일부 결제'; bannerKind = 'expired'; }
                        else if (isPartial)         { statusText = `부분 예매 (${reservations.length}/${r.requested_adults || leg.adults}좌석)`; bannerKind = 'standby'; }
                        else if (isSplit)           { statusText = `분할 예매 (${reservations.length}건) · 결제 대기`; bannerKind = ''; }
                        else                         { statusText = '예매 완료 · 결제 대기'; bannerKind = ''; }

                        const resItemsHtml = reservations.length
                            ? reservations.map((rr, i) => `
                                <div class="reservation-item ${rr.paid ? 'paid' : ''}">
                                    <div class="res-num">#${i+1} · ${rr.reservation_number || '-'}</div>
                                    <div class="res-meta">
                                        ${rr.train?.train_number || '-'} ${rr.train?.dep_time || ''} → ${rr.train?.arr_time || ''}
                                        · 결제기한 ${rr.payment_deadline || '-'}
                                        ${rr.total_cost ? `· \\u20a9${Number(rr.total_cost).toLocaleString()}` : ''}
                                    </div>
                                    <div class="res-paid">${rr.paid ? '결제완료' : '결제대기'}</div>
                                </div>`).join('')
                            : `<div class="reservation-item">
                                <div class="res-num">${r.reservation_number || '-'}</div>
                                <div class="res-meta">${r.train?.dep_time || ''} → ${r.train?.arr_time || ''} · 결제기한 ${r.payment_deadline || '-'}</div>
                                <div class="res-paid">${isPaid ? '결제완료' : '결제대기'}</div>
                              </div>`;

                        let paymentLine;
                        if (isStandby) paymentLine = '예약대기 등록 상태입니다.';
                        else if (isPaid) paymentLine = `결제 확인 시각: ${formatTime(r.payment_checked_at)}`;
                        else if (isExpired || isPartialExpired) paymentLine = `결제 마감 지남. '결과 초기화' 버튼으로 재감시 시작 가능.`;
                        else paymentLine = `<span class="payment-timer ${secondsUntil(r.payment_check_until) !== null && secondsUntil(r.payment_check_until) < 600 ? 'urgent' : ''}">결제 마감까지 ${untilRemaining}</span> <span style="color: var(--text-muted); font-size: 0.75rem;">다음 확인: ${formatTime(r.next_payment_check_at)}</span>`;

                        resultHtml = `
                            <div class="result-banner ${bannerKind}">
                                <div class="result-header">
                                    <span>${statusText}</span>
                                    <span style="font-weight: 400; font-size: 0.875rem; color: var(--text-muted); margin-left: auto;">
                                        ${(r.captured_at || '').slice(11, 19)}
                                    </span>
                                </div>
                                <div style="font-size: 0.8125rem; color: var(--secondary);">
                                    ${paymentLine}
                                </div>
                            </div>
                            <div class="reservation-list">
                                ${resItemsHtml}
                            </div>
                        `;
                    }

                    const trainRows = leg.candidates.map(t => `
                        <tr>
                            <td style="font-weight: 700;">${t.dep_time}</td>
                            <td style="color: var(--text-muted);">${t.arr_time}</td>
                            <td>${t.train_number}</td>
                            <td>${t.general}</td>
                            <td>${t.special}</td>
                            <td><span class="badge ${t.seat_available ? 'badge-success' : 'badge-muted'}">${t.seat_available ? '가능' : '매진'}</span></td>
                            <td><span class="badge ${t.standby_available ? 'badge-warning' : 'badge-muted'}">${t.standby_available ? '대기' : '-'}</span></td>
                        </tr>
                    `).join('');

                    if (!leg.active) card.classList.add('inactive');
                    const r0 = leg.result;
                    const isExpired = r0 && (r0.payment_status === 'expired' || r0.payment_status === 'partially_paid_expired');
                    const isBooked = !!(r0 && !isExpired && (r0.paid || r0.type === 'standby' || r0.type === 'reserve'));
                    if (isExpired) card.classList.add('expired');
                    else if (isBooked && leg.active) card.classList.add('booked');

                    const dateFmt = `${leg.date.slice(0,4)}-${leg.date.slice(4,6)}-${leg.date.slice(6,8)}`;
                    const timeFmt = `${leg.time_start.slice(0,2)}:${leg.time_start.slice(2,4)} ~ ${leg.time_end.slice(0,2)}:${leg.time_end.slice(2,4)}`;
                    const pauseLabel = leg.active ? '일시정지' : '재개';
                    const resetBtn = r0
                        ? `<button class="btn btn-sm" onclick="resetWatch('${leg.id}')">결과 초기화</button>` : '';

                    // 좌석 전략 배지
                    const strategyBadge = (leg.adults >= 2)
                        ? `<span class="leg-status-pill ${leg.seat_strategy === 'split_ok' ? 'split' : 'paused'}">${leg.seat_strategy === 'split_ok' ? '분할 허용' : '같이만'}</span>`
                        : '';

                    let pillClass = 'active', pillLabel = '감시 중', pillPulse = true;
                    if (!leg.active) { pillClass = 'paused'; pillLabel = '일시정지'; pillPulse = false; }
                    else if (leg.awaiting_window_open_at) {
                        pillClass = 'paused'; pillPulse = false;
                        pillLabel = `예매 오픈 대기 · ${formatDateTimeShort(leg.awaiting_window_open_at)}`;
                    }
                    else if (isExpired) {
                        pillClass = 'expired'; pillPulse = false;
                        pillLabel = r0.payment_status === 'partially_paid_expired' ? '일부 결제 / 마감' : '결제 마감';
                    }
                    else if (r0 && r0.paid) { pillClass = 'booked'; pillLabel = '결제 완료'; pillPulse = false; }
                    else if (r0 && r0.type === 'standby') { pillClass = 'booked'; pillLabel = '예약대기'; pillPulse = false; }
                    else if (r0 && r0.partial) { pillClass = 'partial'; pillLabel = `부분 ${(r0.reservations||[]).length}좌석`; pillPulse = true; }
                    else if (r0 && r0.split) { pillClass = 'split'; pillLabel = `분할 ${(r0.reservations||[]).length}건`; pillPulse = true; }
                    else if (r0) { pillClass = 'booked'; pillLabel = '예매 완료'; pillPulse = true; }
                    const pillHtml = `<span class="leg-status-pill ${pillClass}">${pillPulse ? '<span class="pulse-mini"></span>' : ''}${pillLabel}</span>${strategyBadge}`;
                    const safeLabel = leg.label.replace(/'/g, "&#39;");

                    card.innerHTML = `
                        <div class="leg-header">
                            <div class="leg-title">
                                <div style="display:flex; align-items:center; gap:0.5rem; flex-wrap:wrap;">
                                    <h2>${leg.label}</h2>
                                    ${pillHtml}
                                </div>
                                <div class="leg-subtitle">
                                    <span class="leg-route-arrow">${leg.dep} → ${leg.arr}</span>
                                    <span>${dateFmt}</span>
                                    <span class="sep">·</span>
                                    <span>${timeFmt}</span>
                                    <span class="sep">·</span>
                                    <span>성인 ${leg.adults}명</span>
                                    ${leg.fare ? `<span class="sep">·</span><span class="leg-fare">일반 \\u20a9${(leg.fare.general * leg.adults).toLocaleString()}</span>` : ''}
                                </div>
                            </div>
                            <div class="leg-actions">
                                <button class="btn btn-sm btn-edit" onclick='startEdit(${JSON.stringify(leg).replace(/'/g, "&#39;")})'>수정</button>
                                ${resetBtn}
                                <button class="btn btn-sm" onclick="toggleWatch('${leg.id}', ${!leg.active})">${pauseLabel}</button>
                                <button class="btn btn-sm btn-danger" onclick="deleteWatch('${leg.id}', '${safeLabel}')">삭제</button>
                            </div>
                        </div>
                        ${resultHtml}
                        <div class="leg-stats">
                            <div class="status-card" style="box-shadow:none; border-color:#f1f5f9;">
                                <h4>시도 횟수</h4>
                                <div class="value">${leg.attempts}</div>
                            </div>
                            <div class="status-card" style="box-shadow:none; border-color:#f1f5f9;">
                                <h4>검색 결과</h4>
                                <div class="value">${leg.candidates.length}</div>
                            </div>
                            <div class="status-card" style="box-shadow:none; border-color:#f1f5f9;">
                                <h4>잔여석</h4>
                                <div class="value ${leg.available_count ? 'ok' : ''}" style="color:${leg.available_count ? 'var(--success)' : 'inherit'}">
                                    ${leg.available_count}
                                </div>
                            </div>
                        </div>
                        <div class="leg-body">
                            <div class="train-table-wrapper">
                                <table>
                                    <thead>
                                        <tr>
                                            <th>출발</th>
                                            <th>도착</th>
                                            <th>열차</th>
                                            <th>일반</th>
                                            <th>특실</th>
                                            <th>예매</th>
                                            <th>대기</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        ${trainRows || '<tr><td colspan="7" style="text-align:center; padding:2rem; color:var(--text-muted);">조건에 맞는 열차가 없습니다.</td></tr>'}
                                    </tbody>
                                </table>
                            </div>
                        </div>
                    `;
                    legsRoot.appendChild(card);
                }

                // Update Logs
                const logsEl = document.getElementById('logs');
                const isAtBottom = logsEl.scrollHeight - logsEl.clientHeight <= logsEl.scrollTop + 1;
                logsEl.textContent = (state.logs || []).join('\\n');
                if (isAtBottom) {
                    logsEl.scrollTop = logsEl.scrollHeight;
                }

            } catch (err) {
                console.error('State fetch failed:', err);
            }
        }

        // ===== Form state =====
        let editingId = null;          // 편집 중인 watch id
        let stationsPopulated = false; // 드롭다운 초기화 여부

        function isoDate(offsetDays) {
            const d = new Date();
            d.setDate(d.getDate() + (offsetDays || 0));
            const yyyy = d.getFullYear();
            const mm = String(d.getMonth() + 1).padStart(2, '0');
            const dd = String(d.getDate()).padStart(2, '0');
            return `${yyyy}-${mm}-${dd}`;
        }

        function formatDateKR(yyyymmdd) {
            return `${yyyymmdd.slice(0,4)}-${yyyymmdd.slice(4,6)}-${yyyymmdd.slice(6,8)}`;
        }
        function dbDateToInput(yyyymmdd) {
            // 저장형식(YYYYMMDD) → input[type=date] (YYYY-MM-DD)
            return `${yyyymmdd.slice(0,4)}-${yyyymmdd.slice(4,6)}-${yyyymmdd.slice(6,8)}`;
        }
        function dbTimeToInput(hhmmss) {
            return `${hhmmss.slice(0,2)}:${hhmmss.slice(2,4)}`;
        }

        function setStationOptions(selectEl, stations, selected) {
            selectEl.innerHTML = stations
                .map(s => `<option value="${s}" ${s === selected ? 'selected' : ''}>${s}</option>`)
                .join('');
        }

        function updateFareHint() {
            const dep = document.querySelector('#dep-select')?.value;
            const arr = document.querySelector('#arr-select')?.value;
            const hint = document.getElementById('arr-fare-hint');
            if (!hint) return;
            const fares = window.__fares || {};
            if (dep === '수서' && fares[arr]) {
                hint.textContent = `예상 운임: 일반 \\u20a9${fares[arr].general.toLocaleString()} / 특실 \\u20a9${fares[arr].special.toLocaleString()}`;
                hint.classList.add('active');
            } else if (dep !== '수서') {
                hint.textContent = '운임표는 수서 출발만 제공';
                hint.classList.remove('active');
            } else {
                hint.textContent = '';
                hint.classList.remove('active');
            }
        }

        function populateStations(stations) {
            if (stationsPopulated || !stations || !stations.length) return;
            setStationOptions(document.getElementById('dep-select'), stations, '수서');
            setStationOptions(document.getElementById('arr-select'), stations, '부산');
            stationsPopulated = true;

            // 날짜: min=오늘, 기본값=내일
            const today = isoDate(0);
            const tomorrow = isoDate(1);
            const maxDate = isoDate(45);
            const dateInput = document.querySelector('#watch-form input[name="date"]');
            const returnDate = document.querySelector('#watch-form input[name="return_date"]');
            if (dateInput) {
                dateInput.min = today;
                dateInput.max = maxDate;
                if (!dateInput.value) dateInput.value = tomorrow;
            }
            if (returnDate) {
                returnDate.min = today;
                returnDate.max = maxDate;
            }

            // 가는편 날짜 변경 → 오는편 디폴트를 가는편+1일로 자동 세팅 (사용자가 직접 바꾼 적 없을 때만)
            if (dateInput && returnDate && !dateInput.dataset.returnBound) {
                dateInput.addEventListener('change', () => {
                    if (returnDate.dataset.userSet) return;
                    const dep = new Date(dateInput.value);
                    if (Number.isNaN(dep.getTime())) return;
                    dep.setDate(dep.getDate() + 1);
                    const y = dep.getFullYear();
                    const m = String(dep.getMonth()+1).padStart(2,'0');
                    const d = String(dep.getDate()).padStart(2,'0');
                    returnDate.value = `${y}-${m}-${d}`;
                });
                returnDate.addEventListener('change', () => { returnDate.dataset.userSet = '1'; });
                dateInput.dataset.returnBound = '1';
            }
            // 초기값도 가는편 + 1일
            if (returnDate && !returnDate.value && dateInput && dateInput.value) {
                const dep = new Date(dateInput.value);
                if (!Number.isNaN(dep.getTime())) {
                    dep.setDate(dep.getDate() + 1);
                    const y = dep.getFullYear();
                    const m = String(dep.getMonth()+1).padStart(2,'0');
                    const d = String(dep.getDate()).padStart(2,'0');
                    returnDate.value = `${y}-${m}-${d}`;
                }
            }

            // 인원 변경 → 좌석전략 표시 토글
            const adultsInput = document.querySelector('#watch-form input[name="adults"]');
            if (adultsInput && !adultsInput.dataset.bound) {
                adultsInput.addEventListener('input', updateSeatStrategyVisibility);
                adultsInput.dataset.bound = '1';
                updateSeatStrategyVisibility();
            }

            // 운임 힌트 이벤트 바인딩
            const depSel = document.getElementById('dep-select');
            const arrSel = document.getElementById('arr-select');
            if (!depSel.dataset.fareBound) {
                depSel.addEventListener('change', updateFareHint);
                arrSel.addEventListener('change', updateFareHint);
                depSel.dataset.fareBound = '1';
            }
            updateFareHint();
        }

        function updateSeatStrategyVisibility() {
            const adultsInput = document.querySelector('#watch-form input[name="adults"]');
            const row = document.getElementById('seat-strategy-row');
            if (!adultsInput || !row) return;
            const n = Number(adultsInput.value || 1);
            if (n >= 2) {
                row.classList.remove('hidden');
            } else {
                row.classList.add('hidden');
            }
        }

        function setFormResult(message, kind) {
            const el = document.getElementById('form-result');
            el.textContent = message || '';
            el.className = 'form-result' + (kind ? ' ' + kind : '');
            if (message) {
                setTimeout(() => {
                    if (el.textContent === message) {
                        el.textContent = '';
                        el.className = 'form-result';
                    }
                }, 4000);
            }
        }

        function toggleForm() {
            const form = document.getElementById('watch-form');
            const btn = document.getElementById('form-toggle');
            form.classList.toggle('collapsed');
            const collapsed = form.classList.contains('collapsed');
            btn.textContent = collapsed ? '펼치기' : '접기';
            btn.setAttribute('aria-expanded', collapsed ? 'false' : 'true');
        }

        function toggleRoundTrip() {
            const block = document.getElementById('return-block');
            const cb = document.getElementById('round-trip-toggle');
            block.hidden = !cb.checked;
            const inputs = block.querySelectorAll('input');
            inputs.forEach(i => i.required = cb.checked);
        }

        function setFormMode(mode) {
            const section = document.querySelector('.watch-form-section');
            const title = document.getElementById('form-title');
            const subtitle = document.getElementById('form-subtitle');
            const submitBtn = document.getElementById('form-submit');
            const cancelBtn = document.getElementById('form-cancel');
            const rtRow = document.getElementById('round-trip-row');
            const returnBlock = document.getElementById('return-block');
            const outboundTag = document.getElementById('leg-tag-outbound');

            if (mode === 'edit') {
                section.classList.add('editing');
                title.textContent = '감시 수정';
                subtitle.textContent = '기존 감시 항목을 수정합니다. 잡힌 예매 결과는 유지됩니다.';
                submitBtn.textContent = '수정 저장';
                cancelBtn.hidden = false;
                rtRow.classList.add('hidden');
                returnBlock.hidden = true;
                outboundTag.textContent = '편집';
            } else {
                section.classList.remove('editing');
                title.textContent = '신규 감시 추가';
                subtitle.textContent = '출발 · 도착 · 날짜 · 시간대를 지정하면 잔여석 발생 시 자동 예매됩니다.';
                submitBtn.textContent = '감시 추가';
                cancelBtn.hidden = true;
                rtRow.classList.remove('hidden');
                outboundTag.textContent = '가는편';
            }
        }

        function cancelEdit() {
            editingId = null;
            const form = document.getElementById('watch-form');
            form.reset();
            document.getElementById('form-id').value = '';
            stationsPopulated = false;
            populateStations(window.__lastStations);
            toggleRoundTrip();
            setFormMode('create');
            setFormResult('');
        }

        function startEdit(leg) {
            editingId = leg.id;
            const form = document.getElementById('watch-form');
            form.querySelector('input[name="label"]').value = leg.label || '';
            setStationOptions(document.getElementById('dep-select'), window.__lastStations || [], leg.dep);
            setStationOptions(document.getElementById('arr-select'), window.__lastStations || [], leg.arr);
            form.querySelector('input[name="adults"]').value = leg.adults;
            form.querySelector('input[name="date"]').value = dbDateToInput(leg.date);
            form.querySelector('input[name="time_start"]').value = dbTimeToInput(leg.time_start);
            form.querySelector('input[name="time_end"]').value = dbTimeToInput(leg.time_end);
            // 좌석 전략 라디오
            const strat = leg.seat_strategy || 'split_ok';
            const radios = form.querySelectorAll('input[name="seat_strategy"]');
            radios.forEach(r => { r.checked = (r.value === strat); });
            updateSeatStrategyVisibility();

            document.getElementById('form-id').value = leg.id;
            // 왕복 토글은 편집에선 사용 안 함
            document.getElementById('round-trip-toggle').checked = false;
            toggleRoundTrip();
            setFormMode('edit');
            // 폼이 접혀있으면 열어줌
            const formEl = document.getElementById('watch-form');
            if (formEl.classList.contains('collapsed')) toggleForm();
            document.querySelector('.watch-form-section').scrollIntoView({behavior: 'smooth', block: 'start'});
            setFormResult(`수정 모드: ${leg.label}`, 'success');
        }

        async function createWatch(payload) {
            const res = await fetch('/api/watches', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload),
            });
            const body = await res.json();
            if (!res.ok) throw new Error(body.detail || '추가 실패');
            return body;
        }

        async function patchWatch(id, payload) {
            const res = await fetch(`/api/watches/${id}`, {
                method: 'PATCH',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(payload),
            });
            const body = await res.json();
            if (!res.ok) throw new Error(body.detail || '수정 실패');
            return body;
        }

        async function submitWatch(event) {
            event.preventDefault();
            const form = event.target;
            const data = new FormData(form);
            const isRoundTrip = document.getElementById('round-trip-toggle').checked && !editingId;

            const payload = {
                label: data.get('label') || '',
                dep: data.get('dep'),
                arr: data.get('arr'),
                date: data.get('date'),
                time_start: data.get('time_start'),
                time_end: data.get('time_end'),
                adults: Number(data.get('adults')),
                seat_strategy: data.get('seat_strategy') || 'split_ok',
            };

            try {
                if (editingId) {
                    await patchWatch(editingId, payload);
                    setFormResult('수정 완료', 'success');
                    cancelEdit();
                } else if (isRoundTrip) {
                    const returnPayload = {
                        label: (payload.label ? payload.label + ' (오는편)' : ''),
                        dep: payload.arr,
                        arr: payload.dep,
                        date: data.get('return_date'),
                        time_start: data.get('return_time_start'),
                        time_end: data.get('return_time_end'),
                        adults: payload.adults,
                        seat_strategy: payload.seat_strategy,
                    };
                    await createWatch(payload);
                    await createWatch(returnPayload);
                    setFormResult('왕복 감시 2건 추가 완료', 'success');
                    form.reset();
                    document.getElementById('round-trip-toggle').checked = false;
                    toggleRoundTrip();
                    stationsPopulated = false;
                    populateStations(window.__lastStations);
                } else {
                    await createWatch(payload);
                    setFormResult('감시 추가 완료', 'success');
                    form.reset();
                    stationsPopulated = false;
                    populateStations(window.__lastStations);
                }
                updateDashboard();
            } catch (err) {
                setFormResult(String(err.message || err), 'error');
            }
            return false;
        }

        async function deleteWatch(id, label) {
            if (!confirm(`'${label}' 감시를 삭제할까요?`)) return;
            try {
                const res = await fetch(`/api/watches/${id}`, {method: 'DELETE'});
                if (!res.ok) {
                    const body = await res.json().catch(() => ({}));
                    alert('삭제 실패: ' + (body.detail || res.status));
                    return;
                }
                if (editingId === id) cancelEdit();
                updateDashboard();
            } catch (err) {
                alert('네트워크 오류: ' + err);
            }
        }

        async function toggleWatch(id, active) {
            try {
                await patchWatch(id, {active});
                updateDashboard();
            } catch (err) {
                alert('변경 실패: ' + (err.message || err));
            }
        }

        async function resetWatch(id) {
            if (!confirm('예매 결과를 초기화하고 다시 감시할까요?')) return;
            try {
                const res = await fetch(`/api/watches/${id}/reset`, {method: 'POST'});
                if (!res.ok) {
                    const body = await res.json().catch(() => ({}));
                    alert('초기화 실패: ' + (body.detail || res.status));
                    return;
                }
                updateDashboard();
            } catch (err) {
                alert('네트워크 오류: ' + err);
            }
        }

        async function testEmail() {
            const btn = event.target;
            const originalText = btn.textContent;
            btn.disabled = true;
            btn.textContent = '발송 중...';
            
            try {
                const r = await fetch('/api/test-email', { method: 'POST' });
                const j = await r.json();
                const resultEl = document.getElementById('email-result');
                resultEl.textContent = j.ok ? '✅ 발송 성공' : '❌ 발송 실패';
                setTimeout(() => resultEl.textContent = '', 5000);
            } catch (e) {
                console.error(e);
            } finally {
                btn.disabled = false;
                btn.textContent = originalText;
            }
        }

        async function openNotifyEditor() {
            try {
                const res = await fetch('/api/settings');
                const data = await res.json();
                const emails = (data.notify_emails || []);
                document.getElementById('notify-emails-input').value = emails.join('\\n');
                document.getElementById('notify-result').textContent = '';
                document.getElementById('notify-result').className = 'form-result';
                document.getElementById('notify-modal').hidden = false;
            } catch (e) {
                alert('설정 불러오기 실패: ' + e);
            }
        }

        function closeNotifyEditor(ev) {
            if (ev && ev.target.id && ev.target.id !== 'notify-modal') return;
            document.getElementById('notify-modal').hidden = true;
        }

        async function saveNotifyEmails() {
            const raw = document.getElementById('notify-emails-input').value;
            const list = raw.split(/[\\s,]+/).map(s => s.trim()).filter(s => s.length && s.includes('@'));
            const resultEl = document.getElementById('notify-result');
            try {
                const res = await fetch('/api/settings', {
                    method: 'PATCH',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({notify_emails: list}),
                });
                const data = await res.json();
                if (!res.ok) {
                    resultEl.textContent = data.detail || '저장 실패';
                    resultEl.className = 'form-result error';
                    return;
                }
                resultEl.textContent = `저장됨 (${list.length}명)`;
                resultEl.className = 'form-result success';
                setTimeout(() => {
                    document.getElementById('notify-modal').hidden = true;
                    updateDashboard();
                }, 800);
            } catch (e) {
                resultEl.textContent = '오류: ' + e;
                resultEl.className = 'form-result error';
            }
        }

        // Initial update and interval
        updateDashboard();
        setInterval(updateDashboard, 1500);
    </script>
</body>
</html>
"""


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    return HTMLResponse(HTML)


def main() -> None:
    if AUTO_BOOKING:
        t = threading.Thread(target=worker_loop, name="srt-worker", daemon=True)
        t.start()
    else:
        STATE.worker_status = "disabled"
        STATE.login_status = "disabled"
        STATE.log("SRT 자동 예매 비활성 (SRT_AUTO_BOOKING=0) — 조회 전용 모드")
    print(f"\n  대시보드: http://{HOST}:{PORT}\n", flush=True)
    uvicorn.run(app, host=HOST, port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
