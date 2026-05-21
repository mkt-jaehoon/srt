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

LEGS: dict[str, dict[str, Any]] = {
    "outbound": {
        "label": "하행 (수서 → 부산)",
        "dep": "수서",
        "arr": "부산",
        "date": "20260523",
        "time_start": "140000",
        "time_end": "170000",
        "adults": 2,
    },
    "return": {
        "label": "상행 (부산 → 수서)",
        "dep": "부산",
        "arr": "수서",
        "date": "20260525",
        "time_start": "140000",
        "time_end": "190000",
        "adults": 2,
    },
}

INTERVAL = float(os.getenv("SRT_INTERVAL", "4"))
STANDBY = os.getenv("SRT_STANDBY", "0") == "1"
PORT = int(os.getenv("DASH_PORT", "8765"))
BOOKING_RETRY_AFTER_SECONDS = int(os.getenv("SRT_BOOKING_RETRY_AFTER_SECONDS", "600"))
TIME_OPTIONS_PATH = Path(os.getenv("SRT_TIME_OPTIONS_PATH", "time_options.json"))
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


def retry_at_from(captured_at: datetime) -> datetime:
    return captured_at + timedelta(seconds=BOOKING_RETRY_AFTER_SECONDS)


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def reservation_result(
    result_type: str,
    reserve_obj,
    train,
    captured_at: datetime,
) -> dict[str, Any]:
    return {
        "type": result_type,
        "summary": str(reserve_obj),
        "reservation_number": getattr(reserve_obj, "reservation_number", None),
        "payment_deadline": (
            f"{getattr(reserve_obj, 'payment_date', '')} "
            f"{getattr(reserve_obj, 'payment_time', '')}"
        ).strip(),
        "paid": bool(getattr(reserve_obj, "paid", False)),
        "train": serialize_train(train),
        "captured_at": captured_at.isoformat(timespec="seconds"),
        "retry_after_at": retry_at_from(captured_at).isoformat(timespec="seconds"),
    }


def _parse_emails(raw: str) -> list[str]:
    return [e.strip() for e in raw.split(",") if e.strip()]


NOTIFY_EMAILS: list[str] = _parse_emails(
    os.getenv("NOTIFY_EMAIL", "eksska12@naver.com")
)


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


class State:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.started_at = datetime.now().isoformat(timespec="seconds")
        self.worker_status = "starting"   # starting / running / done / error / stopped
        self.login_status = "pending"
        self.last_error: str | None = None
        self.email_status = "not-sent"
        self.legs: dict[str, LegState] = {n: LegState(n, c) for n, c in LEGS.items()}
        self.logs: deque[str] = deque(maxlen=300)

    def log(self, msg: str) -> None:
        line = f"{datetime.now():%H:%M:%S} {msg}"
        with self.lock:
            self.logs.appendleft(line)
        print(line, flush=True)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return {
                "started_at": self.started_at,
                "worker_status": self.worker_status,
                "login_status": self.login_status,
                "last_error": self.last_error,
                "email_status": self.email_status,
                "notify_emails": NOTIFY_EMAILS,
                "interval": INTERVAL,
                "standby": STANDBY,
                "legs": {
                    n: {
                        "label": leg.cfg["label"],
                        "dep": leg_route(leg.cfg)["dep"],
                        "arr": leg_route(leg.cfg)["arr"],
                        "date": leg.cfg["date"],
                        "time_start": leg.cfg["time_start"],
                        "time_end": leg.cfg["time_end"],
                        "adults": leg.cfg["adults"],
                        "attempts": leg.attempts,
                        "last_poll_at": leg.last_poll_at,
                        "last_search_ok": leg.last_search_ok,
                        "candidates": list(leg.candidates),
                        "available_count": leg.available_count,
                        "standby_count": leg.standby_count,
                        "result": leg.result,
                    }
                    for n, leg in self.legs.items()
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
    if not user or not pw:
        return False, "SMTP_USER/SMTP_PASSWORD 미설정"
    if not sender:
        return False, "SMTP_FROM 또는 SMTP_USER 없음"
    if not NOTIFY_EMAILS:
        return False, "NOTIFY_EMAIL 없음"
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(NOTIFY_EMAILS)
    msg.set_content(body)
    try:
        with smtplib.SMTP(host, port, timeout=15) as s:
            s.ehlo()
            s.starttls()
            s.login(user, pw)
            s.send_message(msg, to_addrs=NOTIFY_EMAILS)
        return True, f"sent → {len(NOTIFY_EMAILS)}명"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"


def notify_booking(leg: LegState, reserve_obj) -> None:
    train = leg.candidates  # not used; we already have train info via result
    cfg = leg.cfg
    route = leg_route(cfg)
    subject = f"[SRT] {cfg['label']} 예매 완료 — 10분 내 결제 필요"
    body = (
        f"SRT 자동 예매가 잡혔습니다. 10분 내 SRT 앱에서 결제하세요.\n\n"
        f"구간       : {route['dep']} → {route['arr']}\n"
        f"일자       : {cfg['date']}\n"
        f"인원       : 성인 {cfg['adults']}명\n"
        f"잡힌 시각  : {datetime.now():%Y-%m-%d %H:%M:%S}\n\n"
        f"예약 정보:\n{reserve_obj}\n\n"
        f"SRT 앱: https://etk.srail.kr/cmc/01/selectLoginForm.do\n"
    )
    ok, info = send_email(subject, body)
    with STATE.lock:
        STATE.email_status = (
            f"{datetime.now():%H:%M:%S} {leg.name}: " + ("OK" if ok else f"FAIL ({info})")
        )
    STATE.log(
        f"email → {', '.join(NOTIFY_EMAILS)}: "
        f"{'OK' if ok else 'FAIL ' + info}"
    )


def try_leg(srt: SRT, leg: LegState) -> bool:
    """Returns True 이면 leg 종료(예매 성공)."""
    cfg = leg.cfg
    route = leg_route(cfg)
    leg.attempts += 1
    try:
        trains = srt.search_train(
            route["dep"], route["arr"],
            date=cfg["date"], time=cfg["time_start"],
            available_only=False,
        )
        leg.last_search_ok = True
    except SRTError as e:
        leg.last_search_ok = False
        STATE.last_error = f"{leg.name} search: {e}"
        STATE.log(f"[{leg.name}] search 실패: {e}")
        return False

    leg.last_poll_at = datetime.now().isoformat(timespec="seconds")
    candidates = sorted(
        [t for t in trains if in_window(t, cfg)], key=lambda t: t.dep_time
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

    passengers = [Adult(cfg["adults"])]

    for t, seat_label, seat_type in [
        *[(t, "일반석", SeatType.GENERAL_ONLY) for t in general_available],
        *[(t, "특실", SeatType.SPECIAL_ONLY) for t in special_only_available],
    ]:
        STATE.log(
            f"[{leg.name}] {seat_label} 예매 시도 {t.dep_time[:2]}:{t.dep_time[2:4]} "
            f"{t.train_number}"
        )
        try:
            r = srt.reserve(t, passengers=passengers, special_seat=seat_type)
            leg.result = reservation_result("reserve", r, t, datetime.now())
            STATE.log(f"[{leg.name}] [SUCCESS] 예매 완료")
            notify_booking(leg, r)
            return True
        except SRTResponseError as e:
            STATE.log(f"[{leg.name}]   실패: {e}")

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
                leg.result = reservation_result("standby", r, t, datetime.now())
                STATE.log(f"[{leg.name}] [STANDBY] 예약대기 등록")
                notify_booking(leg, r)
                return True
            except SRTResponseError as e:
                STATE.log(f"[{leg.name}]   대기 실패: {e}")

    return False


def paid_reservation_numbers(srt: SRT) -> set[str]:
    return {
        str(r.reservation_number)
        for r in srt.get_reservations(paid_only=True)
        if getattr(r, "reservation_number", None)
    }


def refresh_booking_results(srt: SRT) -> None:
    now = datetime.now()
    due_legs: list[LegState] = []
    with STATE.lock:
        for leg in STATE.legs.values():
            result = leg.result
            if not result or result.get("type") != "reserve" or result.get("paid"):
                continue

            retry_after = parse_iso_datetime(result.get("retry_after_at"))
            if retry_after is None:
                retry_after = retry_at_from(
                    parse_iso_datetime(result.get("captured_at")) or now
                )
                result["retry_after_at"] = retry_after.isoformat(timespec="seconds")

            if now >= retry_after:
                due_legs.append(leg)

    if not due_legs:
        return

    try:
        paid_numbers = paid_reservation_numbers(srt)
    except SRTError as e:
        paid_numbers = set()
        STATE.last_error = f"payment check: {e}"
        STATE.log(f"결제 확인 실패: {e} — 미결제로 보고 감시 재개")

    for leg in due_legs:
        paid_confirmed = False
        with STATE.lock:
            result = leg.result
            if not result:
                continue
            reservation_number = result.get("reservation_number")
            if reservation_number and str(reservation_number) in paid_numbers:
                result["paid"] = True
                result["payment_checked_at"] = datetime.now().isoformat(
                    timespec="seconds"
                )
                paid_confirmed = True
            else:
                leg.result = None
                leg.available_count = 0
                leg.standby_count = 0

        if paid_confirmed:
            STATE.log(f"[{leg.name}] 결제 완료 확인 — 해당 구간 감시 종료 유지")
            continue

        STATE.log(f"[{leg.name}] 10분 내 결제 미확인 — 다시 감시 시작")


def leg_is_final(leg: LegState) -> bool:
    if not leg.result:
        return False
    return leg.result.get("type") == "standby" or bool(leg.result.get("paid"))


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
        refresh_booking_results(srt)
        pending_legs = [leg for leg in STATE.legs.values() if leg.result is None]
        if not pending_legs:
            if all(leg_is_final(leg) for leg in STATE.legs.values()):
                STATE.worker_status = "done"
                STATE.log("모든 leg 처리 완료 — 워커 종료")
                return

            STATE.worker_status = "waiting_payment"
            time.sleep(min(INTERVAL, 10))
            continue

        STATE.worker_status = "running"

        for leg in pending_legs:
            if STOP.is_set():
                break
            done = try_leg(srt, leg)
            if done:
                continue
            time.sleep(0.5)   # leg 간 짧은 간격

        # 사이클 사이 폴링 간격
        if any(leg.result is None for leg in STATE.legs.values()):
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
    return JSONResponse({"ok": ok, "info": info, "to": NOTIFY_EMAILS})


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
    <style>
        :root {
            --primary: #6a1212; /* SRT Burgundy */
            --primary-light: #8e1a1a;
            --secondary: #475569;
            --success: #10b981;
            --warning: #f59e0b;
            --danger: #ef4444;
            --bg: #f8fafc;
            --card-bg: #ffffff;
            --text-main: #1e293b;
            --text-muted: #64748b;
            --border: #e2e8f0;
        }

        * { box-sizing: border-box; }
        body {
            font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
            margin: 0;
            padding: 0;
            background-color: var(--bg);
            color: var(--text-main);
            line-height: 1.5;
        }

        .container {
            max-width: 1000px;
            margin: 0 auto;
            padding: 2rem 1rem;
        }

        header {
            display: flex;
            justify-content: space-between;
            align-items: flex-end;
            margin-bottom: 2rem;
            border-bottom: 2px solid var(--primary);
            padding-bottom: 1rem;
        }

        .header-title h1 {
            font-size: 1.875rem;
            font-weight: 800;
            margin: 0;
            color: var(--primary);
            letter-spacing: -0.025em;
        }

        .header-info {
            font-size: 0.875rem;
            color: var(--text-muted);
            margin-top: 0.5rem;
        }

        .btn {
            background-color: white;
            border: 1px solid var(--border);
            padding: 0.5rem 1rem;
            border-radius: 0.5rem;
            font-size: 0.875rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.2s;
            color: var(--secondary);
        }

        .btn:hover {
            background-color: var(--bg);
            border-color: var(--text-muted);
        }

        .btn-primary {
            background-color: var(--primary);
            color: white;
            border: none;
        }

        .btn-primary:hover {
            background-color: var(--primary-light);
        }

        .status-banner {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }

        .status-card {
            background: var(--card-bg);
            padding: 1rem;
            border-radius: 0.75rem;
            border: 1px solid var(--border);
            box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        }

        .status-card h4 {
            margin: 0;
            font-size: 0.75rem;
            text-transform: uppercase;
            color: var(--text-muted);
            letter-spacing: 0.05em;
        }

        .status-card .value {
            font-size: 1.25rem;
            font-weight: 700;
            margin-top: 0.25rem;
        }

        .leg-container {
            display: flex;
            flex-direction: column;
            gap: 2rem;
        }

        .leg-card {
            background: var(--card-bg);
            border-radius: 1rem;
            border: 1px solid var(--border);
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
            overflow: hidden;
        }

        .leg-header {
            padding: 1.5rem;
            background: linear-gradient(to right, #fcfcfc, #f8fafc);
            border-bottom: 1px solid var(--border);
            display: flex;
            justify-content: space-between;
            align-items: center;
        }

        .leg-title h2 {
            margin: 0;
            font-size: 1.25rem;
            font-weight: 700;
        }

        .leg-subtitle {
            font-size: 0.875rem;
            color: var(--text-muted);
            margin-top: 0.25rem;
        }

        .leg-stats {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
            gap: 1rem;
            padding: 1.5rem;
            background: #fff;
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

        @media (max-width: 640px) {
            .header-bar { flex-direction: column; align-items: flex-start; }
            .leg-header { flex-direction: column; align-items: flex-start; gap: 1rem; }
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div class="header-title">
                <h1>SRT Smart Booking</h1>
                <div class="header-info" id="global-status">연결 중...</div>
            </div>
            <div style="display: flex; gap: 0.5rem; align-items: center;">
                <span id="email-result" style="font-size: 0.75rem; color: var(--text-muted);"></span>
                <button class="btn" onclick="testEmail()">알림 테스트</button>
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
            <div class="status-card">
                <h4>알림 대상</h4>
                <div class="value" id="notify-info" style="font-size: 0.875rem;">-</div>
            </div>
        </div>

        <div id="legs" class="leg-container"></div>

        <div class="log-section">
            <div class="log-header">
                <h3 style="margin:0; font-size: 1.125rem; font-weight: 700;">실시간 로그</h3>
                <span style="font-size: 0.75rem; color: var(--text-muted);">최근 300개 항목</span>
            </div>
            <pre id="logs"></pre>
        </div>
    </div>

    <script>
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
                document.getElementById('notify-info').textContent = (state.notify_emails || []).join(', ') || '없음';

                // Error Banner
                const bannerContainer = document.getElementById('banner-container');
                bannerContainer.innerHTML = '';
                if (state.worker_status === 'error') {
                    const div = document.createElement('div');
                    div.className = 'error-banner';
                    div.innerHTML = `<span>⚠️</span> <div><strong>워커 오류:</strong> ${state.last_error || '알 수 없는 오류'}</div>`;
                    bannerContainer.appendChild(div);
                }

                // Render Legs
                const legsRoot = document.getElementById('legs');
                legsRoot.innerHTML = '';
                
                for (const [id, leg] of Object.entries(state.legs)) {
                    const card = document.createElement('div');
                    card.className = 'leg-card';
                    
                    let resultHtml = '';
                    if (leg.result) {
                        const isStandby = leg.result.type === 'standby';
                        resultHtml = `
                            <div class="result-banner ${isStandby ? 'standby' : ''}">
                                <div class="result-header">
                                    <span>${isStandby ? '⏳ 예약대기 등록됨' : '✅ 예매 완료'}</span>
                                    <span style="font-weight: 400; font-size: 0.875rem; color: var(--text-muted); margin-left: auto;">
                                        ${leg.result.captured_at.slice(11, 19)}
                                    </span>
                                </div>
                                <div style="font-size: 0.9375rem; font-weight: 600;">
                                    ${leg.result.train.dep_time} ${leg.result.train.dep} → ${leg.result.train.arr} (${leg.result.train.train_number})
                                </div>
                                <div style="font-size: 0.8125rem; color: var(--secondary); margin-top: 0.25rem;">
                                    10분 이내에 SRT 앱에서 결제를 완료해야 합니다.
                                </div>
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

                    card.innerHTML = `
                        <div class="leg-header">
                            <div class="leg-title">
                                <h2>${leg.label}</h2>
                                <div class="leg-subtitle">${leg.dep} ➔ ${leg.arr} | ${leg.date} | ${leg.adults}명</div>
                            </div>
                            <div class="badge badge-muted">${leg.time_start.slice(0,2)}:${leg.time_start.slice(2,4)} ~ ${leg.time_end.slice(0,2)}:${leg.time_end.slice(2,4)}</div>
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
    t = threading.Thread(target=worker_loop, name="srt-worker", daemon=True)
    t.start()
    print(f"\n  대시보드: http://127.0.0.1:{PORT}\n", flush=True)
    uvicorn.run(app, host="127.0.0.1", port=PORT, log_level="warning")


if __name__ == "__main__":
    main()
