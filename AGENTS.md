# SRT Booking Project Notes

## Response / Work Style
- 기본 응답은 한국어로 작성한다.
- 실무용으로 짧고 명확하게 답한다.
- 민감정보는 절대 출력하거나 커밋하지 않는다.
- `.env`, 앱 비밀번호, SRT 계정 정보는 서버/로컬에만 둔다.

## Project
- GitHub: `https://github.com/mkt-jaehoon/srt`
- Main entrypoint: `dashboard.py`
- CLI entrypoint: `book.py`
- Runtime: Python via `uv`
- Dashboard port: `8765`

## Current Booking Logic
- 왕복 자동예매 대시보드가 `dashboard.py`에서 실행된다.
- 좌석 우선순위는 `일반석 > 특실`이다.
- 이메일 알림은 유지한다.
- 알림 수신자는 `users.json` 에 등록된 사용자 중 `notify_enabled=true` 인 사용자의 이메일 합집합이다.
- 발신 SMTP는 Gmail 계정을 사용한다.
- 예매 성공 후 결제 대기 상태로 전환한다.
- 결제 확인은 예매 후 9분 30초부터 시작한다.
- 결제 확인은 15초 간격으로 최대 3회 시도한다.
- 결제 확인 성공 시 해당 구간 감시를 종료 유지한다.
- 결제 미확인 시 해당 구간을 다시 감시 상태로 돌린다.

## Auth / Users
- 대시보드는 `/login` 페이지에서 개별 로그인이 필요하다.
- 사용자 저장소: `users.json` (gitignore). 비밀번호는 sha256+salt 로 해시.
- 세션 쿠키: `srt_session` (HMAC 서명). 비밀키는 `.session_secret` 파일 (자동 생성, gitignore).
- 초기 시드 계정: `.env` 의 `ADMIN_USERNAME` / `ADMIN_PASSWORD` (없으면 admin/admin).
- 첫 실행 시 시드 admin 1명이 자동 생성된다. 비밀번호는 첫 로그인 후 "내 계정" 모달에서 변경.
- 회원가입: 기본 활성 (`SRT_ALLOW_SIGNUP=0` 으로 차단 가능). 로그인 페이지의 '가입' 탭에서 self-signup.
- 사용자별 알림 이메일: 로그인 후 "내 계정" 모달에서 각자 설정. 자기 이메일만 변경 가능.
- 관리자(`is_admin=true`)는 `GET /api/users`, `DELETE /api/users/{id}` 가능.

## Time Window / Stop Conditions
- 시간대 옵션 API 껍데기는 구현되어 있으나, 현재 워커에는 연결하지 않았다.
- `GET /api/time-options`
- `POST /api/time-options/windows`
- `DELETE /api/time-options/windows/{window_id}`
- 수서-부산 시간대 옵션은 나중에 워커와 연결할 수 있도록 저장 구조만 있다.
- 추후 leg별 강제 종료 시각을 넣을 계획:
  - 하행: 2026-05-23 17:00 KST 이후 감시 종료
  - 상행: 2026-05-24 23:59 KST 이후 감시 종료

## NCP Server Operation
- Server IP: `49.50.136.239`
- Server user used during setup: `root`
- Server project path: `/root/srt`
- Service manager: `systemd`
- Service name: `srt-dashboard`
- Service working directory: `/root/srt`
- Service command: `/root/.local/bin/uv run python dashboard.py`
- Server dashboard listens on `127.0.0.1:8765`.
- The dashboard is not publicly exposed.
- Access dashboard from PC via PuTTY SSH tunnel:
  - Source port: `8765`
  - Destination: `127.0.0.1:8765`
  - Browser URL after tunnel: `http://127.0.0.1:8765`

## Server Commands
```bash
systemctl status srt-dashboard
journalctl -u srt-dashboard -f
systemctl restart srt-dashboard
systemctl stop srt-dashboard
```

## Deployment Flow
- Code changes should be committed and pushed to GitHub.
- On NCP server:
```bash
cd /root/srt
git pull origin main
systemctl restart srt-dashboard
```
- Do not upload code manually with WinSCP unless there is a specific reason.
- WinSCP is mainly for checking/editing server-only files such as `.env`.

## Do Not Commit
- `.env`
- any `*.env` copy
- `*:Zone.Identifier`
- `.omc/`
- `.venv/`
- `__pycache__/`
