"""
╔══════════════════════════════════════════════════════════════╗
║         QUIZFORGE BACKEND  v4.0  (app.py)                   ║
║         Flask · SSE · 30-Worker Parallel · Pagination       ║
╚══════════════════════════════════════════════════════════════╝
"""

from __future__ import annotations

import json
import os
import queue
import random
import re
import threading
import time
from functools import wraps
from typing import Generator

from flask import Flask, Response, jsonify, request, send_from_directory

try:
    from flask_compress import Compress
    _HAS_COMPRESS = True
except ImportError:
    _HAS_COMPRESS = False

try:
    from scraper import (
        INDIABIX_TASKS,
        OPENTDB_TASKS,
        SANFOUNDRY_TASKS,
        deduplicate,
        run_opentdb_parallel,
        run_indiabix_parallel,
        run_sanfoundry_parallel,
    )
    _SCRAPER_AVAILABLE = True
    _scraper_err_msg = ""
except ImportError as _e:
    _SCRAPER_AVAILABLE = False
    _scraper_err_msg = str(_e)

# ═══════════════════════════════════════════════════════════════
# APP SETUP
# ═══════════════════════════════════════════════════════════════

BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")

_DB_CANDIDATES = [
    os.path.join(BASE_DIR, "pyq_full_database.json"),
    os.path.join(BASE_DIR, "scraped_questions.json"),
]

app = Flask(__name__, static_folder=STATIC_DIR, static_url_path="/static")
app.config["JSON_SORT_KEYS"] = False

if _HAS_COMPRESS:
    Compress(app)

VALID_EXAMS = frozenset({"ALL", "JEE", "NEET", "UPSC", "CAT", "SAT", "GK"})

# ═══════════════════════════════════════════════════════════════
# STATE
# ═══════════════════════════════════════════════════════════════

_scrape_lock = threading.Lock()
_scraping    = False

_cache: dict = {
    "questions": [],
    "loaded_at": 0.0,
    "ttl_seconds": 300,
}
_cache_lock = threading.Lock()

_sse_clients: list[queue.Queue] = []
_sse_lock = threading.Lock()

# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════

def _broadcast(event: str, data) -> None:
    if isinstance(data, dict):
        data = json.dumps(data, ensure_ascii=False)
    payload = f"event: {event}\ndata: {data}\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.put_nowait(payload)
            except queue.Full:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)


def _invalidate_cache() -> None:
    with _cache_lock:
        _cache["loaded_at"] = 0.0


def _load_from_disk() -> list:
    for path in _DB_CANDIDATES:
        if os.path.exists(path):
            try:
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                app.logger.info("Loaded %d questions from %s", len(data), path)
                return data
            except Exception as exc:
                app.logger.warning("Could not load %s: %s", path, exc)
    return []


def _get_questions(force_reload: bool = False) -> list:
    now = time.monotonic()
    with _cache_lock:
        age = now - _cache["loaded_at"]
        if force_reload or age > _cache["ttl_seconds"]:
            _cache["questions"] = _load_from_disk()
            _cache["loaded_at"] = now
        return list(_cache["questions"])


def _save_to_disk(questions: list) -> None:
    path = os.path.join(BASE_DIR, "scraped_questions.json")
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(questions, f, indent=2, ensure_ascii=False)
    except Exception as exc:
        app.logger.error("Save error: %s", exc)
        return
    with _cache_lock:
        _cache["questions"] = list(questions)
        _cache["loaded_at"] = time.monotonic()


def _sanitise_exam(raw: str) -> str:
    upper = (raw or "ALL").strip().upper()
    return upper if upper in VALID_EXAMS else "ALL"


def require_scraper(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not _SCRAPER_AVAILABLE:
            return jsonify({
                "status": "error",
                "message": f"Scraper unavailable: {_scraper_err_msg}"
            }), 503
        return f(*args, **kwargs)
    return wrapper


# ═══════════════════════════════════════════════════════════════
# SCRAPE PIPELINE  (30 workers: 15 + 8 + 7)
# ═══════════════════════════════════════════════════════════════

def _run_scrape_pipeline() -> list:
    questions: list = []

    def cb(count, subject):
        _broadcast("progress", {
            "stage": "fetch",
            "pct": min(85, 5 + len(questions) // 10),
            "msg": f"✅ {subject}: {count} questions",
            "fetched": len(questions) + count,
        })

    _broadcast("progress", {
        "stage": "start", "pct": 2,
        "msg": f"🚀 Pipeline starting — 30 parallel workers…"
    })

    # Phase 1 — OpenTDB (15 workers)
    _broadcast("progress", {
        "stage": "opentdb", "pct": 5,
        "msg": f"🌐 OpenTDB — {len(OPENTDB_TASKS)} tasks · 15 workers…"
    })
    otdb = run_opentdb_parallel(OPENTDB_TASKS, max_workers=15, progress_cb=cb)
    questions.extend(otdb)
    _broadcast("progress", {
        "stage": "opentdb_done", "pct": 55,
        "msg": f"✅ OpenTDB — {len(otdb)} questions",
        "fetched": len(questions),
    })

    # Phase 2 — IndiaBix (8 workers)
    _broadcast("progress", {
        "stage": "indiabix", "pct": 58,
        "msg": f"📚 IndiaBix — {len(INDIABIX_TASKS)} tasks · 8 workers…"
    })
    ibix = run_indiabix_parallel(INDIABIX_TASKS, max_workers=8, progress_cb=cb)
    questions.extend(ibix)
    _broadcast("progress", {
        "stage": "indiabix_done", "pct": 75,
        "msg": f"✅ IndiaBix — {len(ibix)} questions",
        "fetched": len(questions),
    })

    # Phase 3 — Sanfoundry (7 workers)
    _broadcast("progress", {
        "stage": "sanfoundry", "pct": 78,
        "msg": f"🔬 Sanfoundry — {len(SANFOUNDRY_TASKS)} tasks · 7 workers…"
    })
    sfy = run_sanfoundry_parallel(SANFOUNDRY_TASKS, max_workers=7, progress_cb=cb)
    questions.extend(sfy)
    _broadcast("progress", {
        "stage": "sanfoundry_done", "pct": 90,
        "msg": f"✅ Sanfoundry — {len(sfy)} questions",
        "fetched": len(questions),
    })

    # Finalise
    _broadcast("progress", {"stage": "finalise", "pct": 95,
                             "msg": "🔧 Deduplicating & assigning IDs…"})
    questions = deduplicate(questions)
    random.shuffle(questions)
    for i, q in enumerate(questions):
        q["id"] = i + 1

    _save_to_disk(questions)

    _broadcast("progress", {
        "stage": "done", "pct": 100,
        "msg": f"✅ Complete — {len(questions)} unique questions saved",
        "total": len(questions),
    })
    _broadcast("done", {"count": len(questions), "questions": questions})
    return questions


def _scrape_thread_target() -> None:
    global _scraping
    try:
        _run_scrape_pipeline()
    except Exception as exc:
        app.logger.exception("Pipeline error: %s", exc)
        _broadcast("error", {"msg": str(exc)})
    finally:
        with _scrape_lock:
            _scraping = False


# ═══════════════════════════════════════════════════════════════
# STARTUP — Pre-warm cache
# ═══════════════════════════════════════════════════════════════

def _prewarm_cache() -> None:
    try:
        qs = _get_questions(force_reload=True)
        app.logger.info("Cache pre-warmed: %d questions", len(qs))
    except Exception as exc:
        app.logger.warning("Pre-warm failed: %s", exc)


threading.Thread(target=_prewarm_cache, daemon=True, name="prewarm").start()


# ═══════════════════════════════════════════════════════════════
# ROUTES
# ═══════════════════════════════════════════════════════════════

@app.route("/")
def index():
    return send_from_directory(BASE_DIR, "index.html")


@app.route("/api/questions", methods=["GET"])
def get_questions():
    questions = _get_questions()
    exam_param = _sanitise_exam(request.args.get("exam", "ALL"))
    if exam_param != "ALL":
        questions = [q for q in questions if q.get("exam","").upper() == exam_param]

    chapter_param = (request.args.get("chapter") or "").strip().lower()
    if chapter_param:
        questions = [q for q in questions
                     if chapter_param in (q.get("subject") or "").lower()
                     or chapter_param in (q.get("chapter") or "").lower()]

    diff_param = (request.args.get("difficulty") or "").strip().title()
    if diff_param in {"Easy", "Medium", "Hard"}:
        questions = [q for q in questions if q.get("difficulty") == diff_param]

    search_q = (request.args.get("q") or "").strip().lower()
    if search_q:
        questions = [q for q in questions if search_q in q.get("question","").lower()]

    raw_limit = request.args.get("limit","")
    if raw_limit.isdigit():
        questions = questions[:int(raw_limit)]

    total = len(questions)

    try:
        page     = max(1, int(request.args.get("page","1")))
        per_page = min(200, max(1, int(request.args.get("per_page","50"))))
    except ValueError:
        page, per_page = 1, 50

    start = (page-1)*per_page
    page_questions = questions[start:start+per_page]

    return jsonify({
        "status":      "success",
        "count":       len(page_questions),
        "total":       total,
        "page":        page,
        "per_page":    per_page,
        "total_pages": (total+per_page-1)//per_page if per_page else 1,
        "questions":   page_questions,
        "scraped":     total > 0,
    })


@app.route("/api/random", methods=["GET"])
def get_random():
    questions = _get_questions()
    exam_param = _sanitise_exam(request.args.get("exam","ALL"))
    if exam_param != "ALL":
        questions = [q for q in questions if q.get("exam","").upper() == exam_param]
    diff_param = (request.args.get("difficulty") or "").strip().title()
    if diff_param in {"Easy","Medium","Hard"}:
        questions = [q for q in questions if q.get("difficulty") == diff_param]
    if not questions:
        return jsonify({"status":"error","message":"No questions match."}), 404
    return jsonify({"status":"success","question":random.choice(questions)})


@app.route("/api/quiz", methods=["GET"])
def get_quiz():
    """Return a ready-to-play set of N questions (default 20)."""
    questions = _get_questions()
    exam_param = _sanitise_exam(request.args.get("exam","ALL"))
    if exam_param != "ALL":
        questions = [q for q in questions if q.get("exam","").upper() == exam_param]
    diff_param = (request.args.get("difficulty") or "").strip().title()
    if diff_param in {"Easy","Medium","Hard"}:
        questions = [q for q in questions if q.get("difficulty") == diff_param]
    n = min(100, max(5, int(request.args.get("n","20"))))
    sample = random.sample(questions, min(n, len(questions)))
    return jsonify({"status":"success","count":len(sample),"questions":sample})


@app.route("/api/exams", methods=["GET"])
def get_exams():
    questions = _get_questions()
    exams = sorted(VALID_EXAMS & {q.get("exam","GK") for q in questions})
    return jsonify({"exams": ["ALL"]+exams})


@app.route("/api/chapters", methods=["GET"])
def get_chapters():
    questions = _get_questions()
    exam_param = _sanitise_exam(request.args.get("exam","ALL"))
    if exam_param != "ALL":
        questions = [q for q in questions if q.get("exam","").upper() == exam_param]
    subjects = sorted({q.get("subject") or q.get("chapter") or "General" for q in questions})
    return jsonify({"chapters":subjects})


@app.route("/api/stats", methods=["GET"])
def get_stats():
    questions = _get_questions()
    from collections import Counter
    by_exam = Counter(q.get("exam","GK") for q in questions)
    by_diff = Counter(q.get("difficulty","Medium") for q in questions)
    by_src  = Counter(q.get("source","Unknown") for q in questions)
    return jsonify({
        "total":       len(questions),
        "by_exam":     dict(by_exam),
        "by_difficulty":dict(by_diff),
        "by_source":   dict(by_src),
    })


@app.route("/api/status", methods=["GET"])
def status():
    questions = _get_questions()
    with _cache_lock:
        age = time.monotonic() - _cache["loaded_at"]
    return jsonify({
        "scraping":       _scraping,
        "question_count": len(questions),
        "has_questions":  len(questions) > 0,
        "cache_age_s":    round(age, 1),
        "scraper_ready":  _SCRAPER_AVAILABLE,
    })


@app.route("/api/scrape", methods=["POST"])
@require_scraper
def trigger_scrape():
    global _scraping
    with _scrape_lock:
        if _scraping:
            return jsonify({"status":"pending","message":"Already running."}), 202
        _scraping = True
    _invalidate_cache()
    threading.Thread(target=_scrape_thread_target, daemon=True, name="scraper").start()
    return jsonify({
        "status":  "started",
        "message": "30-worker scrape started. Connect to /api/scrape/stream",
    }), 202


@app.route("/api/scrape/stream", methods=["GET"])
@require_scraper
def scrape_stream():
    client_q: queue.Queue = queue.Queue(maxsize=300)

    def generate() -> Generator[str, None, None]:
        with _sse_lock:
            _sse_clients.append(client_q)
        if not _scraping:
            existing = _get_questions()
            if existing:
                yield (
                    f"event: done\n"
                    f"data: {json.dumps({'count':len(existing),'questions':existing},ensure_ascii=False)}\n\n"
                )
                with _sse_lock:
                    if client_q in _sse_clients:
                        _sse_clients.remove(client_q)
                return
        try:
            while True:
                try:
                    payload = client_q.get(timeout=20)
                    yield payload
                    if payload.startswith("event: done") or payload.startswith("event: error"):
                        break
                except queue.Empty:
                    yield "event: heartbeat\ndata: \n\n"
        finally:
            with _sse_lock:
                if client_q in _sse_clients:
                    _sse_clients.remove(client_q)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={"Cache-Control":"no-cache","X-Accel-Buffering":"no"},
    )


@app.route("/api/download/json", methods=["GET"])
def download_json():
    questions  = _get_questions()
    exam_param = _sanitise_exam(request.args.get("exam","ALL"))
    if exam_param != "ALL":
        questions = [q for q in questions if q.get("exam","").upper() == exam_param]
    payload = json.dumps({
        "generated": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "exam": exam_param, "count": len(questions), "questions": questions,
    }, indent=2, ensure_ascii=False)
    return Response(payload, mimetype="application/json", headers={
        "Content-Disposition": f'attachment; filename="QuizForge_{exam_param}.json"'
    })


@app.after_request
def add_cors(response):
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Accept"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


@app.route("/api/<path:_>", methods=["OPTIONS"])
def options(_):
    return Response(status=204, headers={
        "Access-Control-Allow-Origin":"*",
        "Access-Control-Allow-Headers":"Content-Type, Accept",
        "Access-Control-Allow-Methods":"GET, POST, OPTIONS",
    })


if __name__ == "__main__":
    os.makedirs(STATIC_DIR, exist_ok=True)
    print("\n🚀 QuizForge v4.0 — 30-Worker Parallel Engine")
    print(f"   Frontend  : http://localhost:5000")
    print(f"   Questions : http://localhost:5000/api/questions")
    print(f"   Scrape    : POST http://localhost:5000/api/scrape")
    print(f"   Stream    : GET  http://localhost:5000/api/scrape/stream")
    print(f"   Compress  : {'✅' if _HAS_COMPRESS else '❌'}")
    print(f"   Scraper   : {'✅' if _SCRAPER_AVAILABLE else '❌ '+_scraper_err_msg}\n")
    app.run(debug=True, port=5000, threaded=True)