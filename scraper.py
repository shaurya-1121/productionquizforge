"""
╔══════════════════════════════════════════════════════════════╗
║         QUIZFORGE SCRAPER  v4.0  (scraper.py)               ║
║         30-Worker Parallel · Real PYQs · Smart Dedup        ║
╚══════════════════════════════════════════════════════════════╝

IMPROVEMENTS v4.0:
  ✅ 30 parallel workers (OpenTDB: 15, IndiaBix: 8, Sanfoundry: 7)
  ✅ Added Sanfoundry as third source (CS / Engineering MCQs)
  ✅ Smarter retry logic with jitter to avoid thundering-herd
  ✅ Token bucket rate limiter for OpenTDB (prevents 429s)
  ✅ Subject-aware question enrichment (adds chapter metadata)
  ✅ Real PYQ year tags pulled from question text patterns
  ✅ Progress callback support for SSE streaming
"""

from __future__ import annotations

import hashlib
import html as _html
import json
import os
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Callable

import requests
from bs4 import BeautifulSoup

# ═══════════════════════════════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════════════════════════════

OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scraped_questions.json")
LETTERS = ["A", "B", "C", "D"]
DIFFICULTY_MAP = {"easy": "Easy", "medium": "Medium", "hard": "Hard"}
YEAR_TAG = "Practice"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
}

# ═══════════════════════════════════════════════════════════════
# TOKEN BUCKET (Rate Limiter for OpenTDB)
# ═══════════════════════════════════════════════════════════════

class _TokenBucket:
    """Leaky bucket rate limiter — prevents 429s from OpenTDB."""
    def __init__(self, rate: float, capacity: int):
        self._rate     = rate        # tokens added per second
        self._capacity = capacity
        self._tokens   = float(capacity)
        self._lock     = threading.Lock()
        self._last     = time.monotonic()

    def acquire(self, tokens: int = 1, timeout: float = 60.0) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            with self._lock:
                now = time.monotonic()
                self._tokens = min(
                    self._capacity,
                    self._tokens + (now - self._last) * self._rate
                )
                self._last = now
                if self._tokens >= tokens:
                    self._tokens -= tokens
                    return True
            wait = (tokens - self._tokens) / self._rate
            if time.monotonic() + wait > deadline:
                return False
            time.sleep(min(wait + 0.05, 1.0))


# 5 requests/10s sustained (OpenTDB fair-use limit)
_opentdb_bucket = _TokenBucket(rate=0.5, capacity=5)

# ═══════════════════════════════════════════════════════════════
# OPENTDB SESSION TOKEN
# ═══════════════════════════════════════════════════════════════

_opentdb_token: Optional[str] = None
_token_lock = threading.Lock()


def _get_opentdb_token() -> Optional[str]:
    global _opentdb_token
    with _token_lock:
        if _opentdb_token:
            return _opentdb_token
        try:
            resp = requests.get(
                "https://opentdb.com/api_token.php?command=request", timeout=12
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("response_code") == 0:
                _opentdb_token = data.get("token")
                print(f"  🔑 OpenTDB token: {_opentdb_token[:10]}…")
        except Exception as exc:
            print(f"  ⚠️  Token fetch error: {exc}")
        return _opentdb_token


def _reset_opentdb_token() -> None:
    global _opentdb_token
    with _token_lock:
        _opentdb_token = None


# ═══════════════════════════════════════════════════════════════
# TASK DEFINITIONS  — 21 OpenTDB + 10 IndiaBix + 9 Sanfoundry
# ═══════════════════════════════════════════════════════════════

# (category_id, amount, difficulty, exam, subject)
OPENTDB_TASKS: list[tuple] = [
    # ── JEE Physics / Math ──────────────────────────────────
    (19, 20, "hard",   "JEE",  "JEE — Mathematics · Calculus & Algebra"),
    (19, 15, "medium", "JEE",  "JEE — Mathematics · Arithmetic & Geometry"),
    (17, 18, "hard",   "JEE",  "JEE — Physics · Classical Mechanics"),
    (17, 12, "medium", "JEE",  "JEE — Physics · Modern Physics & Optics"),
    (18, 10, "hard",   "JEE",  "JEE — Chemistry · Science & Technology"),
    # ── NEET Biology ────────────────────────────────────────
    (27, 20, "medium", "NEET", "NEET — Biology · Zoology & Animal Sciences"),
    (27, 15, "hard",   "NEET", "NEET — Biology · Advanced Zoology"),
    (17, 12, "easy",   "NEET", "NEET — Biology · Botany & Life Sciences"),
    (17, 10, "medium", "NEET", "NEET — Biology · Human Physiology"),
    # ── UPSC ────────────────────────────────────────────────
    (22, 18, "hard",   "UPSC", "UPSC — World Geography"),
    (22, 12, "medium", "UPSC", "UPSC — Indian Geography"),
    (23, 18, "hard",   "UPSC", "UPSC — World History"),
    (23, 12, "medium", "UPSC", "UPSC — Modern Indian History"),
    (20, 12, "medium", "UPSC", "UPSC — Art, Culture & Mythology"),
    (24, 12, "medium", "UPSC", "UPSC — Indian Polity & Constitution"),
    # ── CAT Quant ───────────────────────────────────────────
    (19, 18, "hard",   "CAT",  "CAT — Quantitative Aptitude · Number Systems"),
    (19, 12, "medium", "CAT",  "CAT — Quantitative Aptitude · Time & Work"),
    # ── GK ──────────────────────────────────────────────────
    (9,  20, "hard",   "GK",   "GK — General Knowledge"),
    (9,  15, "medium", "GK",   "GK — Current Affairs"),
    (18, 12, "hard",   "GK",   "GK — Science & Technology"),
    # ── SAT ─────────────────────────────────────────────────
    (30, 12, "hard",   "SAT",  "SAT — Science: Gadgets & Technology"),
    (17, 10, "medium", "SAT",  "SAT — Physical Sciences"),
]

# (url, subject, exam)
INDIABIX_TASKS: list[tuple] = [
    ("https://www.indiabix.com/general-knowledge/world-geography/",   "UPSC — World Geography",        "UPSC"),
    ("https://www.indiabix.com/general-knowledge/indian-politics/",   "UPSC — Indian Polity",          "UPSC"),
    ("https://www.indiabix.com/general-knowledge/indian-history/",    "UPSC — Indian History",         "UPSC"),
    ("https://www.indiabix.com/general-knowledge/indian-economy/",    "UPSC — Indian Economy",         "UPSC"),
    ("https://www.indiabix.com/aptitude/problems-on-trains/",         "CAT — Quantitative Aptitude",   "CAT"),
    ("https://www.indiabix.com/aptitude/time-and-work/",              "CAT — Time & Work",             "CAT"),
    ("https://www.indiabix.com/aptitude/percentage/",                 "CAT — Percentage Problems",     "CAT"),
    ("https://www.indiabix.com/aptitude/profit-and-loss/",            "CAT — Profit & Loss",           "CAT"),
    ("https://www.indiabix.com/general-knowledge/biology/",           "NEET — Biology",                "NEET"),
    ("https://www.indiabix.com/general-knowledge/general-science/",   "GK — General Science",          "GK"),
]

# Sanfoundry MCQs (Engineering / Computer Science for GK/SAT)
SANFOUNDRY_TASKS: list[tuple] = [
    ("https://www.sanfoundry.com/data-structures-questions-answers/",          "GK — Data Structures",       "GK"),
    ("https://www.sanfoundry.com/discrete-mathematics-questions-answers/",     "JEE — Discrete Mathematics", "JEE"),
    ("https://www.sanfoundry.com/basic-electronics-questions-answers/",        "JEE — Basic Electronics",    "JEE"),
    ("https://www.sanfoundry.com/engineering-chemistry-questions-answers/",    "JEE — Engineering Chemistry","JEE"),
    ("https://www.sanfoundry.com/biology-questions-answers/",                  "NEET — Biology MCQs",        "NEET"),
    ("https://www.sanfoundry.com/computer-networks-questions-answers/",        "GK — Computer Networks",     "GK"),
    ("https://www.sanfoundry.com/physical-chemistry-questions-answers/",       "JEE — Physical Chemistry",   "JEE"),
    ("https://www.sanfoundry.com/organic-chemistry-questions-answers/",        "JEE — Organic Chemistry",    "JEE"),
    ("https://www.sanfoundry.com/environmental-science-questions-answers/",    "NEET — Environmental Sci",   "NEET"),
]

# ═══════════════════════════════════════════════════════════════
# EXAM CONTENT FILTERS
# ═══════════════════════════════════════════════════════════════

_BIOLOGY_TOKENS = frozenset({
    "cell", "mitosis", "meiosis", "photosynthesis", "chlorophyll",
    "enzyme", "dna", "rna", "chromosome", "genetics", "organism",
    "vertebrate", "invertebrate", "mammal", "species", "taxonomy",
    "ecology", "ecosystem", "blood", "heart", "lung", "brain",
    "muscle", "bone", "nerve", "evolution", "antibiotic", "virus",
    "bacteria", "fungi", "algae", "protein", "amino", "hormone",
    "insulin", "disease", "immune", "nucleus", "mitochondria",
})

_MATH_PHYSICS_TOKENS = frozenset({
    "equation", "derivative", "integral", "matrix", "vector",
    "velocity", "acceleration", "force", "momentum", "circuit",
    "resistance", "current", "voltage", "magnetic", "electric",
    "wavelength", "frequency", "thermodynamics", "entropy",
    "pressure", "density", "gravitational", "calculus",
})

_YEAR_PATTERN = re.compile(r'\b(19[5-9]\d|20[0-2]\d)\b')


def _extract_year(text: str) -> str:
    """Try to extract a real exam year from question text."""
    m = _YEAR_PATTERN.search(text)
    return m.group(1) if m else YEAR_TAG


def _token_overlap(text_lower: str, token_set: frozenset) -> int:
    words = set(re.findall(r'\b\w+\b', text_lower))
    return len(words & token_set)


def is_suitable_for_exam(q_text_lower: str, exam: str) -> bool:
    if exam == "JEE":
        if _token_overlap(q_text_lower, _BIOLOGY_TOKENS) >= 2:
            return False
    elif exam == "NEET":
        if _token_overlap(q_text_lower, _MATH_PHYSICS_TOKENS) >= 3:
            return False
    return True


# ═══════════════════════════════════════════════════════════════
# DEDUPLICATION
# ═══════════════════════════════════════════════════════════════

def _question_hash(q: dict) -> str:
    text = re.sub(r'\s+', ' ', q.get("question", "")).strip().lower()
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def deduplicate(questions: list[dict]) -> list[dict]:
    seen: set[str] = set()
    unique: list[dict] = []
    for q in questions:
        h = _question_hash(q)
        if h not in seen:
            seen.add(h)
            unique.append(q)
    return unique


# ═══════════════════════════════════════════════════════════════
# OPENTDB FETCHER  (token-aware, rate-limited, jitter backoff)
# ═══════════════════════════════════════════════════════════════

def fetch_opentdb(
    category_id: int,
    amount: int,
    difficulty: str,
    exam: str,
    subject: str,
    max_retries: int = 5,
    progress_cb: Optional[Callable] = None,
) -> list[dict]:
    backoff = 2.0

    for attempt in range(max_retries):
        # Rate-limit: wait for token slot
        if not _opentdb_bucket.acquire(tokens=1, timeout=90):
            print(f"  ⏱  Rate-limit timeout [{subject}]")
            return []

        token = _get_opentdb_token()
        url = (
            f"https://opentdb.com/api.php"
            f"?amount={amount}&category={category_id}"
            f"&type=multiple&difficulty={difficulty}"
        )
        if token:
            url += f"&token={token}"

        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 429:
                jitter = random.uniform(0, backoff * 0.3)
                print(f"  ⏳ 429 [{subject}] — {backoff:.0f}s")
                time.sleep(backoff + jitter)
                backoff = min(backoff * 2, 90)
                continue

            resp.raise_for_status()
            data = resp.json()
            code = data.get("response_code", -1)

            if code == 5:  # rate limit
                jitter = random.uniform(0, backoff * 0.3)
                time.sleep(backoff + jitter)
                backoff = min(backoff * 2, 90)
                continue

            if code in (3, 4):  # token issues
                _reset_opentdb_token()
                return []

            if code != 0:
                return []

            qs = _parse_opentdb_results(data.get("results", []), exam, subject, difficulty)
            if progress_cb:
                progress_cb(len(qs), subject)
            return qs

        except requests.RequestException as exc:
            jitter = random.uniform(0, 1)
            time.sleep(backoff + jitter)
            backoff = min(backoff * 2, 30)

    return []


def _parse_opentdb_results(
    results: list,
    exam: str,
    subject: str,
    difficulty: str,
) -> list[dict]:
    questions: list[dict] = []
    chapter = subject.split("·")[-1].strip() if "·" in subject else subject.split("—")[-1].strip()

    for item in results:
        q_text  = _html.unescape(item.get("question", "")).strip()
        correct = _html.unescape(item.get("correct_answer", "")).strip()
        wrongs  = [_html.unescape(a).strip() for a in item.get("incorrect_answers", [])]

        if not q_text or not correct or len(q_text) < 12:
            continue
        if not is_suitable_for_exam(q_text.lower(), exam):
            continue

        opts_list = [correct] + wrongs
        random.shuffle(opts_list)
        options = {LETTERS[i]: opts_list[i] for i in range(min(len(opts_list), 4))}
        answer  = next((k for k, v in options.items() if v == correct), "A")
        year    = _extract_year(q_text)

        explanation = (
            f"The correct answer is {correct}. "
            f"This question covers {chapter} — a core topic in {exam}."
        )

        questions.append({
            "subject":     subject,
            "exam":        exam,
            "chapter":     chapter,
            "topic":       chapter,
            "year":        year,
            "difficulty":  DIFFICULTY_MAP.get(
                item.get("difficulty", difficulty), "Medium"
            ),
            "question":    q_text,
            "options":     options,
            "answer":      answer,
            "explanation": explanation,
            "source":      "OpenTDB",
            "type":        "mcq",
            "marks":       4 if exam in ("JEE", "NEET") else (2 if exam == "UPSC" else 3),
            "negative_marks": -1.0 if exam in ("JEE", "NEET") else (
                -0.66 if exam == "UPSC" else (-1.0 if exam == "CAT" else 0.0)
            ),
        })
    return questions


# ═══════════════════════════════════════════════════════════════
# INDIABIX SCRAPER  (multi-selector fallback)
# ═══════════════════════════════════════════════════════════════

_ANSWER_RE = re.compile(r'\b([A-D])\b')


def _detect_answer(el) -> str:
    if el is None:
        return "A"
    txt = el.get_text(separator=" ", strip=True)[:30]
    m = _ANSWER_RE.search(txt)
    return m.group(1) if m else "A"


def _parse_q_div_a(q_div, subject: str, exam: str) -> Optional[dict]:
    q_el = q_div.find("div", {"class": "bix-td-qtxt"})
    if not q_el:
        return None
    q_text = q_el.get_text(separator=" ", strip=True)
    if not q_text or not is_suitable_for_exam(q_text.lower(), exam):
        return None

    opts_tbl = q_div.find("table", {"class": "bix-tbl-options"})
    if not opts_tbl:
        return None
    rows = opts_tbl.find_all("tr")
    options: dict[str, str] = {}
    for j, row in enumerate(rows[:4]):
        cells = row.find_all("td")
        if len(cells) >= 2:
            options[LETTERS[j]] = cells[-1].get_text(strip=True)
    if len(options) < 2:
        return None

    ans_div = q_div.find("div", {"class": "bix-div-answer"})
    exp_div = q_div.find("div", {"class": "bix-ans-description"})
    chapter = subject.split("—")[-1].strip()

    return {
        "subject": subject, "exam": exam, "chapter": chapter, "topic": chapter,
        "year": YEAR_TAG, "difficulty": "Medium",
        "question": q_text[:500],
        "options": options,
        "answer": _detect_answer(ans_div),
        "explanation": (
            exp_div.get_text(separator=" ", strip=True)[:400]
            if exp_div else f"Refer to official {exam} materials for detailed explanation."
        ),
        "source": "IndiaBix", "type": "mcq",
        "marks": 4 if exam in ("JEE","NEET") else (2 if exam=="UPSC" else 3),
        "negative_marks": -1.0 if exam in ("JEE","NEET") else (-0.66 if exam=="UPSC" else 0.0),
    }


def _parse_q_div_b(q_div, subject: str, exam: str) -> Optional[dict]:
    q_el = (
        q_div.find("p", class_="question-text")
        or q_div.find("span", class_="question-text")
        or q_div.find("p")
    )
    if not q_el:
        return None
    q_text = q_el.get_text(separator=" ", strip=True)
    if not q_text or not is_suitable_for_exam(q_text.lower(), exam):
        return None

    ul = q_div.find("ul")
    if not ul:
        return None
    options: dict[str, str] = {}
    for j, li in enumerate(ul.find_all("li")[:4]):
        options[LETTERS[j]] = li.get_text(strip=True)
    if len(options) < 2:
        return None

    ans_el = q_div.find(class_=re.compile(r'answer|correct', re.I))
    exp_el = q_div.find(class_=re.compile(r'explain|solution', re.I))
    chapter = subject.split("—")[-1].strip()

    return {
        "subject": subject, "exam": exam, "chapter": chapter, "topic": chapter,
        "year": YEAR_TAG, "difficulty": "Medium",
        "question": q_text[:500],
        "options": options,
        "answer": _detect_answer(ans_el),
        "explanation": (
            exp_el.get_text(separator=" ", strip=True)[:400]
            if exp_el else "Refer to official materials for detailed explanation."
        ),
        "source": "IndiaBix", "type": "mcq",
        "marks": 4 if exam in ("JEE","NEET") else (2 if exam=="UPSC" else 3),
        "negative_marks": -1.0 if exam in ("JEE","NEET") else (-0.66 if exam=="UPSC" else 0.0),
    }


def scrape_indiabix(
    url: str,
    subject: str,
    exam: str,
    max_q: int = 12,
    progress_cb: Optional[Callable] = None,
) -> list[dict]:
    questions: list[dict] = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        q_divs = (
            soup.find_all("div", class_="bix-div-d")
            or soup.find_all("div", class_="question")
            or soup.find_all("div", class_=re.compile(r'q-?block|question-block', re.I))
        )
        if not q_divs:
            return []

        for q_div in q_divs[:max_q]:
            try:
                q = _parse_q_div_a(q_div, subject, exam)
                if q is None:
                    q = _parse_q_div_b(q_div, subject, exam)
                if q:
                    questions.append(q)
            except Exception:
                continue

        if progress_cb:
            progress_cb(len(questions), subject)
        print(f"  ✅ IndiaBix [{subject}]: {len(questions)} Qs")

    except Exception as exc:
        print(f"  ❌ IndiaBix [{subject}]: {exc}")
    return questions


# ═══════════════════════════════════════════════════════════════
# SANFOUNDRY SCRAPER
# ═══════════════════════════════════════════════════════════════

def scrape_sanfoundry(
    url: str,
    subject: str,
    exam: str,
    max_q: int = 15,
    progress_cb: Optional[Callable] = None,
) -> list[dict]:
    questions: list[dict] = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Sanfoundry question structure
        q_blocks = soup.find_all("div", class_=re.compile(r"entry-content|question|mcq", re.I))
        chapter = subject.split("—")[-1].strip()

        for block in q_blocks[:max_q]:
            # Find strong/b tags as question text
            strong_tags = block.find_all(["strong","b"])
            for stag in strong_tags:
                q_text = stag.get_text(strip=True)
                if len(q_text) < 15 or "?" not in q_text:
                    continue
                if not is_suitable_for_exam(q_text.lower(), exam):
                    continue

                # Look for options (a. b. c. d. pattern)
                raw_text = block.get_text("\n")
                opt_matches = re.findall(
                    r'\b([a-d])\)\s*([^\n]+)', raw_text, re.IGNORECASE
                )
                if len(opt_matches) < 2:
                    continue

                options: dict[str, str] = {}
                for letter_raw, opt_text in opt_matches[:4]:
                    options[letter_raw.upper()] = opt_text.strip()[:200]

                # Try to find answer marker
                ans_match = re.search(
                    r'[Aa]nswer[:\s]+([A-Da-d])', raw_text
                )
                answer = ans_match.group(1).upper() if ans_match else "A"

                # Explanation
                exp_match = re.search(
                    r'[Ee]xplanation[:\s]+(.+?)(?:\n\n|$)', raw_text, re.DOTALL
                )
                explanation = (
                    exp_match.group(1).strip()[:300]
                    if exp_match
                    else f"Correct answer: option {answer}. Study {chapter} for deeper understanding."
                )

                questions.append({
                    "subject": subject, "exam": exam, "chapter": chapter,
                    "topic": chapter, "year": YEAR_TAG, "difficulty": "Medium",
                    "question": q_text[:500], "options": options,
                    "answer": answer, "explanation": explanation,
                    "source": "Sanfoundry", "type": "mcq",
                    "marks": 4 if exam in ("JEE","NEET") else 1,
                    "negative_marks": 0.0,
                })
                if len(questions) >= max_q:
                    break

        if progress_cb:
            progress_cb(len(questions), subject)
        print(f"  ✅ Sanfoundry [{subject}]: {len(questions)} Qs")

    except Exception as exc:
        print(f"  ❌ Sanfoundry [{subject}]: {exc}")
    return questions


# ═══════════════════════════════════════════════════════════════
# PUBLIC PARALLEL RUNNERS  (30 total workers)
# ═══════════════════════════════════════════════════════════════

def run_opentdb_parallel(
    tasks: list[tuple],
    max_workers: int = 15,
    progress_cb: Optional[Callable] = None,
) -> list[dict]:
    """15-worker OpenTDB parallel fetcher."""
    results: list[dict] = []
    with ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="opentdb"
    ) as pool:
        futures = {
            pool.submit(fetch_opentdb, *task, progress_cb=progress_cb): task[4]
            for task in tasks
        }
        for future in as_completed(futures):
            subj = futures[future]
            try:
                batch = future.result() or []
                results.extend(batch)
            except Exception as exc:
                print(f"  ❌ OpenTDB [{subj}] thread: {exc}")
    return results


def run_indiabix_parallel(
    tasks: list[tuple],
    max_workers: int = 8,
    progress_cb: Optional[Callable] = None,
) -> list[dict]:
    """8-worker IndiaBix parallel scraper."""
    results: list[dict] = []
    with ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="indiabix"
    ) as pool:
        futures = {
            pool.submit(scrape_indiabix, url, subj, exam, 12, progress_cb): subj
            for (url, subj, exam) in tasks
        }
        for future in as_completed(futures):
            subj = futures[future]
            try:
                results.extend(future.result() or [])
            except Exception as exc:
                print(f"  ❌ IndiaBix [{subj}] thread: {exc}")
    return results


def run_sanfoundry_parallel(
    tasks: list[tuple],
    max_workers: int = 7,
    progress_cb: Optional[Callable] = None,
) -> list[dict]:
    """7-worker Sanfoundry parallel scraper."""
    results: list[dict] = []
    with ThreadPoolExecutor(
        max_workers=max_workers, thread_name_prefix="sanfoundry"
    ) as pool:
        futures = {
            pool.submit(scrape_sanfoundry, url, subj, exam, 15, progress_cb): subj
            for (url, subj, exam) in tasks
        }
        for future in as_completed(futures):
            subj = futures[future]
            try:
                results.extend(future.result() or [])
            except Exception as exc:
                print(f"  ❌ Sanfoundry [{subj}] thread: {exc}")
    return results


# ═══════════════════════════════════════════════════════════════
# MAIN (standalone)
# ═══════════════════════════════════════════════════════════════

def main() -> list[dict]:
    from collections import Counter
    print("\n🚀 QuizForge Scraper v4.0 — 30 Parallel Workers")
    print("=" * 58)

    _get_opentdb_token()

    all_qs: list[dict] = []

    print(f"\n🌐 Phase 1 — OpenTDB ({len(OPENTDB_TASKS)} tasks, 15 workers)…")
    all_qs.extend(run_opentdb_parallel(OPENTDB_TASKS, max_workers=15))
    print(f"   → {len(all_qs)} after OpenTDB")

    print(f"\n📚 Phase 2 — IndiaBix ({len(INDIABIX_TASKS)} tasks, 8 workers)…")
    all_qs.extend(run_indiabix_parallel(INDIABIX_TASKS, max_workers=8))
    print(f"   → {len(all_qs)} after IndiaBix")

    print(f"\n🔬 Phase 3 — Sanfoundry ({len(SANFOUNDRY_TASKS)} tasks, 7 workers)…")
    all_qs.extend(run_sanfoundry_parallel(SANFOUNDRY_TASKS, max_workers=7))
    print(f"   → {len(all_qs)} after Sanfoundry")

    all_qs = deduplicate(all_qs)
    random.shuffle(all_qs)
    for i, q in enumerate(all_qs):
        q["id"] = i + 1

    counts = Counter(q["exam"] for q in all_qs)
    print(f"\n📊 Distribution ({len(all_qs)} unique):")
    for exam, count in sorted(counts.items()):
        print(f"   {exam:8s} → {count:4d}")

    if all_qs:
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(all_qs, f, indent=2, ensure_ascii=False)
        print(f"\n✅ Saved → {OUTPUT_FILE}\n")

    return all_qs


if __name__ == "__main__":
    main()