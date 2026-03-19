#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import re
import sqlite3
import sys
import time
from contextlib import closing, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urljoin, urlparse

import subprocess

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
]

SEC_CH_UA_MAP = {
    "Chrome/134": '"Chromium";v="134", "Google Chrome";v="134", "Not:A-Brand";v="99"',
    "Chrome/133": '"Chromium";v="133", "Google Chrome";v="133", "Not:A-Brand";v="99"',
    "Edg/133": '"Chromium";v="133", "Microsoft Edge";v="133", "Not:A-Brand";v="99"',
}


BASE_URL = "https://hrs.isr.umich.edu"
LIST_URL = f"{BASE_URL}/publications/biblio"
FILTER_PARAMS = {"f[type]": "102"}  # Journal Article


def build_list_page_url(page_num: int) -> str:
    """
    Build list page URL with deterministic query ordering.

    Desired format:
      https://hrs.isr.umich.edu/publications/biblio?page=261&f%5Btype%5D=102
    """
    type_key, type_val = next(iter(FILTER_PARAMS.items()))
    type_key_encoded = quote(type_key, safe="")
    return f"{LIST_URL}?page={page_num}&{type_key_encoded}={type_val}"

DEFAULT_DB_PATH = "hrs_scraper.db"
DEFAULT_JSONL = "hrs_abstracts.jsonl"
DEFAULT_CSV = "hrs_abstracts.csv"
DEFAULT_LOG_FILE = "scraper.log"

DEFAULT_REQUEST_TIMEOUT = 30
DEFAULT_DETAIL_DELAY = 1.5
DEFAULT_LIST_DELAY = 2.5
DEFAULT_MAX_RETRIES = 5
DEFAULT_JITTER_MIN = 0.5
DEFAULT_JITTER_MAX = 2.0

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS articles (
    article_id TEXT PRIMARY KEY,
    url TEXT NOT NULL,
    title TEXT,
    publication_type TEXT,
    year TEXT,
    authors TEXT,
    journal TEXT,
    volume TEXT,
    pagination TEXT,
    date_published TEXT,
    issn TEXT,
    keywords TEXT,
    abstract TEXT,
    doi TEXT,
    citation_key TEXT,
    pubmed_id TEXT,
    title_h1 TEXT,
    scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS failed_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_type TEXT NOT NULL,         -- list | detail
    url TEXT NOT NULL,
    reason TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS crawl_checkpoint (
    id INTEGER PRIMARY KEY CHECK (id = 1),   -- singleton row
    last_list_page INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_articles_year ON articles(year);
CREATE INDEX IF NOT EXISTS idx_articles_doi ON articles(doi);
CREATE INDEX IF NOT EXISTS idx_failed_pages_type ON failed_pages(page_type);
"""

UPSERT_SQL = """
INSERT INTO articles (
    article_id, url, title, publication_type, year, authors, journal, volume,
    pagination, date_published, issn, keywords, abstract, doi, citation_key,
    pubmed_id, title_h1, scraped_at, updated_at
) VALUES (
    :article_id, :url, :title, :publication_type, :year, :authors, :journal, :volume,
    :pagination, :date_published, :issn, :keywords, :abstract, :doi, :citation_key,
    :pubmed_id, :title_h1, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
)
ON CONFLICT(article_id) DO UPDATE SET
    url = excluded.url,
    title = excluded.title,
    publication_type = excluded.publication_type,
    year = excluded.year,
    authors = excluded.authors,
    journal = excluded.journal,
    volume = excluded.volume,
    pagination = excluded.pagination,
    date_published = excluded.date_published,
    issn = excluded.issn,
    keywords = excluded.keywords,
    abstract = excluded.abstract,
    doi = excluded.doi,
    citation_key = excluded.citation_key,
    pubmed_id = excluded.pubmed_id,
    title_h1 = excluded.title_h1,
    updated_at = CURRENT_TIMESTAMP
"""

ARTICLE_HREF_RE = re.compile(r"^/publications/biblio/(\d+)/?$")


def canonicalize_article_url(href_or_url: str) -> Optional[str]:
    """
    Normalize a candidate bibliography detail URL to a canonical form.

    This prevents brittle matching when list pages provide relative links,
    absolute URLs, trailing slashes, and/or query strings/fragments.
    """
    full_url = urljoin(BASE_URL, href_or_url.strip())
    parsed = urlparse(full_url)
    m = ARTICLE_HREF_RE.match(parsed.path)
    if not m:
        return None
    article_id = m.group(1)
    return f"{BASE_URL}/publications/biblio/{article_id}"


LABEL_MAP = {
    "title": ["title"],
    "publication_type": ["publication type"],
    "year": ["year of publication", "year"],
    "authors": ["authors", "author(s)"],
    "journal": ["journal"],
    "volume": ["volume"],
    "pagination": ["pagination", "pages"],
    "date_published": ["date published", "publication date"],
    "issn": ["issn number", "issn"],
    "keywords": ["keywords", "keyword(s)"],
    "abstract": ["abstract"],
    "doi": ["doi"],
    "citation_key": ["citation key"],
    "pubmed_id": ["pubmed id", "pmid"],
}
NORMALIZED_FIELD_LOOKUP = {
    alias: key
    for key, aliases in LABEL_MAP.items()
    for alias in aliases
}


@dataclass
class Config:
    db_path: str
    jsonl_path: str
    csv_path: str
    log_file: str
    request_timeout: int
    detail_delay: float
    list_delay: float
    max_retries: int
    jitter_min: float
    jitter_max: float
    use_browser: bool = True
    browser_headless: bool = False
    chrome_profile_dir: Optional[str] = None


@dataclass
class ScrapeStats:
    list_pages_seen: int = 0
    list_pages_failed: int = 0
    article_links_seen: int = 0
    article_pages_attempted: int = 0
    article_pages_inserted: int = 0
    article_pages_updated: int = 0
    article_pages_failed: int = 0
    suspicious_records: int = 0


log = logging.getLogger("hrs_scraper_cli")


def setup_logging(log_file: str, verbose: bool = False) -> None:
    log.setLevel(logging.DEBUG if verbose else logging.INFO)
    log.handlers.clear()

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG if verbose else logging.INFO)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(logging.DEBUG if verbose else logging.INFO)

    log.addHandler(fh)
    log.addHandler(sh)


def _pick_ua_and_headers() -> dict[str, str]:
    """Pick a random User-Agent and build matching browser-like headers."""
    ua = random.choice(USER_AGENTS)
    headers = {
        "User-Agent": ua,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "DNT": "1",
        "Cache-Control": "max-age=0",
    }
    # Add sec-ch-ua headers for Chromium-based UAs
    for key, value in SEC_CH_UA_MAP.items():
        if key in ua:
            headers["Sec-Ch-Ua"] = value
            headers["Sec-Ch-Ua-Mobile"] = "?0"
            headers["Sec-Ch-Ua-Platform"] = '"macOS"' if "Mac" in ua else '"Windows"'
            headers["Sec-Fetch-Dest"] = "document"
            headers["Sec-Fetch-Mode"] = "navigate"
            headers["Sec-Fetch-Site"] = "same-origin"
            headers["Sec-Fetch-User"] = "?1"
            break
    return headers


def build_session(cfg: Config) -> requests.Session:
    session = requests.Session()

    retry = Retry(
        total=cfg.max_retries,
        connect=cfg.max_retries,
        read=cfg.max_retries,
        status=cfg.max_retries,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        backoff_factor=2.0,
        respect_retry_after_header=True,
        raise_on_status=False,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    session.mount("http://", adapter)
    session.mount("https://", adapter)

    session.headers.update(_pick_ua_and_headers())
    return session

def warm_up_session(
    session: requests.Session,
    cfg: Config,
    browser_fetcher: Optional["BrowserHTMLFetcher"] = None,
) -> None:
    """Simulate a real user navigating to the bibliography from the homepage."""
    warm_urls = [
        "https://hrs.isr.umich.edu/",
        "https://hrs.isr.umich.edu/publications",
        "https://hrs.isr.umich.edu/publications/biblio",
    ]
    for url in warm_urls:
        try:
            if browser_fetcher is not None:
                html = browser_fetcher.get_html(url)
                status = "ok" if html else "empty"
                log.info("warm-up (browser) %s -> %s", url, status)
            else:
                r = session.get(url, timeout=cfg.request_timeout)
                log.info("warm-up %s -> %s", url, r.status_code)
            time.sleep(random.uniform(2.0, 4.5))
        except Exception as e:
            log.warning("warm-up failed %s -> %s", url, e)

def polite_sleep(base: float, cfg: Config) -> None:
    """Sleep with human-like variable timing (occasionally longer pauses)."""
    jitter = random.uniform(cfg.jitter_min, cfg.jitter_max)
    # ~10% chance of a longer "reading" pause to mimic human browsing
    if random.random() < 0.10:
        jitter += random.uniform(3.0, 8.0)
        log.debug("Extended pause (simulating reading): %.1fs", base + jitter)
    time.sleep(base + jitter)


class BrowserHTMLFetcher:
    """
    HTML fetcher using undetected-chromedriver (a patched real Chrome).

    Unlike Playwright, undetected-chromedriver patches the actual Chrome
    binary and chromedriver to remove all automation fingerprints that
    Cloudflare detects.  This means Cloudflare challenges either don't
    appear at all, or — if they do — the user can solve them once and the
    cf_clearance cookie persists for subsequent requests.

    The browser window is visible by default so the user can interact
    with any challenge that does appear.
    """

    CHALLENGE_WAIT_TIMEOUT = 60  # 1 minute

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.driver = None

    def __enter__(self) -> "BrowserHTMLFetcher":
        try:
            import undetected_chromedriver as uc  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "undetected-chromedriver is not installed. "
                "Install it with: pip install undetected-chromedriver"
            ) from e

        options = uc.ChromeOptions()
        options.add_argument("--window-size=1280,900")
        options.add_argument("--lang=en-US")

        if self.cfg.browser_headless:
            options.add_argument("--headless=new")

        # Use a persistent Chrome profile so cf_clearance cookies survive restarts.
        if self.cfg.chrome_profile_dir:
            profile_dir = self.cfg.chrome_profile_dir
        else:
            profile_dir = str(Path.home() / ".hrs_scraper_chrome_profile")

        options.add_argument(f"--user-data-dir={profile_dir}")

        self.driver = uc.Chrome(options=options, version_main=None)
        self.driver.set_page_load_timeout(self.cfg.request_timeout)
        log.info("Chrome launched (profile: %s)", profile_dir)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass

    def _is_challenge_page(self) -> bool:
        """Check whether the current page is a Cloudflare challenge."""
        try:
            title = self.driver.title.lower()
            src = self.driver.page_source.lower()

            # Title-based checks (most reliable)
            if "just a moment" in title or "attention required" in title:
                return True

            # The challenge token is only present on actual challenge pages
            if "__cf_chl_tk" in src:
                return True

            # Turnstile iframe is the interactive challenge widget
            if "challenges.cloudflare.com" in src:
                return True

            return False
        except Exception:
            return False

    @staticmethod
    def _bring_chrome_to_front() -> None:
        """Bring Chrome to the foreground on macOS."""
        try:
            subprocess.run(
                ["osascript", "-e", 'tell application "Google Chrome" to activate'],
                timeout=5,
                capture_output=True,
            )
        except Exception:
            pass

    def _wait_for_challenge_solved(self, url: str) -> None:
        """
        Poll until the Cloudflare challenge page is gone.
        The browser is visible — the user clicks the challenge themselves.
        """
        self._bring_chrome_to_front()
        log.warning(
            "Cloudflare challenge on %s — "
            "please solve it in the Chrome window. Waiting up to %ds …",
            url,
            self.CHALLENGE_WAIT_TIMEOUT,
        )
        deadline = time.time() + self.CHALLENGE_WAIT_TIMEOUT
        while time.time() < deadline:
            time.sleep(2)
            if not self._is_challenge_page():
                log.info("Cloudflare challenge solved! Resuming scraping.")
                time.sleep(random.uniform(1.5, 3.0))
                return
        log.error(
            "Timed out waiting for Cloudflare challenge (%ds).",
            self.CHALLENGE_WAIT_TIMEOUT,
        )

    def get_html(self, url: str, params: Optional[dict] = None) -> Optional[str]:
        if not self.driver:
            raise RuntimeError("BrowserHTMLFetcher not initialized")

        full_url = requests.Request("GET", url, params=params).prepare().url
        try:
            self.driver.get(full_url)

            # Give the page time to render / JS to execute.
            time.sleep(random.uniform(1.0, 2.5))

            # If Cloudflare challenge appears, wait for user to solve it.
            if self._is_challenge_page():
                self._wait_for_challenge_solved(full_url)

            return self.driver.page_source
        except Exception as e:
            log.error("Chrome fetch failed: %s ; error=%s", full_url, e)
            try:
                if self._is_challenge_page():
                    self._wait_for_challenge_solved(full_url)
                    return self.driver.page_source
            except Exception:
                pass
            return None


_request_counter = 0


def _maybe_rotate_ua(session: requests.Session) -> None:
    """Occasionally rotate User-Agent to mimic a real browser updating or different sessions."""
    global _request_counter
    _request_counter += 1
    # Rotate roughly every 40-80 requests
    if _request_counter % random.randint(40, 80) == 0:
        new_headers = _pick_ua_and_headers()
        session.headers.update(new_headers)
        log.debug("Rotated User-Agent: %s", new_headers["User-Agent"][:60])


def fetch_soup(
    session: requests.Session,
    cfg: Config,
    url: str,
    params: Optional[dict] = None,
    browser_fetcher: Optional[BrowserHTMLFetcher] = None,
    referer: Optional[str] = None,
) -> Optional[BeautifulSoup]:
    if cfg.use_browser:
        if browser_fetcher is None:
            with BrowserHTMLFetcher(cfg) as bf:
                html = bf.get_html(url, params=params)
        else:
            html = browser_fetcher.get_html(url, params=params)

        if not html:
            return None
        return BeautifulSoup(html, "lxml")

    _maybe_rotate_ua(session)

    req_headers = {}
    if referer:
        req_headers["Referer"] = referer
    else:
        # Default: pretend we came from the publications listing
        req_headers["Referer"] = LIST_URL

    try:
        resp = session.get(url, params=params, timeout=cfg.request_timeout, headers=req_headers)

        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after and retry_after.isdigit() else 30.0
            log.warning("429 Too Many Requests: %s ; wait %.1fs", resp.url, wait_s)
            time.sleep(wait_s + random.uniform(5.0, 15.0))
            resp = session.get(url, params=params, timeout=cfg.request_timeout, headers=req_headers)

        if resp.status_code == 403:
            log.warning("403 Forbidden (possible Cloudflare block): %s ; backing off", resp.url)
            time.sleep(random.uniform(30.0, 60.0))
            # Rotate UA and retry once
            session.headers.update(_pick_ua_and_headers())
            resp = session.get(url, params=params, timeout=cfg.request_timeout, headers=req_headers)

        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")

    except requests.RequestException as e:
        full_url = requests.Request("GET", url, params=params).prepare().url
        log.error("请求失败: %s ; error=%s", full_url, e)
        return None


def init_db(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def record_failed_page(conn: sqlite3.Connection, page_type: str, url: str, reason: str) -> None:
    conn.execute(
        "INSERT INTO failed_pages (page_type, url, reason) VALUES (?, ?, ?)",
        (page_type, url, reason[:1000]),
    )
    conn.commit()


def clear_failed_page(conn: sqlite3.Connection, page_type: str, url: str) -> None:
    conn.execute(
        "DELETE FROM failed_pages WHERE page_type = ? AND url = ?",
        (page_type, url),
    )
    conn.commit()


def save_checkpoint(conn: sqlite3.Connection, page_num: int) -> None:
    conn.execute(
        """INSERT INTO crawl_checkpoint (id, last_list_page, updated_at)
           VALUES (1, ?, CURRENT_TIMESTAMP)
           ON CONFLICT(id) DO UPDATE SET
               last_list_page = excluded.last_list_page,
               updated_at = CURRENT_TIMESTAMP""",
        (page_num,),
    )
    conn.commit()


def load_checkpoint(conn: sqlite3.Connection) -> Optional[int]:
    row = conn.execute(
        "SELECT last_list_page FROM crawl_checkpoint WHERE id = 1"
    ).fetchone()
    return row[0] if row else None


def article_exists(conn: sqlite3.Connection, article_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM articles WHERE article_id = ? LIMIT 1",
        (article_id,),
    ).fetchone()
    return row is not None


def upsert_article(conn: sqlite3.Connection, record: dict) -> bool:
    existed_before = article_exists(conn, record["article_id"])
    conn.execute(UPSERT_SQL, record)
    conn.commit()
    return existed_before


def normalize_ws(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_label(text: str) -> str:
    return normalize_ws(text).replace(":", "").lower()


def normalize_doi(raw: str) -> str:
    s = normalize_ws(raw)
    s = re.sub(r"^https?://(dx\.)?doi\.org/", "", s, flags=re.I)
    return s.strip()


def extract_title_h1(soup: BeautifulSoup) -> str:
    h1 = soup.find("h1")
    return normalize_ws(h1.get_text(" ", strip=True)) if h1 else ""


def extract_detail_fields_from_table(soup: BeautifulSoup) -> dict[str, str]:
    fields = {key: "" for key in LABEL_MAP.keys()}

    for row in soup.find_all("tr"):
        label_td = row.find("td", class_="biblio-row-title")
        if not label_td:
            continue

        label_norm = normalize_label(label_td.get_text(" ", strip=True))
        field_key = NORMALIZED_FIELD_LOOKUP.get(label_norm)
        if not field_key:
            continue

        value_td = label_td.find_next_sibling("td")
        if not value_td:
            continue

        fields[field_key] = normalize_ws(value_td.get_text(" ", strip=True))

    return fields


def build_record(url: str, soup: BeautifulSoup) -> dict:
    article_id = url.rstrip("/").split("/")[-1]
    title_h1 = extract_title_h1(soup)
    fields = extract_detail_fields_from_table(soup)

    return {
        "article_id": article_id,
        "url": url,
        "title": fields["title"] or title_h1,
        "publication_type": fields["publication_type"],
        "year": fields["year"],
        "authors": fields["authors"],
        "journal": fields["journal"],
        "volume": fields["volume"],
        "pagination": fields["pagination"],
        "date_published": fields["date_published"],
        "issn": fields["issn"],
        "keywords": fields["keywords"],
        "abstract": fields["abstract"],
        "doi": normalize_doi(fields["doi"]),
        "citation_key": fields["citation_key"],
        "pubmed_id": fields["pubmed_id"],
        "title_h1": title_h1,
    }


def is_suspicious_record(record: dict) -> bool:
    signals = [
        bool(record.get("title")),
        bool(record.get("authors")),
        bool(record.get("abstract")),
        bool(record.get("journal")),
    ]
    return sum(signals) < 2


def looks_like_cloudflare_challenge(soup: BeautifulSoup) -> bool:
    """
    Heuristic detection for Cloudflare managed challenges.

    When this triggers, the page content usually doesn't include the expected
    bibliography item links, so we should not treat it as a normal "empty page".
    """
    # soup.decode() returns the full HTML; list pages are small so this is acceptable.
    s = soup.decode().lower()
    title_tag = soup.find("title")
    title = title_tag.get_text().lower() if title_tag else ""
    return (
        "__cf_chl_tk" in s
        or "just a moment" in title
        or "attention required" in title
        or "challenges.cloudflare.com" in s
    )


def extract_article_links(soup: BeautifulSoup) -> list[str]:
    found = []
    seen = set()

    candidate_containers = []
    for selector in ["div.view-content", "div.item-list", "main", "section"]:
        candidate_containers.extend(soup.select(selector))

    containers = candidate_containers if candidate_containers else [soup]

    for container in containers:
        for a in container.find_all("a", href=True):
            canonical = canonicalize_article_url(a["href"])
            if canonical and canonical not in seen:
                seen.add(canonical)
                found.append(canonical)

    if not found:
        # Diagnostic: help distinguish selector/content changes vs URL-format mismatch.
        biblio_hrefs = []
        for a in soup.find_all("a", href=True):
            h = (a.get("href") or "").strip()
            if "/publications/biblio/" in h:
                biblio_hrefs.append(h)
                if len(biblio_hrefs) >= 10:
                    break
        if biblio_hrefs:
            log.debug(
                "列表页未提取到文章链接；biblio href样本(可能是之前正则过于严格/URL变体): %s",
                biblio_hrefs,
            )
        else:
            any_hrefs = []
            for a in soup.find_all("a", href=True):
                h = (a.get("href") or "").strip()
                if not h:
                    continue
                any_hrefs.append(h)
                if len(any_hrefs) >= 10:
                    break
            log.debug(
                "列表页未提取到文章链接；未发现包含/publications/biblio/的href；href样本: %s",
                any_hrefs,
            )
    return found


def find_last_page_number(soup: BeautifulSoup) -> Optional[int]:
    max_page = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            n = int(m.group(1))
            if max_page is None or n > max_page:
                max_page = n
    return max_page


def export_jsonl(conn: sqlite3.Connection, jsonl_path: str) -> None:
    rows = conn.execute("""
        SELECT
            url, article_id, title, publication_type, year, authors, journal,
            volume, pagination, date_published, issn, keywords, abstract, doi,
            citation_key, pubmed_id, title_h1, scraped_at, updated_at
        FROM articles
        ORDER BY CAST(article_id AS INTEGER)
    """).fetchall()

    with open(jsonl_path, "w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(dict(row), ensure_ascii=False) + "\n")


def export_csv(conn: sqlite3.Connection, csv_path: str) -> None:
    rows = conn.execute("""
        SELECT
            url, article_id, title, publication_type, year, authors, journal,
            volume, pagination, date_published, issn, keywords, abstract, doi,
            citation_key, pubmed_id, title_h1, scraped_at, updated_at
        FROM articles
        ORDER BY CAST(article_id AS INTEGER)
    """).fetchall()

    if not rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            pass
        return

    fieldnames = list(rows[0].keys())
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def get_total_articles(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]


def print_report(conn: sqlite3.Connection) -> None:
    total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    abstract_empty = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE COALESCE(TRIM(abstract), '') = ''"
    ).fetchone()[0]
    doi_empty = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE COALESCE(TRIM(doi), '') = ''"
    ).fetchone()[0]
    title_empty = conn.execute(
        "SELECT COUNT(*) FROM articles WHERE COALESCE(TRIM(title), '') = ''"
    ).fetchone()[0]
    failed_list = conn.execute(
        "SELECT COUNT(*) FROM failed_pages WHERE page_type='list'"
    ).fetchone()[0]
    failed_detail = conn.execute(
        "SELECT COUNT(*) FROM failed_pages WHERE page_type='detail'"
    ).fetchone()[0]

    log.info("========== 数据报告 ==========")
    log.info("articles 总数: %d", total)
    log.info("title 为空: %d", title_empty)
    log.info("abstract 为空: %d", abstract_empty)
    log.info("doi 为空: %d", doi_empty)
    log.info("failed list pages: %d", failed_list)
    log.info("failed detail pages: %d", failed_detail)

    top_years = conn.execute("""
        SELECT year, COUNT(*) AS n
        FROM articles
        GROUP BY year
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()

    if top_years:
        log.info("Top years:")
        for row in top_years:
            log.info("  %s -> %s", row["year"], row["n"])


def scrape_detail_page(
    session: requests.Session,
    cfg: Config,
    conn: sqlite3.Connection,
    url: str,
    stats: ScrapeStats,
    force_refresh: bool = False,
    browser_fetcher: Optional[BrowserHTMLFetcher] = None,
    referer: Optional[str] = None,
) -> None:
    article_id = url.rstrip("/").split("/")[-1]

    if (not force_refresh) and article_exists(conn, article_id):
        return

    stats.article_pages_attempted += 1
    log.info("详情页: %s", url)

    soup = fetch_soup(session, cfg, url, browser_fetcher=browser_fetcher, referer=referer)
    if not soup:
        stats.article_pages_failed += 1
        record_failed_page(conn, "detail", url, "fetch_failed")
        return

    record = build_record(url, soup)
    if is_suspicious_record(record):
        stats.suspicious_records += 1
        log.warning("疑似结构变化 / 空值过多: %s", url)

    existed_before = upsert_article(conn, record)
    clear_failed_page(conn, "detail", url)

    if existed_before:
        stats.article_pages_updated += 1
    else:
        stats.article_pages_inserted += 1

    polite_sleep(cfg.detail_delay, cfg)


def run_crawl(
    cfg: Config,
    start_page: int = 0,
    max_pages: Optional[int] = None,
    force_refresh: bool = False,
    resume: bool = True,
) -> None:
    session = build_session(cfg)
    stats = ScrapeStats()

    browser_cm = BrowserHTMLFetcher(cfg) if cfg.use_browser else nullcontext(None)
    with browser_cm as browser_fetcher:
        warm_up_session(session, cfg, browser_fetcher=browser_fetcher)
        with closing(init_db(cfg.db_path)) as conn:
            # Auto-resume: if --start-page was not explicitly set, pick up
            # from the last checkpoint stored in the database.
            if resume and start_page == 0:
                checkpoint = load_checkpoint(conn)
                if checkpoint is not None and checkpoint > 0:
                    start_page = checkpoint
                    log.info("断点续爬: 从 page=%d 恢复", start_page)

            first_url = build_list_page_url(start_page)
            first_soup = fetch_soup(session, cfg, first_url, browser_fetcher=browser_fetcher)
            if not first_soup:
                record_failed_page(conn, "list", first_url, "fetch_failed_page")
                log.error("无法抓取起始列表页，终止")
                return

            detected_last_page = find_last_page_number(first_soup)
            if detected_last_page is None:
                log.warning("未探测到分页上限，将采用抓到连续空页即停模式")
            else:
                log.info("自动探测到最后一页: page=%d", detected_last_page)

            page_num = start_page
            empty_streak = 0
            challenge_streak = 0
            processed_pages = 0

            while True:
                if max_pages is not None and processed_pages >= max_pages:
                    log.info("达到 --max-pages=%d，停止", max_pages)
                    break

                prepared_url = build_list_page_url(page_num)

                if page_num == start_page:
                    soup = first_soup
                else:
                    # Set referer to previous page to simulate clicking "next"
                    prev_url = build_list_page_url(page_num - 1) if page_num > 0 else LIST_URL
                    soup = fetch_soup(
                        session,
                        cfg,
                        prepared_url,
                        browser_fetcher=browser_fetcher,
                        referer=prev_url,
                    )

                stats.list_pages_seen += 1
                processed_pages += 1
                log.info("列表页: %s", prepared_url)

                if not soup:
                    stats.list_pages_failed += 1
                    record_failed_page(conn, "list", prepared_url, "fetch_failed")
                    page_num += 1
                    polite_sleep(cfg.list_delay, cfg)
                    if detected_last_page is not None and page_num > detected_last_page:
                        break
                    continue

                links = extract_article_links(soup)
                stats.article_links_seen += len(links)

                if not links:
                    if looks_like_cloudflare_challenge(soup):
                        challenge_streak += 1
                        stats.list_pages_failed += 1
                        record_failed_page(conn, "list", prepared_url, "cloudflare_challenge")
                        empty_streak = 0
                        log.warning(
                            "检测到 Cloudflare challenge（跳过空页停止逻辑）：%s",
                            prepared_url,
                        )
                        # Back off aggressively and rotate identity
                        backoff = min(30.0 * challenge_streak, 120.0) + random.uniform(5.0, 15.0)
                        log.info("Cloudflare backoff: %.1fs, rotating UA", backoff)
                        session.headers.update(_pick_ua_and_headers())
                        time.sleep(backoff)
                        # Re-warm the session after a challenge
                        if challenge_streak >= 2:
                            log.info("Re-warming session after repeated challenges")
                            warm_up_session(session, cfg, browser_fetcher=browser_fetcher)
                        page_num += 1
                        if detected_last_page is not None and page_num > detected_last_page:
                            break
                        if max_pages is None and challenge_streak >= 4:
                            log.info("Cloudflare challenge 连续 >= 4，停止")
                            break
                        continue

                    empty_streak += 1
                    log.warning("列表页无文章链接: %s", prepared_url)
                    if detected_last_page is None and empty_streak >= 2:
                        log.info("连续空页 >= 2，停止")
                        break
                else:
                    empty_streak = 0
                    challenge_streak = 0
                    clear_failed_page(conn, "list", prepared_url)
                    log.info("  -> 本页找到 %d 篇", len(links))
                    for link in links:
                        scrape_detail_page(
                            session=session,
                            cfg=cfg,
                            conn=conn,
                            url=link,
                            stats=stats,
                            force_refresh=force_refresh,
                            browser_fetcher=browser_fetcher,
                            referer=prepared_url,
                        )

                # Save checkpoint after each list page so we can resume here.
                save_checkpoint(conn, page_num)

                polite_sleep(cfg.list_delay, cfg)

                if detected_last_page is not None and page_num >= detected_last_page:
                    break

                page_num += 1

            log.info("========== crawl 完成 ==========")
            log.info("列表页访问数: %d", stats.list_pages_seen)
            log.info("列表页失败数: %d", stats.list_pages_failed)
            log.info("发现文章链接数: %d", stats.article_links_seen)
            log.info("详情页尝试抓取: %d", stats.article_pages_attempted)
            log.info("新增文章数: %d", stats.article_pages_inserted)
            log.info("更新文章数: %d", stats.article_pages_updated)
            log.info("详情页失败数: %d", stats.article_pages_failed)
            log.info("疑似异常记录数: %d", stats.suspicious_records)
            log.info("数据库总文章数: %d", get_total_articles(conn))


def run_export(cfg: Config) -> None:
    with closing(init_db(cfg.db_path)) as conn:
        export_jsonl(conn, cfg.jsonl_path)
        export_csv(conn, cfg.csv_path)
        log.info("导出完成")
        log.info("JSONL -> %s", cfg.jsonl_path)
        log.info("CSV   -> %s", cfg.csv_path)


def run_report(cfg: Config) -> None:
    with closing(init_db(cfg.db_path)) as conn:
        print_report(conn)


def retry_failed_detail_pages(
    session: requests.Session,
    cfg: Config,
    conn: sqlite3.Connection,
    force_refresh: bool = True,
    browser_fetcher: Optional[BrowserHTMLFetcher] = None,
) -> None:
    stats = ScrapeStats()
    rows = conn.execute("""
        SELECT DISTINCT url
        FROM failed_pages
        WHERE page_type = 'detail'
        ORDER BY id
    """).fetchall()

    if not rows:
        log.info("没有 failed detail pages")
        return

    log.info("准备重试 failed detail pages: %d 条", len(rows))
    for row in rows:
        scrape_detail_page(
            session=session,
            cfg=cfg,
            conn=conn,
            url=row["url"],
            stats=stats,
            force_refresh=force_refresh,
            browser_fetcher=browser_fetcher,
        )

    log.info("retry-failed detail 完成，新增=%d 更新=%d 失败=%d",
             stats.article_pages_inserted,
             stats.article_pages_updated,
             stats.article_pages_failed)


def retry_failed_list_pages(
    session: requests.Session,
    cfg: Config,
    conn: sqlite3.Connection,
    browser_fetcher: Optional[BrowserHTMLFetcher] = None,
) -> None:
    rows = conn.execute("""
        SELECT DISTINCT url
        FROM failed_pages
        WHERE page_type = 'list'
        ORDER BY id
    """).fetchall()

    if not rows:
        log.info("没有 failed list pages")
        return

    log.info("准备重试 failed list pages: %d 条", len(rows))
    stats = ScrapeStats()

    for row in rows:
        url = row["url"]
        log.info("重试列表页: %s", url)

        soup = fetch_soup(session, cfg, url, browser_fetcher=browser_fetcher)
        if not soup:
            log.error("重试列表页失败: %s ; fetch_failed", url)
            continue

        links = extract_article_links(soup)
        if not links:
            log.warning("重试列表页成功但无文章链接: %s", url)
            continue

        clear_failed_page(conn, "list", url)
        log.info("  -> 发现 %d 篇", len(links))

        for link in links:
            scrape_detail_page(
                session=session,
                cfg=cfg,
                conn=conn,
                url=link,
                stats=stats,
                force_refresh=False,
                browser_fetcher=browser_fetcher,
            )

        polite_sleep(cfg.list_delay, cfg)

    log.info("retry-failed list 完成，新增=%d 更新=%d 失败=%d",
             stats.article_pages_inserted,
             stats.article_pages_updated,
             stats.article_pages_failed)


def run_retry_failed(cfg: Config, detail_only: bool = False, list_only: bool = False) -> None:
    with closing(init_db(cfg.db_path)) as conn:
        session = build_session(cfg)

        browser_cm = BrowserHTMLFetcher(cfg) if cfg.use_browser else nullcontext(None)
        with browser_cm as browser_fetcher:
            if not detail_only:
                retry_failed_list_pages(session, cfg, conn, browser_fetcher=browser_fetcher)

            if not list_only:
                retry_failed_detail_pages(
                    session,
                    cfg,
                    conn,
                    force_refresh=True,
                    browser_fetcher=browser_fetcher,
                )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hrs_scraper",
        description="CLI scraper for HRS journal article bibliography pages.",
    )

    parser.add_argument("--db", default=DEFAULT_DB_PATH, help="SQLite database path")
    parser.add_argument("--jsonl", default=DEFAULT_JSONL, help="JSONL export path")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="CSV export path")
    parser.add_argument("--log-file", default=DEFAULT_LOG_FILE, help="Log file path")
    parser.add_argument("--request-timeout", type=int, default=DEFAULT_REQUEST_TIMEOUT)
    parser.add_argument("--detail-delay", type=float, default=DEFAULT_DETAIL_DELAY)
    parser.add_argument("--list-delay", type=float, default=DEFAULT_LIST_DELAY)
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES)
    parser.add_argument("--jitter-min", type=float, default=DEFAULT_JITTER_MIN)
    parser.add_argument("--jitter-max", type=float, default=DEFAULT_JITTER_MAX)
    parser.add_argument(
        "--no-browser",
        action="store_true",
        help="Disable Chrome browser and use plain requests (cannot solve Cloudflare challenges).",
    )
    parser.add_argument(
        "--headless",
        action="store_true",
        help="Run Chrome in headless mode (browser window not visible).",
    )
    parser.add_argument(
        "--chrome-profile",
        default=None,
        help="Path to Chrome user-data-dir (persists cookies/sessions across runs).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose logging")

    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl = subparsers.add_parser("crawl", help="Crawl list pages and detail pages")
    crawl.add_argument("--start-page", type=int, default=0, help="Start page number")
    crawl.add_argument("--max-pages", type=int, default=None, help="Maximum number of list pages to process")
    crawl.add_argument("--force-refresh", action="store_true", help="Re-fetch existing article detail pages")
    crawl.add_argument("--no-resume", action="store_true", help="Ignore checkpoint, start from --start-page")

    export = subparsers.add_parser("export", help="Export DB to JSONL and CSV")

    report = subparsers.add_parser("report", help="Show DB quality report")

    retry_failed = subparsers.add_parser("retry-failed", help="Retry failed pages")
    retry_failed.add_argument("--detail-only", action="store_true", help="Retry only failed detail pages")
    retry_failed.add_argument("--list-only", action="store_true", help="Retry only failed list pages")

    return parser


def parse_args() -> tuple[argparse.Namespace, Config]:
    parser = build_parser()
    args = parser.parse_args()

    cfg = Config(
        db_path=args.db,
        jsonl_path=args.jsonl,
        csv_path=args.csv,
        log_file=args.log_file,
        request_timeout=args.request_timeout,
        detail_delay=args.detail_delay,
        list_delay=args.list_delay,
        max_retries=args.max_retries,
        jitter_min=args.jitter_min,
        jitter_max=args.jitter_max,
        use_browser=not args.no_browser,
        browser_headless=args.headless,
        chrome_profile_dir=args.chrome_profile,
    )
    return args, cfg


def main() -> None:
    args, cfg = parse_args()
    setup_logging(cfg.log_file, verbose=args.verbose)

    log.info("启动命令: %s", args.command)
    log.info("数据库: %s", cfg.db_path)

    if args.command == "crawl":
        run_crawl(
            cfg=cfg,
            start_page=args.start_page,
            max_pages=args.max_pages,
            force_refresh=args.force_refresh,
            resume=not args.no_resume,
        )
    elif args.command == "export":
        run_export(cfg)
    elif args.command == "report":
        run_report(cfg)
    elif args.command == "retry-failed":
        run_retry_failed(
            cfg=cfg,
            detail_only=args.detail_only,
            list_only=args.list_only,
        )
    else:
        raise SystemExit(f"未知命令: {args.command}")


if __name__ == "__main__":
    main()
