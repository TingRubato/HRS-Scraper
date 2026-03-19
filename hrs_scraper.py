#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
HRS Journal Article Bibliography Scraper
HRS 期刊文章文献目录爬虫

Scrapes journal article metadata (title, authors, abstract, DOI, etc.)
from the Health and Retirement Study (HRS) publication database at
https://hrs.isr.umich.edu/publications/biblio

从密歇根大学健康与退休研究 (HRS) 出版数据库抓取期刊文章元数据
（标题、作者、摘要、DOI 等）。

Features / 功能特性:
  - Cloudflare bypass via undetected-chromedriver (visible browser)
    使用 undetected-chromedriver 绕过 Cloudflare（可见浏览器窗口）
  - Checkpoint-based resume crawling (断点续爬)
  - Human-like browsing simulation (模拟真人浏览行为)
  - SQLite storage with JSONL/CSV export (SQLite 存储，支持 JSONL/CSV 导出)
  - Failed page tracking and retry (失败页面记录与重试)
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import re
import sqlite3
import subprocess
import sys
import time
from contextlib import closing, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import quote, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ---------------------------------------------------------------------------
# User-Agent pool for rotation / UA 池，用于轮换
# ---------------------------------------------------------------------------
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:135.0) Gecko/20100101 Firefox/135.0",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/133.0.0.0 Safari/537.36 Edg/133.0.0.0",
]

# Sec-Ch-Ua header values matching the UA pool / 与 UA 池匹配的 Sec-Ch-Ua 头
SEC_CH_UA_MAP = {
    "Chrome/134": '"Chromium";v="134", "Google Chrome";v="134", "Not:A-Brand";v="99"',
    "Chrome/133": '"Chromium";v="133", "Google Chrome";v="133", "Not:A-Brand";v="99"',
    "Edg/133": '"Chromium";v="133", "Microsoft Edge";v="133", "Not:A-Brand";v="99"',
}

# ---------------------------------------------------------------------------
# Target site URLs / 目标站点 URL
# ---------------------------------------------------------------------------
BASE_URL = "https://hrs.isr.umich.edu"
LIST_URL = f"{BASE_URL}/publications/biblio"
FILTER_PARAMS = {"f[type]": "102"}  # Journal Article / 仅期刊文章


def build_list_page_url(page_num: int) -> str:
    """
    Build a list page URL with deterministic query ordering.
    构建列表页 URL，查询参数顺序固定。

    Output format / 输出格式:
      https://hrs.isr.umich.edu/publications/biblio?page=261&f%5Btype%5D=102
    """
    type_key, type_val = next(iter(FILTER_PARAMS.items()))
    type_key_encoded = quote(type_key, safe="")
    return f"{LIST_URL}?page={page_num}&{type_key_encoded}={type_val}"


# ---------------------------------------------------------------------------
# Default configuration / 默认配置
# ---------------------------------------------------------------------------
DEFAULT_DB_PATH = "hrs_scraper.db"
DEFAULT_JSONL = "hrs_abstracts.jsonl"
DEFAULT_CSV = "hrs_abstracts.csv"
DEFAULT_LOG_FILE = "scraper.log"

DEFAULT_REQUEST_TIMEOUT = 30   # seconds / 秒
DEFAULT_DETAIL_DELAY = 1.5     # base delay between detail pages / 详情页基础延迟
DEFAULT_LIST_DELAY = 2.5       # base delay between list pages / 列表页基础延迟
DEFAULT_MAX_RETRIES = 5        # HTTP retry count / HTTP 重试次数
DEFAULT_JITTER_MIN = 0.5       # min random jitter / 最小随机抖动
DEFAULT_JITTER_MAX = 2.0       # max random jitter / 最大随机抖动

# Legacy headers (used as fallback when browser mode is off)
# 备用请求头（浏览器模式关闭时使用）
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------------------------------------------------------
# Database schema / 数据库表结构
# ---------------------------------------------------------------------------
SCHEMA_SQL = """
-- Main table: one row per article / 主表：每篇文章一行
CREATE TABLE IF NOT EXISTS articles (
    article_id TEXT PRIMARY KEY,     -- HRS internal ID / HRS 内部 ID
    url TEXT NOT NULL,               -- canonical detail page URL / 详情页规范 URL
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
    title_h1 TEXT,                   -- <h1> text from detail page / 详情页 <h1> 文本
    scraped_at TEXT DEFAULT CURRENT_TIMESTAMP,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Track pages that failed to fetch / 记录抓取失败的页面
CREATE TABLE IF NOT EXISTS failed_pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    page_type TEXT NOT NULL,         -- 'list' or 'detail' / 列表页或详情页
    url TEXT NOT NULL,
    reason TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);

-- Checkpoint for resume crawling (singleton row)
-- 断点续爬记录（单行表）
CREATE TABLE IF NOT EXISTS crawl_checkpoint (
    id INTEGER PRIMARY KEY CHECK (id = 1),
    last_list_page INTEGER NOT NULL DEFAULT 0,
    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_articles_year ON articles(year);
CREATE INDEX IF NOT EXISTS idx_articles_doi ON articles(doi);
CREATE INDEX IF NOT EXISTS idx_failed_pages_type ON failed_pages(page_type);
"""

# Upsert: insert or update on conflict / 插入或冲突时更新
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

# Regex to match article detail paths / 匹配文章详情页路径的正则
ARTICLE_HREF_RE = re.compile(r"^/publications/biblio/(\d+)/?$")


def canonicalize_article_url(href_or_url: str) -> Optional[str]:
    """
    Normalize a bibliography detail URL to canonical form.
    将文献详情 URL 规范化为统一格式。

    Handles relative links, trailing slashes, query strings, fragments.
    处理相对链接、尾部斜杠、查询字符串和锚点。
    """
    full_url = urljoin(BASE_URL, href_or_url.strip())
    parsed = urlparse(full_url)
    m = ARTICLE_HREF_RE.match(parsed.path)
    if not m:
        return None
    article_id = m.group(1)
    return f"{BASE_URL}/publications/biblio/{article_id}"


# ---------------------------------------------------------------------------
# Field label mapping for detail page parsing
# 详情页字段标签映射（HTML 表格中的标签 -> 数据库字段名）
# ---------------------------------------------------------------------------
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
# Reverse lookup: label text -> field key / 反向查找：标签文本 -> 字段名
NORMALIZED_FIELD_LOOKUP = {
    alias: key
    for key, aliases in LABEL_MAP.items()
    for alias in aliases
}


# ---------------------------------------------------------------------------
# Configuration and statistics dataclasses / 配置与统计数据类
# ---------------------------------------------------------------------------
@dataclass
class Config:
    """Runtime configuration / 运行时配置"""
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
    use_browser: bool = True             # use Chrome browser / 使用 Chrome 浏览器
    browser_headless: bool = False       # show browser window / 显示浏览器窗口
    chrome_profile_dir: Optional[str] = None  # persistent profile / 持久化浏览器配置目录


@dataclass
class ScrapeStats:
    """Counters for a single crawl run / 单次爬取的计数器"""
    list_pages_seen: int = 0             # 已访问列表页数
    list_pages_failed: int = 0           # 列表页失败数
    article_links_seen: int = 0          # 发现的文章链接数
    article_pages_attempted: int = 0     # 尝试抓取的详情页数
    article_pages_inserted: int = 0      # 新增文章数
    article_pages_updated: int = 0       # 更新文章数
    article_pages_failed: int = 0        # 详情页失败数
    suspicious_records: int = 0          # 疑似异常记录数


# ---------------------------------------------------------------------------
# Logging / 日志配置
# ---------------------------------------------------------------------------
log = logging.getLogger("hrs_scraper_cli")


def setup_logging(log_file: str, verbose: bool = False) -> None:
    """
    Configure file + console logging.
    配置文件日志和控制台日志。
    """
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


# ---------------------------------------------------------------------------
# HTTP session helpers (fallback when browser is off)
# HTTP 会话工具（浏览器模式关闭时的备用方案）
# ---------------------------------------------------------------------------
def _pick_ua_and_headers() -> dict[str, str]:
    """
    Pick a random User-Agent and build matching browser-like headers.
    随机选择 UA 并构建匹配的浏览器请求头。
    """
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
    # Chromium-based UAs need sec-ch-ua headers / Chromium 系 UA 需要 sec-ch-ua 头
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
    """
    Build a requests.Session with retry and browser-like headers.
    构建带重试和浏览器请求头的 requests.Session。
    """
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
    """
    Simulate a real user navigating: homepage -> publications -> biblio.
    模拟真人浏览路径：首页 -> 出版物 -> 文献目录。

    This establishes cookies (including cf_clearance) before scraping starts.
    在正式抓取前建立 cookies（包括 cf_clearance）。
    """
    warm_urls = [
        "https://hrs.isr.umich.edu/",
        "https://hrs.isr.umich.edu/publications",
        "https://hrs.isr.umich.edu/publications/biblio",
    ]
    for url in warm_urls:
        try:
            if browser_fetcher is not None:
                # Use the visible browser for warm-up / 用可见浏览器预热
                html = browser_fetcher.get_html(url)
                status = "ok" if html else "empty"
                log.info("warm-up (browser) %s -> %s", url, status)
            else:
                r = session.get(url, timeout=cfg.request_timeout)
                log.info("warm-up %s -> %s", url, r.status_code)
            # Human-like pause between navigations / 模拟真人浏览间隔
            time.sleep(random.uniform(2.0, 4.5))
        except Exception as e:
            log.warning("warm-up failed %s -> %s", url, e)


def polite_sleep(base: float, cfg: Config) -> None:
    """
    Sleep with human-like variable timing.
    模拟真人节奏的随机等待。

    ~10% chance of a longer "reading" pause.
    约 10% 概率出现较长的"阅读"停顿。
    """
    jitter = random.uniform(cfg.jitter_min, cfg.jitter_max)
    if random.random() < 0.10:
        jitter += random.uniform(3.0, 8.0)
        log.debug("Extended pause (simulating reading) / 延长停顿（模拟阅读）: %.1fs", base + jitter)
    time.sleep(base + jitter)


# ---------------------------------------------------------------------------
# Browser-based HTML fetcher (Cloudflare bypass)
# 基于浏览器的 HTML 获取器（绕过 Cloudflare）
# ---------------------------------------------------------------------------
class BrowserHTMLFetcher:
    """
    HTML fetcher using undetected-chromedriver (a patched real Chrome).
    使用 undetected-chromedriver（修补过的真实 Chrome）获取 HTML。

    Unlike Playwright, undetected-chromedriver patches the actual Chrome
    binary to remove all automation fingerprints that Cloudflare detects.
    与 Playwright 不同，undetected-chromedriver 修补真实 Chrome 二进制文件，
    移除所有 Cloudflare 可检测的自动化指纹。

    The browser window is visible by default. When a Cloudflare challenge
    appears, the scraper pauses and waits for the user to solve it manually.
    默认显示浏览器窗口。当 Cloudflare 验证出现时，爬虫暂停等待用户手动解决。
    """

    # Max seconds to wait for user to solve a challenge / 等待用户解决验证的最大秒数
    CHALLENGE_WAIT_TIMEOUT = 60

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.driver = None

    def __enter__(self) -> "BrowserHTMLFetcher":
        try:
            import undetected_chromedriver as uc  # type: ignore
        except ImportError as e:
            raise RuntimeError(
                "undetected-chromedriver is not installed. "
                "Install it with: pip install undetected-chromedriver\n"
                "未安装 undetected-chromedriver，请运行: pip install undetected-chromedriver"
            ) from e

        options = uc.ChromeOptions()
        options.add_argument("--window-size=1280,900")
        options.add_argument("--lang=en-US")

        if self.cfg.browser_headless:
            options.add_argument("--headless=new")

        # Persistent Chrome profile: cf_clearance cookies survive restarts
        # 持久化 Chrome 配置目录：cf_clearance cookie 在重启后保留
        if self.cfg.chrome_profile_dir:
            profile_dir = self.cfg.chrome_profile_dir
        else:
            profile_dir = str(Path.home() / ".hrs_scraper_chrome_profile")

        options.add_argument(f"--user-data-dir={profile_dir}")

        self.driver = uc.Chrome(options=options, version_main=None)
        self.driver.set_page_load_timeout(self.cfg.request_timeout)
        log.info("Chrome launched (profile: %s) / Chrome 已启动（配置目录: %s）",
                 profile_dir, profile_dir)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self.driver:
            try:
                self.driver.quit()
            except Exception:
                pass

    def _is_challenge_page(self) -> bool:
        """
        Check if current page is a Cloudflare challenge.
        检测当前页面是否为 Cloudflare 验证页。
        """
        try:
            title = self.driver.title.lower()
            src = self.driver.page_source.lower()

            # Title-based (most reliable) / 基于标题检测（最可靠）
            if "just a moment" in title or "attention required" in title:
                return True

            # Challenge token present / 存在验证令牌
            if "__cf_chl_tk" in src:
                return True

            # Turnstile challenge iframe / Turnstile 验证 iframe
            if "challenges.cloudflare.com" in src:
                return True

            return False
        except Exception:
            return False

    @staticmethod
    def _bring_chrome_to_front() -> None:
        """
        Bring Chrome to foreground on macOS.
        在 macOS 上将 Chrome 窗口置于最前。
        """
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
        Pause and wait for user to solve the Cloudflare challenge in the browser.
        暂停并等待用户在浏览器中解决 Cloudflare 验证。
        """
        self._bring_chrome_to_front()
        log.warning(
            "Cloudflare challenge on %s — please solve it in Chrome. Waiting %ds …\n"
            "Cloudflare 验证出现于 %s — 请在 Chrome 中完成验证。等待 %d 秒 …",
            url, self.CHALLENGE_WAIT_TIMEOUT,
            url, self.CHALLENGE_WAIT_TIMEOUT,
        )
        deadline = time.time() + self.CHALLENGE_WAIT_TIMEOUT
        while time.time() < deadline:
            time.sleep(2)
            if not self._is_challenge_page():
                log.info("Cloudflare challenge solved! Resuming. / Cloudflare 验证已通过！继续抓取。")
                time.sleep(random.uniform(1.5, 3.0))
                return
        log.error(
            "Timed out waiting for challenge (%ds). / 等待验证超时 (%d秒)。",
            self.CHALLENGE_WAIT_TIMEOUT, self.CHALLENGE_WAIT_TIMEOUT,
        )

    def get_html(self, url: str, params: Optional[dict] = None) -> Optional[str]:
        """
        Navigate to URL and return page HTML. Handles Cloudflare challenges.
        导航到 URL 并返回页面 HTML。自动处理 Cloudflare 验证。
        """
        if not self.driver:
            raise RuntimeError("BrowserHTMLFetcher not initialized / 浏览器未初始化")

        full_url = requests.Request("GET", url, params=params).prepare().url
        try:
            self.driver.get(full_url)

            # Human-like wait for JS rendering / 模拟真人等待 JS 渲染
            time.sleep(random.uniform(1.0, 2.5))

            # If challenge appears, wait for user / 如果出现验证，等待用户解决
            if self._is_challenge_page():
                self._wait_for_challenge_solved(full_url)

            return self.driver.page_source
        except Exception as e:
            log.error("Chrome fetch failed / Chrome 获取失败: %s ; error=%s", full_url, e)
            try:
                if self._is_challenge_page():
                    self._wait_for_challenge_solved(full_url)
                    return self.driver.page_source
            except Exception:
                pass
            return None


# ---------------------------------------------------------------------------
# UA rotation counter (for requests fallback mode)
# UA 轮换计数器（用于 requests 备用模式）
# ---------------------------------------------------------------------------
_request_counter = 0


def _maybe_rotate_ua(session: requests.Session) -> None:
    """
    Occasionally rotate User-Agent to look like different sessions.
    偶尔轮换 UA，模拟不同的浏览会话。
    """
    global _request_counter
    _request_counter += 1
    # Rotate roughly every 40-80 requests / 大约每 40-80 个请求轮换一次
    if _request_counter % random.randint(40, 80) == 0:
        new_headers = _pick_ua_and_headers()
        session.headers.update(new_headers)
        log.debug("Rotated User-Agent / 已轮换 UA: %s", new_headers["User-Agent"][:60])


def fetch_soup(
    session: requests.Session,
    cfg: Config,
    url: str,
    params: Optional[dict] = None,
    browser_fetcher: Optional[BrowserHTMLFetcher] = None,
    referer: Optional[str] = None,
) -> Optional[BeautifulSoup]:
    """
    Fetch a page and return parsed BeautifulSoup.
    获取页面并返回解析后的 BeautifulSoup 对象。

    Uses browser_fetcher if available, otherwise falls back to requests.
    优先使用浏览器获取，否则使用 requests 备用方案。
    """
    # --- Browser mode / 浏览器模式 ---
    if cfg.use_browser:
        if browser_fetcher is None:
            with BrowserHTMLFetcher(cfg) as bf:
                html = bf.get_html(url, params=params)
        else:
            html = browser_fetcher.get_html(url, params=params)

        if not html:
            return None
        return BeautifulSoup(html, "lxml")

    # --- Requests fallback mode / requests 备用模式 ---
    _maybe_rotate_ua(session)

    req_headers = {}
    if referer:
        req_headers["Referer"] = referer
    else:
        # Pretend we came from the listing / 伪装来源为列表页
        req_headers["Referer"] = LIST_URL

    try:
        resp = session.get(url, params=params, timeout=cfg.request_timeout, headers=req_headers)

        # Handle rate limiting / 处理速率限制
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            wait_s = float(retry_after) if retry_after and retry_after.isdigit() else 30.0
            log.warning("429 Too Many Requests / 请求过于频繁: %s ; wait %.1fs", resp.url, wait_s)
            time.sleep(wait_s + random.uniform(5.0, 15.0))
            resp = session.get(url, params=params, timeout=cfg.request_timeout, headers=req_headers)

        # Handle Cloudflare block / 处理 Cloudflare 拦截
        if resp.status_code == 403:
            log.warning("403 Forbidden (possible Cloudflare block) / 403 禁止访问（可能被 Cloudflare 拦截）: %s", resp.url)
            time.sleep(random.uniform(30.0, 60.0))
            session.headers.update(_pick_ua_and_headers())
            resp = session.get(url, params=params, timeout=cfg.request_timeout, headers=req_headers)

        resp.raise_for_status()
        return BeautifulSoup(resp.text, "lxml")

    except requests.RequestException as e:
        full_url = requests.Request("GET", url, params=params).prepare().url
        log.error("Request failed / 请求失败: %s ; error=%s", full_url, e)
        return None


# ---------------------------------------------------------------------------
# Database operations / 数据库操作
# ---------------------------------------------------------------------------
def init_db(db_path: str) -> sqlite3.Connection:
    """
    Initialize SQLite database and create tables if needed.
    初始化 SQLite 数据库，按需创建表。
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.executescript(SCHEMA_SQL)
    conn.commit()
    return conn


def record_failed_page(conn: sqlite3.Connection, page_type: str, url: str, reason: str) -> None:
    """Record a failed page for later retry. / 记录失败页面，供后续重试。"""
    conn.execute(
        "INSERT INTO failed_pages (page_type, url, reason) VALUES (?, ?, ?)",
        (page_type, url, reason[:1000]),
    )
    conn.commit()


def clear_failed_page(conn: sqlite3.Connection, page_type: str, url: str) -> None:
    """Remove a page from the failed list after success. / 成功后从失败列表中移除。"""
    conn.execute(
        "DELETE FROM failed_pages WHERE page_type = ? AND url = ?",
        (page_type, url),
    )
    conn.commit()


def save_checkpoint(conn: sqlite3.Connection, page_num: int) -> None:
    """
    Save the current list page number for resume crawling.
    保存当前列表页码，用于断点续爬。
    """
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
    """
    Load the last crawled page number from checkpoint.
    从断点记录中读取上次爬到的页码。
    """
    row = conn.execute(
        "SELECT last_list_page FROM crawl_checkpoint WHERE id = 1"
    ).fetchone()
    return row[0] if row else None


def article_exists(conn: sqlite3.Connection, article_id: str) -> bool:
    """Check if an article is already in the database. / 检查文章是否已存在于数据库中。"""
    row = conn.execute(
        "SELECT 1 FROM articles WHERE article_id = ? LIMIT 1",
        (article_id,),
    ).fetchone()
    return row is not None


def upsert_article(conn: sqlite3.Connection, record: dict) -> bool:
    """
    Insert or update an article. Returns True if it was an update.
    插入或更新文章。如果是更新则返回 True。
    """
    existed_before = article_exists(conn, record["article_id"])
    conn.execute(UPSERT_SQL, record)
    conn.commit()
    return existed_before


# ---------------------------------------------------------------------------
# Text normalization helpers / 文本规范化工具
# ---------------------------------------------------------------------------
def normalize_ws(text: str) -> str:
    """Collapse whitespace to single spaces. / 将连续空白合并为单个空格。"""
    return re.sub(r"\s+", " ", text or "").strip()


def normalize_label(text: str) -> str:
    """Normalize a field label for lookup. / 规范化字段标签用于查找。"""
    return normalize_ws(text).replace(":", "").lower()


def normalize_doi(raw: str) -> str:
    """Strip URL prefix from DOI. / 去除 DOI 的 URL 前缀。"""
    s = normalize_ws(raw)
    s = re.sub(r"^https?://(dx\.)?doi\.org/", "", s, flags=re.I)
    return s.strip()


# ---------------------------------------------------------------------------
# HTML parsing (detail page) / HTML 解析（详情页）
# ---------------------------------------------------------------------------
def extract_title_h1(soup: BeautifulSoup) -> str:
    """Extract text from the first <h1> tag. / 提取第一个 <h1> 标签的文本。"""
    h1 = soup.find("h1")
    return normalize_ws(h1.get_text(" ", strip=True)) if h1 else ""


def extract_detail_fields_from_table(soup: BeautifulSoup) -> dict[str, str]:
    """
    Parse the biblio detail table into a field dictionary.
    从文献详情表格解析出字段字典。

    Each row has a label cell (class="biblio-row-title") and a value cell.
    每行包含一个标签单元格（class="biblio-row-title"）和一个值单元格。
    """
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
    """
    Build a database record dict from a detail page.
    从详情页构建数据库记录字典。
    """
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
    """
    Heuristic: flag records missing most key fields.
    启发式检测：标记缺少大部分关键字段的记录。
    """
    signals = [
        bool(record.get("title")),
        bool(record.get("authors")),
        bool(record.get("abstract")),
        bool(record.get("journal")),
    ]
    return sum(signals) < 2


# ---------------------------------------------------------------------------
# Cloudflare challenge detection (for BeautifulSoup parsed pages)
# Cloudflare 验证检测（用于 BeautifulSoup 解析后的页面）
# ---------------------------------------------------------------------------
def looks_like_cloudflare_challenge(soup: BeautifulSoup) -> bool:
    """
    Detect Cloudflare challenge pages from parsed HTML.
    从解析后的 HTML 检测 Cloudflare 验证页。

    Only matches actual challenge pages, not pages that merely use Cloudflare CDN.
    仅匹配真正的验证页面，不匹配仅使用 Cloudflare CDN 的页面。
    """
    s = soup.decode().lower()
    title_tag = soup.find("title")
    title = title_tag.get_text().lower() if title_tag else ""
    return (
        "__cf_chl_tk" in s
        or "just a moment" in title
        or "attention required" in title
        or "challenges.cloudflare.com" in s
    )


# ---------------------------------------------------------------------------
# List page parsing / 列表页解析
# ---------------------------------------------------------------------------
def extract_article_links(soup: BeautifulSoup) -> list[str]:
    """
    Extract all article detail URLs from a list page.
    从列表页提取所有文章详情 URL。
    """
    found = []
    seen = set()

    # Try known container selectors first / 优先尝试已知容器选择器
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
        # Diagnostic logging / 诊断日志
        biblio_hrefs = []
        for a in soup.find_all("a", href=True):
            h = (a.get("href") or "").strip()
            if "/publications/biblio/" in h:
                biblio_hrefs.append(h)
                if len(biblio_hrefs) >= 10:
                    break
        if biblio_hrefs:
            log.debug(
                "No article links extracted; sample biblio hrefs (possible regex/URL mismatch) / "
                "列表页未提取到文章链接；biblio href 样本（可能正则/URL 不匹配）: %s",
                biblio_hrefs,
            )
        else:
            any_hrefs = [
                (a.get("href") or "").strip()
                for a in soup.find_all("a", href=True)
                if (a.get("href") or "").strip()
            ][:10]
            log.debug(
                "No article links or biblio hrefs found; sample hrefs / "
                "未发现文章链接或 biblio href；href 样本: %s",
                any_hrefs,
            )
    return found


def find_last_page_number(soup: BeautifulSoup) -> Optional[int]:
    """
    Detect the last page number from pagination links.
    从分页链接中探测最后一页的页码。
    """
    max_page = None
    for a in soup.find_all("a", href=True):
        href = a["href"]
        m = re.search(r"[?&]page=(\d+)", href)
        if m:
            n = int(m.group(1))
            if max_page is None or n > max_page:
                max_page = n
    return max_page


# ---------------------------------------------------------------------------
# Export / 数据导出
# ---------------------------------------------------------------------------
def export_jsonl(conn: sqlite3.Connection, jsonl_path: str) -> None:
    """Export all articles to JSONL file. / 将所有文章导出为 JSONL 文件。"""
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
    """Export all articles to CSV file. / 将所有文章导出为 CSV 文件。"""
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


# ---------------------------------------------------------------------------
# Report / 数据报告
# ---------------------------------------------------------------------------
def get_total_articles(conn: sqlite3.Connection) -> int:
    """Get total article count. / 获取文章总数。"""
    return conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]


def print_report(conn: sqlite3.Connection) -> None:
    """Print a data quality report. / 打印数据质量报告。"""
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

    log.info("========== Data Report / 数据报告 ==========")
    log.info("Total articles / 文章总数: %d", total)
    log.info("Empty title / 标题为空: %d", title_empty)
    log.info("Empty abstract / 摘要为空: %d", abstract_empty)
    log.info("Empty DOI / DOI 为空: %d", doi_empty)
    log.info("Failed list pages / 失败列表页: %d", failed_list)
    log.info("Failed detail pages / 失败详情页: %d", failed_detail)

    top_years = conn.execute("""
        SELECT year, COUNT(*) AS n
        FROM articles
        GROUP BY year
        ORDER BY n DESC
        LIMIT 10
    """).fetchall()

    if top_years:
        log.info("Top years by article count / 文章数最多的年份:")
        for row in top_years:
            log.info("  %s -> %s", row["year"], row["n"])


# ---------------------------------------------------------------------------
# Core scraping logic / 核心抓取逻辑
# ---------------------------------------------------------------------------
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
    """
    Scrape a single article detail page and save to database.
    抓取单个文章详情页并保存到数据库。

    Skips if already exists (unless force_refresh is True).
    如果已存在则跳过（除非 force_refresh 为 True）。
    """
    article_id = url.rstrip("/").split("/")[-1]

    if (not force_refresh) and article_exists(conn, article_id):
        return

    stats.article_pages_attempted += 1
    log.info("Detail page / 详情页: %s", url)

    soup = fetch_soup(session, cfg, url, browser_fetcher=browser_fetcher, referer=referer)
    if not soup:
        stats.article_pages_failed += 1
        record_failed_page(conn, "detail", url, "fetch_failed")
        return

    record = build_record(url, soup)
    if is_suspicious_record(record):
        stats.suspicious_records += 1
        log.warning("Suspicious record (too many empty fields) / 疑似异常记录（空值过多）: %s", url)

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
    """
    Main crawl loop: iterate list pages, scrape each article detail page.
    主爬取循环：遍历列表页，抓取每篇文章的详情页。

    Supports resume from checkpoint (断点续爬).
    """
    session = build_session(cfg)
    stats = ScrapeStats()

    browser_cm = BrowserHTMLFetcher(cfg) if cfg.use_browser else nullcontext(None)
    with browser_cm as browser_fetcher:
        warm_up_session(session, cfg, browser_fetcher=browser_fetcher)
        with closing(init_db(cfg.db_path)) as conn:
            # --- Checkpoint resume / 断点续爬 ---
            if resume and start_page == 0:
                checkpoint = load_checkpoint(conn)
                if checkpoint is not None and checkpoint > 0:
                    start_page = checkpoint
                    log.info("Resuming from checkpoint page=%d / 断点续爬: 从 page=%d 恢复",
                             start_page, start_page)

            # Fetch the first list page / 获取第一个列表页
            first_url = build_list_page_url(start_page)
            first_soup = fetch_soup(session, cfg, first_url, browser_fetcher=browser_fetcher)
            if not first_soup:
                record_failed_page(conn, "list", first_url, "fetch_failed_page")
                log.error("Cannot fetch start page, aborting / 无法抓取起始列表页，终止")
                return

            # Detect pagination upper bound / 探测分页上限
            detected_last_page = find_last_page_number(first_soup)
            if detected_last_page is None:
                log.warning("Cannot detect last page; will stop on consecutive empty pages / "
                            "未探测到分页上限，将在连续空页时停止")
            else:
                log.info("Detected last page: page=%d / 探测到最后一页: page=%d",
                         detected_last_page, detected_last_page)

            page_num = start_page
            empty_streak = 0
            challenge_streak = 0
            processed_pages = 0

            # --- Main pagination loop / 主分页循环 ---
            while True:
                if max_pages is not None and processed_pages >= max_pages:
                    log.info("Reached --max-pages=%d, stopping / 达到 max-pages=%d，停止",
                             max_pages, max_pages)
                    break

                prepared_url = build_list_page_url(page_num)

                if page_num == start_page:
                    soup = first_soup
                else:
                    # Referer = previous page (simulates clicking "next")
                    # 来源页 = 上一页（模拟点击"下一页"）
                    prev_url = build_list_page_url(page_num - 1) if page_num > 0 else LIST_URL
                    soup = fetch_soup(
                        session, cfg, prepared_url,
                        browser_fetcher=browser_fetcher, referer=prev_url,
                    )

                stats.list_pages_seen += 1
                processed_pages += 1
                log.info("List page / 列表页: %s", prepared_url)

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
                    # --- Cloudflare challenge handling / Cloudflare 验证处理 ---
                    if looks_like_cloudflare_challenge(soup):
                        challenge_streak += 1
                        stats.list_pages_failed += 1
                        record_failed_page(conn, "list", prepared_url, "cloudflare_challenge")
                        empty_streak = 0
                        log.warning(
                            "Cloudflare challenge detected / 检测到 Cloudflare 验证: %s",
                            prepared_url,
                        )
                        # Aggressive backoff + UA rotation / 大幅退避 + 轮换 UA
                        backoff = min(30.0 * challenge_streak, 120.0) + random.uniform(5.0, 15.0)
                        log.info("Cloudflare backoff: %.1fs, rotating UA / "
                                 "Cloudflare 退避: %.1f秒，轮换 UA", backoff, backoff)
                        session.headers.update(_pick_ua_and_headers())
                        time.sleep(backoff)
                        # Re-warm after repeated challenges / 连续验证后重新预热
                        if challenge_streak >= 2:
                            log.info("Re-warming session / 重新预热会话")
                            warm_up_session(session, cfg, browser_fetcher=browser_fetcher)
                        page_num += 1
                        if detected_last_page is not None and page_num > detected_last_page:
                            break
                        if max_pages is None and challenge_streak >= 4:
                            log.info("Too many consecutive challenges (>=4), stopping / "
                                     "连续验证次数过多 (>=4)，停止")
                            break
                        continue

                    # --- Empty page handling / 空页处理 ---
                    empty_streak += 1
                    log.warning("No article links on page / 列表页无文章链接: %s", prepared_url)
                    if detected_last_page is None and empty_streak >= 2:
                        log.info("2+ consecutive empty pages, stopping / 连续空页 >= 2，停止")
                        break
                else:
                    empty_streak = 0
                    challenge_streak = 0
                    clear_failed_page(conn, "list", prepared_url)
                    log.info("  -> Found %d articles / 本页找到 %d 篇", len(links), len(links))
                    for link in links:
                        scrape_detail_page(
                            session=session, cfg=cfg, conn=conn, url=link,
                            stats=stats, force_refresh=force_refresh,
                            browser_fetcher=browser_fetcher, referer=prepared_url,
                        )

                # Save checkpoint for resume / 保存断点用于续爬
                save_checkpoint(conn, page_num)

                polite_sleep(cfg.list_delay, cfg)

                if detected_last_page is not None and page_num >= detected_last_page:
                    break

                page_num += 1

            # --- Summary / 汇总 ---
            log.info("========== Crawl complete / 爬取完成 ==========")
            log.info("List pages visited / 列表页访问数: %d", stats.list_pages_seen)
            log.info("List pages failed / 列表页失败数: %d", stats.list_pages_failed)
            log.info("Article links found / 发现文章链接数: %d", stats.article_links_seen)
            log.info("Detail pages attempted / 详情页尝试数: %d", stats.article_pages_attempted)
            log.info("Articles inserted / 新增文章数: %d", stats.article_pages_inserted)
            log.info("Articles updated / 更新文章数: %d", stats.article_pages_updated)
            log.info("Detail pages failed / 详情页失败数: %d", stats.article_pages_failed)
            log.info("Suspicious records / 疑似异常记录数: %d", stats.suspicious_records)
            log.info("Total in database / 数据库总文章数: %d", get_total_articles(conn))


# ---------------------------------------------------------------------------
# Subcommands / 子命令
# ---------------------------------------------------------------------------
def run_export(cfg: Config) -> None:
    """Export database to JSONL and CSV. / 将数据库导出为 JSONL 和 CSV。"""
    with closing(init_db(cfg.db_path)) as conn:
        export_jsonl(conn, cfg.jsonl_path)
        export_csv(conn, cfg.csv_path)
        log.info("Export complete / 导出完成")
        log.info("JSONL -> %s", cfg.jsonl_path)
        log.info("CSV   -> %s", cfg.csv_path)


def run_report(cfg: Config) -> None:
    """Show data quality report. / 显示数据质量报告。"""
    with closing(init_db(cfg.db_path)) as conn:
        print_report(conn)


def retry_failed_detail_pages(
    session: requests.Session,
    cfg: Config,
    conn: sqlite3.Connection,
    force_refresh: bool = True,
    browser_fetcher: Optional[BrowserHTMLFetcher] = None,
) -> None:
    """Retry all previously failed detail pages. / 重试所有之前失败的详情页。"""
    stats = ScrapeStats()
    rows = conn.execute("""
        SELECT DISTINCT url
        FROM failed_pages
        WHERE page_type = 'detail'
        ORDER BY id
    """).fetchall()

    if not rows:
        log.info("No failed detail pages / 没有失败的详情页")
        return

    log.info("Retrying %d failed detail pages / 准备重试 %d 个失败的详情页", len(rows), len(rows))
    for row in rows:
        scrape_detail_page(
            session=session, cfg=cfg, conn=conn, url=row["url"],
            stats=stats, force_refresh=force_refresh, browser_fetcher=browser_fetcher,
        )

    log.info("Retry detail complete: inserted=%d updated=%d failed=%d / "
             "详情页重试完成: 新增=%d 更新=%d 失败=%d",
             stats.article_pages_inserted, stats.article_pages_updated, stats.article_pages_failed,
             stats.article_pages_inserted, stats.article_pages_updated, stats.article_pages_failed)


def retry_failed_list_pages(
    session: requests.Session,
    cfg: Config,
    conn: sqlite3.Connection,
    browser_fetcher: Optional[BrowserHTMLFetcher] = None,
) -> None:
    """Retry all previously failed list pages. / 重试所有之前失败的列表页。"""
    rows = conn.execute("""
        SELECT DISTINCT url
        FROM failed_pages
        WHERE page_type = 'list'
        ORDER BY id
    """).fetchall()

    if not rows:
        log.info("No failed list pages / 没有失败的列表页")
        return

    log.info("Retrying %d failed list pages / 准备重试 %d 个失败的列表页", len(rows), len(rows))
    stats = ScrapeStats()

    for row in rows:
        url = row["url"]
        log.info("Retrying list page / 重试列表页: %s", url)

        soup = fetch_soup(session, cfg, url, browser_fetcher=browser_fetcher)
        if not soup:
            log.error("Retry failed / 重试失败: %s", url)
            continue

        links = extract_article_links(soup)
        if not links:
            log.warning("Retry succeeded but no article links / 重试成功但无文章链接: %s", url)
            continue

        clear_failed_page(conn, "list", url)
        log.info("  -> Found %d articles / 发现 %d 篇", len(links), len(links))

        for link in links:
            scrape_detail_page(
                session=session, cfg=cfg, conn=conn, url=link,
                stats=stats, force_refresh=False, browser_fetcher=browser_fetcher,
            )

        polite_sleep(cfg.list_delay, cfg)

    log.info("Retry list complete: inserted=%d updated=%d failed=%d / "
             "列表页重试完成: 新增=%d 更新=%d 失败=%d",
             stats.article_pages_inserted, stats.article_pages_updated, stats.article_pages_failed,
             stats.article_pages_inserted, stats.article_pages_updated, stats.article_pages_failed)


def run_retry_failed(cfg: Config, detail_only: bool = False, list_only: bool = False) -> None:
    """Retry all failed pages. / 重试所有失败的页面。"""
    with closing(init_db(cfg.db_path)) as conn:
        session = build_session(cfg)

        browser_cm = BrowserHTMLFetcher(cfg) if cfg.use_browser else nullcontext(None)
        with browser_cm as browser_fetcher:
            if not detail_only:
                retry_failed_list_pages(session, cfg, conn, browser_fetcher=browser_fetcher)

            if not list_only:
                retry_failed_detail_pages(
                    session, cfg, conn,
                    force_refresh=True, browser_fetcher=browser_fetcher,
                )


# ---------------------------------------------------------------------------
# CLI argument parsing / 命令行参数解析
# ---------------------------------------------------------------------------
def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="hrs_scraper",
        description=(
            "CLI scraper for HRS journal article bibliography pages.\n"
            "HRS 期刊文章文献目录命令行爬虫。"
        ),
    )

    # Global options / 全局选项
    parser.add_argument("--db", default=DEFAULT_DB_PATH,
                        help="SQLite database path / 数据库路径")
    parser.add_argument("--jsonl", default=DEFAULT_JSONL,
                        help="JSONL export path / JSONL 导出路径")
    parser.add_argument("--csv", default=DEFAULT_CSV,
                        help="CSV export path / CSV 导出路径")
    parser.add_argument("--log-file", default=DEFAULT_LOG_FILE,
                        help="Log file path / 日志文件路径")
    parser.add_argument("--request-timeout", type=int, default=DEFAULT_REQUEST_TIMEOUT,
                        help="HTTP timeout in seconds / HTTP 超时秒数")
    parser.add_argument("--detail-delay", type=float, default=DEFAULT_DETAIL_DELAY,
                        help="Base delay between detail pages / 详情页基础延迟")
    parser.add_argument("--list-delay", type=float, default=DEFAULT_LIST_DELAY,
                        help="Base delay between list pages / 列表页基础延迟")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES,
                        help="Max HTTP retries / 最大 HTTP 重试次数")
    parser.add_argument("--jitter-min", type=float, default=DEFAULT_JITTER_MIN,
                        help="Min random jitter / 最小随机抖动")
    parser.add_argument("--jitter-max", type=float, default=DEFAULT_JITTER_MAX,
                        help="Max random jitter / 最大随机抖动")
    parser.add_argument(
        "--no-browser", action="store_true",
        help="Use plain requests instead of Chrome (no Cloudflare bypass) / "
             "使用 requests 代替 Chrome（无法绕过 Cloudflare）",
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run Chrome in headless mode (no visible window) / "
             "无头模式运行 Chrome（不显示窗口）",
    )
    parser.add_argument(
        "--chrome-profile", default=None,
        help="Chrome user-data-dir for persistent cookies / "
             "Chrome 配置目录（持久化 cookies）",
    )
    parser.add_argument("-v", "--verbose", action="store_true",
                        help="Verbose debug logging / 详细调试日志")

    # Subcommands / 子命令
    subparsers = parser.add_subparsers(dest="command", required=True)

    crawl = subparsers.add_parser("crawl",
                                  help="Crawl list & detail pages / 爬取列表页和详情页")
    crawl.add_argument("--start-page", type=int, default=0,
                       help="Start from this page number / 从此页码开始")
    crawl.add_argument("--max-pages", type=int, default=None,
                       help="Max list pages to process / 最大处理列表页数")
    crawl.add_argument("--force-refresh", action="store_true",
                       help="Re-fetch existing articles / 重新抓取已有文章")
    crawl.add_argument("--no-resume", action="store_true",
                       help="Ignore checkpoint, start fresh / 忽略断点，从头开始")

    subparsers.add_parser("export",
                          help="Export DB to JSONL and CSV / 导出数据库为 JSONL 和 CSV")

    subparsers.add_parser("report",
                          help="Show data quality report / 显示数据质量报告")

    retry_failed = subparsers.add_parser("retry-failed",
                                         help="Retry failed pages / 重试失败页面")
    retry_failed.add_argument("--detail-only", action="store_true",
                              help="Retry only failed detail pages / 仅重试失败的详情页")
    retry_failed.add_argument("--list-only", action="store_true",
                              help="Retry only failed list pages / 仅重试失败的列表页")

    return parser


def parse_args() -> tuple[argparse.Namespace, Config]:
    """Parse CLI args and build Config. / 解析命令行参数并构建配置。"""
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


# ---------------------------------------------------------------------------
# Entry point / 程序入口
# ---------------------------------------------------------------------------
def main() -> None:
    args, cfg = parse_args()
    setup_logging(cfg.log_file, verbose=args.verbose)

    log.info("Command / 启动命令: %s", args.command)
    log.info("Database / 数据库: %s", cfg.db_path)

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
        raise SystemExit(f"Unknown command / 未知命令: {args.command}")


if __name__ == "__main__":
    main()
