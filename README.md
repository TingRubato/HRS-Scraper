# HRS Journal Article Scraper / HRS 期刊文章爬虫

A CLI scraper for the [Health and Retirement Study (HRS)](https://hrs.isr.umich.edu) publication database. Extracts metadata (title, authors, abstract, DOI, etc.) from journal article bibliography pages.

密歇根大学[健康与退休研究 (HRS)](https://hrs.isr.umich.edu) 出版数据库命令行爬虫。从期刊文章文献目录页提取元数据（标题、作者、摘要、DOI 等）。

## Features / 功能特性

- **Cloudflare Bypass / 绕过 Cloudflare** — Uses [undetected-chromedriver](https://github.com/ultrafunkula/undetected-chromedriver) with a visible Chrome window. When a Cloudflare challenge appears, the scraper pauses and waits for you to solve it manually.
  使用 undetected-chromedriver 打开可见的 Chrome 窗口。当 Cloudflare 验证出现时，爬虫暂停等待您手动完成验证。

- **Resume Crawling / 断点续爬** — Automatically saves progress after each list page. If interrupted, the next run resumes from where it left off.
  每处理完一个列表页自动保存进度。中断后下次运行自动从上次位置继续。

- **Human-like Behavior / 模拟真人行为** — Randomized delays, User-Agent rotation, realistic Referer chains, and occasional "reading" pauses.
  随机延迟、UA 轮换、真实的 Referer 链、偶尔的"阅读"停顿。

- **Persistent Chrome Profile / 持久化浏览器配置** — Cookies (including `cf_clearance`) survive across restarts.
  Cookies（包括 `cf_clearance`）在重启后保留。

- **Failed Page Retry / 失败页面重试** — Failed pages are tracked in the database and can be retried later.
  失败的页面记录在数据库中，可稍后重试。

- **Export / 数据导出** — Export to JSONL and CSV formats.
  支持导出为 JSONL 和 CSV 格式。

## Installation / 安装

```bash
# Install dependencies / 安装依赖
pip install requests beautifulsoup4 lxml undetected-chromedriver

# Chrome must be installed on your system
# 系统需要安装 Chrome 浏览器
```

## Usage / 使用方法

### Crawl / 爬取

```bash
# Start crawling (opens Chrome, resumes from checkpoint)
# 开始爬取（打开 Chrome，从断点恢复）
python hrs_scraper.py crawl

# Start from a specific page / 从指定页码开始
python hrs_scraper.py crawl --start-page 100

# Limit number of list pages / 限制列表页数
python hrs_scraper.py crawl --max-pages 10

# Ignore checkpoint, start fresh / 忽略断点，从头开始
python hrs_scraper.py crawl --no-resume

# Re-fetch articles already in database / 重新抓取已有文章
python hrs_scraper.py crawl --force-refresh

# Run without browser (plain HTTP, no Cloudflare bypass)
# 不使用浏览器（纯 HTTP，无法绕过 Cloudflare）
python hrs_scraper.py crawl --no-browser
```

### Export / 导出

```bash
# Export database to JSONL and CSV / 导出数据库为 JSONL 和 CSV
python hrs_scraper.py export

# Custom output paths / 自定义输出路径
python hrs_scraper.py export --jsonl output.jsonl --csv output.csv
```

### Report / 报告

```bash
# Show data quality report / 显示数据质量报告
python hrs_scraper.py report
```

### Retry Failed Pages / 重试失败页面

```bash
# Retry all failed pages / 重试所有失败页面
python hrs_scraper.py retry-failed

# Retry only failed detail pages / 仅重试失败的详情页
python hrs_scraper.py retry-failed --detail-only

# Retry only failed list pages / 仅重试失败的列表页
python hrs_scraper.py retry-failed --list-only
```

## Global Options / 全局选项

| Option | Default | Description |
|--------|---------|-------------|
| `--db` | `hrs_scraper.db` | SQLite database path / 数据库路径 |
| `--jsonl` | `hrs_abstracts.jsonl` | JSONL export path / JSONL 导出路径 |
| `--csv` | `hrs_abstracts.csv` | CSV export path / CSV 导出路径 |
| `--log-file` | `scraper.log` | Log file path / 日志文件路径 |
| `--no-browser` | off | Use plain requests (no Cloudflare bypass) / 纯 HTTP 模式 |
| `--headless` | off | Hide Chrome window / 无头模式 |
| `--chrome-profile` | `~/.hrs_scraper_chrome_profile` | Chrome profile directory / Chrome 配置目录 |
| `--detail-delay` | `1.5` | Base delay between detail pages (seconds) / 详情页延迟 |
| `--list-delay` | `2.5` | Base delay between list pages (seconds) / 列表页延迟 |
| `--request-timeout` | `30` | HTTP timeout (seconds) / HTTP 超时 |
| `--max-retries` | `5` | Max HTTP retries / 最大重试次数 |
| `-v, --verbose` | off | Debug-level logging / 调试日志 |

## Database Schema / 数据库表结构

### `articles` — Article metadata / 文章元数据

| Column | Description |
|--------|-------------|
| `article_id` | HRS internal ID (primary key) / HRS 内部 ID（主键） |
| `url` | Detail page URL / 详情页 URL |
| `title` | Article title / 文章标题 |
| `authors` | Author list / 作者列表 |
| `abstract` | Abstract text / 摘要 |
| `journal` | Journal name / 期刊名 |
| `year` | Publication year / 发表年份 |
| `doi` | DOI identifier / DOI 标识符 |
| `volume`, `pagination` | Volume and pages / 卷号和页码 |
| `issn` | ISSN number / ISSN 号 |
| `keywords` | Keywords / 关键词 |
| `pubmed_id` | PubMed ID |
| `citation_key` | Citation key / 引用键 |
| `scraped_at`, `updated_at` | Timestamps / 时间戳 |

### `failed_pages` — Tracked failures for retry / 失败记录

### `crawl_checkpoint` — Resume position / 断点续爬位置

## How It Works / 工作原理

```
1. Launch Chrome (undetected-chromedriver)
   启动 Chrome（undetected-chromedriver）

2. Warm-up: visit homepage -> publications -> biblio
   预热：访问首页 -> 出版物 -> 文献目录

3. For each list page (paginated):
   遍历每个列表页（分页）：
   a. Extract article links / 提取文章链接
   b. For each article: fetch detail page, parse fields, save to DB
      对每篇文章：获取详情页，解析字段，保存到数据库
   c. Save checkpoint / 保存断点

4. If Cloudflare challenge appears:
   如果出现 Cloudflare 验证：
   - Chrome window comes to foreground / Chrome 窗口置前
   - Scraper pauses, waiting for you to click through / 爬虫暂停，等待您点击通过
   - Resumes automatically after challenge is solved / 验证通过后自动继续
```

## Output Formats / 输出格式

### JSONL (`hrs_abstracts.jsonl`)

One JSON object per line. Each object contains all fields from the `articles` table.
每行一个 JSON 对象，包含 `articles` 表的所有字段。

### CSV (`hrs_abstracts.csv`)

Standard CSV with header row. Same fields as JSONL.
带表头的标准 CSV，字段与 JSONL 相同。

## License / 许可

MIT
