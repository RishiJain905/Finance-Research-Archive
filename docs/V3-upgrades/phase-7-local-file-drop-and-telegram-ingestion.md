# Phase 7 — Local File Drop + Telegram Ingestion Bot

**Depends on:** Phase 1 complete  
**Priority:** Medium — quality-of-life improvement that dramatically speeds up manual research capture without any web browsing overhead

---

## What This Phase Does

Adds two new ways to get content into the archive without the automated crawl pipelines:

1. **Local file drop** — drop any PDF, text file, or HTML file into `data/inbox/` and a script ingests it automatically
2. **Telegram ingestion bot** — forward a URL or paste text into a private Telegram chat, and the archive queues it for processing

Both convert their input to the standard raw record format and route through the existing `process_record` pipeline.

---

## Part A — Local File Drop

### How It Works

1. You save a PDF, text file, or HTML file into `data/inbox/`
2. Run `python scripts/ingest_inbox.py` (or trigger via `workflow_dispatch` if you push the file to the repo)
3. The script parses each file, extracts text, creates a raw record, and cleans up `inbox/`
4. The record goes through `process_record` → summarize → verify → route

### Step 1 — Create `scripts/ingest_inbox.py`

Logic:

- Scan `data/inbox/` for files with extensions: `.pdf`, `.txt`, `.html`, `.md`
- For `.pdf`: use `pdfminer.six` or `pypdf` to extract text (add to `requirements.txt`)
- For `.html`: use existing `BeautifulSoup` (already a dependency) to extract body text
- For `.txt` / `.md`: read directly
- Build raw record with:
  - `title`: filename (without extension)
  - `source`: `"inbox"`
  - `url`: `"file://inbox/{filename}"`
  - `content`: extracted text
  - `ingested_at`: current timestamp
- Write to `data/raw/inbox_{hash}.txt`
- Move processed file to `data/inbox/processed/` (keep a copy for reference)

### Step 2 — Create `.github/workflows/process-inbox.yml`

A `workflow_dispatch`-only workflow for when you push files to `data/inbox/` via git and want CI to process them:

```yaml
name: Process Inbox Files

on:
  workflow_dispatch:
  push:
    paths:
      - 'data/inbox/**'

jobs:
  process:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python scripts/ingest_inbox.py --process-workers 2
        env:
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          OPENAI_BASE_URL: ${{ secrets.OPENAI_BASE_URL }}
      - name: Commit results
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/
          git diff --staged --quiet || git commit -m "chore: process inbox files"
          git push
```

### Step 3 — Add `.gitignore` Entry for Inbox

Add `data/inbox/processed/` to `.gitignore` to avoid committing the already-processed originals. Keep `data/inbox/` itself tracked so pushing files there triggers the workflow.

---

## Part B — Telegram Ingestion Bot

You already use Telegram for outbound review notifications (`send_pending_reviews.py`). This extends that to inbound ingestion.

### How It Works

1. You send a message to a private bot chat — either a URL or raw text
2. The bot script queues it in `data/inbox_queue.json`
3. The next pipeline run (or a dedicated lightweight cron) processes the queue

### New GitHub Secrets Required

> **ACTION REQUIRED — add before first run:**
>
> Add the following to your GitHub repository secrets (`Settings → Secrets and variables → Actions → New repository secret`):
>
>
> | Secret name                 | Value                              | Notes                                                                                      |
> | --------------------------- | ---------------------------------- | ------------------------------------------------------------------------------------------ |
> | `TELEGRAM_INGEST_BOT_TOKEN` | Bot token from @BotFather          | Different bot from your review bot — create a new one via @BotFather with `/newbot`        |
> | `TELEGRAM_INGEST_CHAT_ID`   | Your personal chat ID with the bot | Get this by messaging the bot and calling `https://api.telegram.org/bot{TOKEN}/getUpdates` |
>
>
> The existing `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` secrets are for outbound review notifications — keep those separate. The ingest bot should be a different bot so you can distinguish messages sent to it from the notification channel.

### Step 1 — Create `scripts/telegram_ingest_bot.py`

Logic (polling mode — no webhook server needed):

1. Call `https://api.telegram.org/bot{TOKEN}/getUpdates` with an `offset` to get unprocessed messages
2. For each message from the configured `TELEGRAM_INGEST_CHAT_ID`:
  - If it starts with `http://` or `https://`: add to URL queue in `data/inbox_queue.json`
  - Otherwise: save raw text to `data/inbox/telegram_{hash}.txt` for the file drop pipeline
3. Acknowledge messages by advancing the offset
4. Run in one-shot mode (not a long-running server) — called from GitHub Actions on a cron

### Step 2 — Create `.github/workflows/process-telegram-inbox.yml`

```yaml
name: Process Telegram Inbox

on:
  workflow_dispatch:
  schedule:
    - cron: '*/30 * * * *'  # Every 30 minutes

jobs:
  process:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: python scripts/telegram_ingest_bot.py
        env:
          TELEGRAM_INGEST_BOT_TOKEN: ${{ secrets.TELEGRAM_INGEST_BOT_TOKEN }}
          TELEGRAM_INGEST_CHAT_ID: ${{ secrets.TELEGRAM_INGEST_CHAT_ID }}
          OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}
          OPENAI_BASE_URL: ${{ secrets.OPENAI_BASE_URL }}
      - name: Commit results
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "github-actions[bot]@users.noreply.github.com"
          git add data/
          git diff --staged --quiet || git commit -m "chore: telegram inbox run"
          git push
```

### Step 3 — URL Queue Processing

Extend `run_ingest_and_process.py` to optionally process `data/inbox_queue.json` URLs at the end of each article pipeline run. URLs from the queue are fetched with the existing `fetch_html` helper and converted to raw records — same path as a standard ingestion target.

---

## Files Changed in This Phase


| File                                           | Change                                        |
| ---------------------------------------------- | --------------------------------------------- |
| `scripts/ingest_inbox.py`                      | New — local file drop parser                  |
| `scripts/telegram_ingest_bot.py`               | New — Telegram polling bot                    |
| `.github/workflows/process-inbox.yml`          | New — inbox workflow                          |
| `.github/workflows/process-telegram-inbox.yml` | New — Telegram bot workflow                   |
| `scripts/run_ingest_and_process.py`            | Add URL queue drain at end of run             |
| `.gitignore`                                   | Ignore `data/inbox/processed/`                |
| `requirements.txt`                             | Add `pypdf` or `pdfminer.six` for PDF parsing |


---

## Acceptance Criteria

- Dropping a PDF into `data/inbox/` and running `ingest_inbox.py` produces a processed record in `data/accepted/` or `data/review_queue/`
- Sending a URL to the Telegram bot results in that URL being processed within 30 minutes (next scheduled run)
- Sending raw text to the Telegram bot saves it as a `.txt` file in `data/inbox/` and gets picked up by the file drop pipeline
- No credentials are stored in the repository — all secrets remain in GitHub Secrets

