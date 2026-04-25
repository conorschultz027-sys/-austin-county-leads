"""
Austin County, TX — Motivated Seller Lead Scraper
==================================================
Clerk portal : https://www.tccsearch.org/  (Playwright async)
Parcel data  : Austin County Appraisal District + PTAD fallback (requests + dbfread)

Run:
    python scraper/fetch.py

Environment variables (optional):
    LOOKBACK_DAYS   — default 7
    HEADLESS        — set to "false" to watch the browser (local debugging only)
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
import os
import re
import sys
import time
import zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

# ── optional deps ──────────────────────────────────────────────────────────────
try:
    from dbfread import DBF
    HAS_DBF = True
except ImportError:
    HAS_DBF = False

try:
    from playwright.async_api import async_playwright
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

# ── logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger("fetch")

# ── paths ──────────────────────────────────────────────────────────────────────
ROOT      = Path(__file__).resolve().parent.parent
DASHBOARD = ROOT / "dashboard"
DATA_DIR  = ROOT / "data"
CACHE_DIR = ROOT / ".cache"
for _d in (DASHBOARD, DATA_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)

# ── config ─────────────────────────────────────────────────────────────────────
LOOKBACK_DAYS    = int(os.getenv("LOOKBACK_DAYS", "7"))
HEADLESS         = os.getenv("HEADLESS", "true").lower() != "false"
CLERK_BASE       = "https://www.tccsearch.org"
CLERK_SEARCH     = "https://www.tccsearch.org/RealEstate/SearchEntry.aspx"
REQUEST_TIMEOUT  = 30   # seconds for HTTP requests
RETRY_ATTEMPTS   = 3
RETRY_DELAY      = 3.0  # seconds (multiplied by attempt number)

# ── document type catalogue ────────────────────────────────────────────────────
DOC_TYPES: dict[str, dict[str, Any]] = {
    "LP":       {"label": "Lis Pendens",             "cat": "lis_pendens", "flags": ["Lis pendens", "Pre-foreclosure"]},
    "NOFC":     {"label": "Notice of Foreclosure",   "cat": "foreclosure", "flags": ["Pre-foreclosure"]},
    "TAXDEED":  {"label": "Tax Deed",                "cat": "tax_deed",    "flags": ["Tax lien"]},
    "JUD":      {"label": "Judgment",                "cat": "judgment",    "flags": ["Judgment lien"]},
    "CCJ":      {"label": "Certified Judgment",      "cat": "judgment",    "flags": ["Judgment lien"]},
    "DRJUD":    {"label": "Domestic Judgment",       "cat": "judgment",    "flags": ["Judgment lien"]},
    "LNCORPTX": {"label": "Corp Tax Lien",           "cat": "lien",        "flags": ["Tax lien"]},
    "LNIRS":    {"label": "IRS Lien",                "cat": "lien",        "flags": ["Tax lien"]},
    "LNFED":    {"label": "Federal Lien",            "cat": "lien",        "flags": ["Tax lien"]},
    "LN":       {"label": "Lien",                    "cat": "lien",        "flags": []},
    "LNMECH":   {"label": "Mechanic Lien",           "cat": "lien",        "flags": ["Mechanic lien"]},
    "LNHOA":    {"label": "HOA Lien",                "cat": "lien",        "flags": []},
    "MEDLN":    {"label": "Medicaid Lien",           "cat": "lien",        "flags": []},
    "PRO":      {"label": "Probate",                 "cat": "probate",     "flags": ["Probate / estate"]},
    "NOC":      {"label": "Notice of Commencement",  "cat": "notice",      "flags": []},
    "RELLP":    {"label": "Release Lis Pendens",     "cat": "release",     "flags": []},
}
TARGET_CODES = set(DOC_TYPES.keys())

# raw instrument string → our code  (uppercase keys)
INSTRUMENT_MAP: dict[str, str] = {
    "LIS PENDENS": "LP",
    "LP": "LP",
    "LIS PENDEN": "LP",
    "NOTICE OF FORECLOSURE": "NOFC",
    "FORECLOSURE": "NOFC",
    "NOTICE OF TRUSTEE SALE": "NOFC",
    "SUBSTITUTE TRUSTEE": "NOFC",
    "SUBSTITUTE TRUSTEE'S DEED": "NOFC",
    "TRUSTEE'S DEED": "NOFC",
    "TRUSTEE DEED": "NOFC",
    "DEED OF TRUST - FORECLOSURE": "NOFC",
    "TAX DEED": "TAXDEED",
    "CONSTABLE TAX DEED": "TAXDEED",
    "SHERIFF DEED": "TAXDEED",
    "SHERIFF'S DEED": "TAXDEED",
    "ABSTRACT OF JUDGMENT": "JUD",
    "ABSTRACT OF JUDGEMENT": "JUD",
    "JUDGMENT": "JUD",
    "JUDGEMENT": "JUD",
    "FOREIGN JUDGMENT": "JUD",
    "FOREIGN JUDGEMENT": "JUD",
    "CERTIFIED COPY OF JUDGMENT": "CCJ",
    "CERTIFIED JUDGMENT": "CCJ",
    "CERTIFIED JUDGEMENT": "CCJ",
    "DOMESTIC JUDGMENT": "DRJUD",
    "DOMESTIC JUDGEMENT": "DRJUD",
    "DOMESTIC RELATIONS ORDER": "DRJUD",
    "CORPORATE TAX LIEN": "LNCORPTX",
    "CORP TAX LIEN": "LNCORPTX",
    "STATE TAX LIEN": "LNCORPTX",
    "TEXAS WORKFORCE COMMISSION LIEN": "LNCORPTX",
    "TWC LIEN": "LNCORPTX",
    "IRS LIEN": "LNIRS",
    "FEDERAL TAX LIEN": "LNIRS",
    "FED TAX LIEN": "LNIRS",
    "NOTICE OF FEDERAL TAX LIEN": "LNIRS",
    "NOTICE OF TAX LIEN": "LNIRS",
    "FEDERAL LIEN": "LNFED",
    "LIEN": "LN",
    "MECHANIC LIEN": "LNMECH",
    "MECHANIC'S LIEN": "LNMECH",
    "MATERIALMAN LIEN": "LNMECH",
    "MATERIALMAN'S LIEN": "LNMECH",
    "CONTRACTOR LIEN": "LNMECH",
    "HOA LIEN": "LNHOA",
    "HOMEOWNER ASSOCIATION LIEN": "LNHOA",
    "HOMEOWNERS ASSOCIATION LIEN": "LNHOA",
    "HOMEOWNERS ASSOC LIEN": "LNHOA",
    "MEDICAID LIEN": "MEDLN",
    "MEDICAL ASSISTANCE LIEN": "MEDLN",
    "PROBATE": "PRO",
    "LETTERS TESTAMENTARY": "PRO",
    "LETTERS OF ADMINISTRATION": "PRO",
    "MUNIMENT OF TITLE": "PRO",
    "AFFIDAVIT OF HEIRSHIP": "PRO",
    "NOTICE OF COMMENCEMENT": "NOC",
    "RELEASE OF LIS PENDENS": "RELLP",
    "RELEASE LIS PENDENS": "RELLP",
    "CANCELLATION OF LIS PENDENS": "RELLP",
}

# instrument keywords to search one-by-one if broad search returns nothing
SEARCH_TERMS = [
    "LIS PENDENS",
    "FORECLOSURE",
    "TRUSTEE",
    "TAX DEED",
    "JUDGMENT",
    "ABSTRACT",
    "IRS LIEN",
    "FEDERAL TAX LIEN",
    "MECHANIC LIEN",
    "HOA LIEN",
    "LIEN",
    "PROBATE",
    "LETTERS TESTAMENTARY",
    "NOTICE OF COMMENCEMENT",
    "RELEASE LIS PENDENS",
]


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def retry_call(fn, attempts: int = RETRY_ATTEMPTS, delay: float = RETRY_DELAY):
    """Call fn(), retrying on any exception with back-off."""
    last: Exception | None = None
    for i in range(attempts):
        try:
            return fn()
        except Exception as exc:
            last = exc
            log.warning("Attempt %d/%d failed — %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    log.error("All %d attempts failed. Last: %s", attempts, last)
    return None


def safe(v, default: str = "") -> str:
    return default if v is None else str(v).strip()


def parse_amount(text: str) -> float | None:
    cleaned = re.sub(r"[$,\s]", "", safe(text))
    m = re.search(r"\d+(?:\.\d{1,2})?", cleaned)
    return float(m.group()) if m else None


def map_instrument(raw: str) -> str | None:
    """Map raw instrument string → DOC_TYPE key, or None."""
    if not raw:
        return None
    upper = raw.strip().upper()
    # exact match first
    if upper in INSTRUMENT_MAP:
        return INSTRUMENT_MAP[upper]
    # substring match
    for key, code in INSTRUMENT_MAP.items():
        if key in upper:
            return code
    return None


def norm_date(raw: str) -> str:
    """Parse any common date format → YYYY-MM-DD, or ''."""
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y",
                "%Y%m%d", "%m/%d/%y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(raw.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return ""


def doc_url(doc_num: str) -> str:
    return f"{CLERK_BASE}/RealEstate/DocView.aspx?id={doc_num}" if doc_num else CLERK_BASE


def name_variants(name: str) -> list[str]:
    """
    Return lookup keys for owner name in three forms:
      'JOHN SMITH'  →  {'JOHN SMITH', 'SMITH JOHN', 'SMITH, JOHN'}
    """
    name = name.strip().upper()
    variants: set[str] = {name}
    cleaned = name.rstrip(",")
    variants.add(cleaned)
    parts = re.split(r"[\s,]+", cleaned)
    parts = [p for p in parts if p]
    if len(parts) >= 2:
        variants.add(f"{parts[-1]} {' '.join(parts[:-1])}")
        variants.add(f"{parts[-1]}, {' '.join(parts[:-1])}")
        variants.add(f"{' '.join(parts[1:])} {parts[0]}")
    return [v for v in variants if v]


# ══════════════════════════════════════════════════════════════════════════════
#  SELLER SCORING
# ══════════════════════════════════════════════════════════════════════════════

def score_record(rec: dict) -> tuple[int, list[str]]:
    """
    Seller score 0-100:
      Base 30
      +10  per distress flag (Lis pendens, Pre-foreclosure, Judgment lien,
           Tax lien, Mechanic lien, Probate / estate, LLC / corp owner)
      +20  LP + Pre-foreclosure combo
      +15  amount > $100,000
      +10  amount > $50,000
      +5   filed within 7 days
      +5   has property address
    """
    flags: list[str] = list(DOC_TYPES.get(rec.get("doc_type", ""), {}).get("flags", []))

    # LLC / corporate owner detection
    owner_up = safe(rec.get("owner", "")).upper()
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bL\.P\.\b|\bLTD\b|\bTRUST\b|\bFUND\b|\bINVEST", owner_up):
        flags.append("LLC / corp owner")

    # new this week
    try:
        if (datetime.now() - datetime.strptime(rec.get("filed", ""), "%Y-%m-%d")).days <= 7:
            flags.append("New this week")
    except Exception:
        pass

    # deduplicate preserving order
    seen: set[str] = set()
    flags = [f for f in flags if not (f in seen or seen.add(f))]  # type: ignore[func-returns-value]

    score = 30

    DISTRESS = {
        "Lis pendens", "Pre-foreclosure", "Judgment lien",
        "Tax lien", "Mechanic lien", "Probate / estate", "LLC / corp owner",
    }
    score += sum(10 for f in flags if f in DISTRESS)

    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

    amt = rec.get("amount")
    if amt:
        if   amt > 100_000: score += 15
        elif amt >  50_000: score += 10

    if "New this week" in flags:
        score += 5
    if rec.get("prop_address"):
        score += 5

    return min(score, 100), flags


# ══════════════════════════════════════════════════════════════════════════════
#  PARCEL LOOKUP  (Austin County Appraisal District + PTAD fallback)
# ══════════════════════════════════════════════════════════════════════════════

class ParcelLookup:
    """
    Attempts to download the county appraisal district bulk parcel DBF,
    builds an owner-name → address index.

    Sources tried in order:
      1. Austin County Appraisal District  (austincad.org)
      2. Texas Comptroller PTAD export
      3. (fail gracefully — scraper continues without addresses)
    """

    ACAD_URLS = [
        "https://austincad.org/data-downloads",
        "https://austincad.org/downloads",
        "https://austincad.org/publicdata",
        "https://austincad.org/GIS",
        "https://austincad.org",
    ]

    def __init__(self):
        self._index: dict[str, dict] = {}

    # ── public API ─────────────────────────────────────────────────────────────

    def load(self):
        if not HAS_DBF:
            log.warning("dbfread not installed — address enrichment skipped.")
            return
        dbf_path = retry_call(self._find_dbf, attempts=2, delay=4.0)
        if dbf_path:
            self._build_index(dbf_path)
        else:
            log.warning("No parcel DBF obtained — records will have no addresses.")

    def lookup(self, name: str) -> dict | None:
        if not name:
            return None
        for variant in name_variants(name):
            hit = self._index.get(variant)
            if hit:
                return hit
        # last-name prefix fallback (catches "SMITH JOHN D" when index has "SMITH JOHN")
        token = name.strip().upper().split()[0]
        if len(token) > 3:
            for key, val in self._index.items():
                if key.startswith(token):
                    return val
        return None

    # ── source 1: ACAD ─────────────────────────────────────────────────────────

    def _find_dbf(self) -> Path | None:
        result = self._try_acad()
        if result:
            return result
        result = self._try_ptad()
        if result:
            return result
        log.warning("All parcel data sources exhausted.")
        return None

    def _try_acad(self) -> Path | None:
        cache = CACHE_DIR / "acad_parcels.dbf"
        if cache.exists() and (time.time() - cache.stat().st_mtime) < 86_400:
            log.info("Using cached ACAD DBF: %s", cache.name)
            return cache

        headers = {"User-Agent": "Mozilla/5.0 (compatible; LeadScraper/1.0)"}
        for url in self.ACAD_URLS:
            try:
                log.info("Checking ACAD source: %s", url)
                resp = requests.get(url, timeout=REQUEST_TIMEOUT, headers=headers)
                if not resp.ok:
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href: str = a["href"]
                    if any(kw in href.lower() for kw in
                           (".dbf", ".zip", "parcel", "apprais", "export", "download", "bulk")):
                        full = href if href.startswith("http") else urljoin(url, href)
                        dl   = self._download(full, CACHE_DIR / "acad_raw")
                        if dl:
                            dbf = self._unpack(dl)
                            if dbf:
                                return dbf
            except Exception as e:
                log.debug("ACAD %s: %s", url, e)
        return None

    # ── source 2: PTAD ─────────────────────────────────────────────────────────

    def _try_ptad(self) -> Path | None:
        cache = CACHE_DIR / "ptad_parcels.dbf"
        if cache.exists() and (time.time() - cache.stat().st_mtime) < 86_400:
            log.info("Using cached PTAD DBF: %s", cache.name)
            return cache

        year = datetime.now().year
        patterns = [
            "https://comptroller.texas.gov/taxes/property-tax/county-directory/data/austin-county-{y}.zip",
            "https://comptroller.texas.gov/taxes/property-tax/county-directory/data/austin-{y}.zip",
            "https://storage.googleapis.com/ptad-downloads/austin-county-{y}.zip",
        ]
        for y in (year, year - 1):
            for pat in patterns:
                url = pat.format(y=y)
                dl  = self._download(url, CACHE_DIR / f"ptad_{y}.zip")
                if dl:
                    dbf = self._unpack(dl)
                    if dbf:
                        return dbf

        # try scraping the PTAD directory page
        try:
            resp = requests.get(
                "https://comptroller.texas.gov/taxes/property-tax/county-directory/",
                timeout=REQUEST_TIMEOUT,
            )
            if resp.ok:
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    if "austin" in a["href"].lower() and (
                        ".zip" in a["href"].lower() or ".dbf" in a["href"].lower()
                    ):
                        full = a["href"] if a["href"].startswith("http") \
                               else urljoin("https://comptroller.texas.gov", a["href"])
                        dl = self._download(full, CACHE_DIR / "ptad_found.zip")
                        if dl:
                            dbf = self._unpack(dl)
                            if dbf:
                                return dbf
        except Exception as e:
            log.debug("PTAD directory scrape: %s", e)

        return None

    # ── download / unpack ──────────────────────────────────────────────────────

    def _download(self, url: str, dest: Path) -> Path | None:
        try:
            log.info("Downloading: %s", url)
            with requests.get(
                url, stream=True, timeout=120,
                headers={"User-Agent": "Mozilla/5.0"}
            ) as r:
                if r.status_code != 200:
                    log.debug("HTTP %s — %s", r.status_code, url)
                    return None
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(65_536):
                        f.write(chunk)
            size_kb = dest.stat().st_size / 1024
            log.info("Saved %s (%.1f KB)", dest.name, size_kb)
            return dest if size_kb > 1 else None      # ignore tiny error pages
        except Exception as e:
            log.debug("Download error %s: %s", url, e)
            return None

    def _unpack(self, path: Path) -> Path | None:
        if path.suffix.lower() == ".dbf":
            return path
        if path.suffix.lower() == ".zip":
            try:
                with zipfile.ZipFile(path) as zf:
                    dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                    if not dbf_names:
                        log.warning("No DBF inside %s", path.name)
                        return None
                    # pick the biggest DBF (owner/parcel file)
                    dbf_names.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
                    out = CACHE_DIR / Path(dbf_names[0]).name
                    with zf.open(dbf_names[0]) as src, open(out, "wb") as dst:
                        dst.write(src.read())
                    log.info("Extracted: %s", out.name)
                    return out
            except zipfile.BadZipFile:
                log.warning("Bad ZIP: %s", path.name)
        return None

    # ── index builder ──────────────────────────────────────────────────────────

    def _build_index(self, path: Path):
        log.info("Indexing parcels from %s …", path.name)
        count = 0
        try:
            table  = DBF(str(path), encoding="latin-1", ignore_missing_memofile=True)
            fields = {f.name.upper() for f in table.fields}

            def col(*candidates: str) -> str | None:
                for c in candidates:
                    if c.upper() in fields:
                        return c.upper()
                return None

            c_own    = col("OWN1",    "OWNER",      "OWNER_NAME", "OWNERNAME",  "NAME")
            c_site   = col("SITEADDR","SITE_ADDR",  "SITUS_ADDR", "PROP_ADDR",  "SITE_ADDRESS")
            c_scity  = col("SITE_CITY","SITECITY",  "SITUS_CITY", "PROP_CITY")
            c_szip   = col("SITE_ZIP", "SITEZIP",   "SITUS_ZIP",  "PROP_ZIP")
            c_mail1  = col("MAILADR1", "ADDR_1",    "MAIL_ADDR1", "MAIL1",      "MAIL_ADDRESS")
            c_mcity  = col("MAILCITY", "CITY",      "MAIL_CITY",  "MCITY")
            c_mstate = col("STATE",    "MAIL_STATE","MAILSTATE",  "MSTATE")
            c_mzip   = col("MAILZIP",  "ZIP",       "MAIL_ZIP",   "MZIP")

            for row in table:
                try:
                    owner = safe(row.get(c_own)) if c_own else ""
                    if not owner:
                        continue
                    parcel = {
                        "prop_address": safe(row.get(c_site))   if c_site   else "",
                        "prop_city":    safe(row.get(c_scity))  if c_scity  else "",
                        "prop_state":   "TX",
                        "prop_zip":     safe(row.get(c_szip))   if c_szip   else "",
                        "mail_address": safe(row.get(c_mail1))  if c_mail1  else "",
                        "mail_city":    safe(row.get(c_mcity))  if c_mcity  else "",
                        "mail_state":   safe(row.get(c_mstate)) if c_mstate else "TX",
                        "mail_zip":     safe(row.get(c_mzip))   if c_mzip   else "",
                    }
                    for v in name_variants(owner):
                        self._index[v] = parcel
                    count += 1
                except Exception:
                    continue
        except Exception as e:
            log.error("DBF read error: %s", e)

        log.info("Parcel index: %d owners, %d name keys.", count, len(self._index))


# ══════════════════════════════════════════════════════════════════════════════
#  CLERK SCRAPER  (tccsearch.org — ASP.NET WebForms via Playwright)
# ══════════════════════════════════════════════════════════════════════════════

class ClerkScraper:
    """
    Playwright-based async scraper for Austin County Clerk portal.

    tccsearch.org is an ASP.NET WebForms app.  The approach:
      1. Load SearchEntry.aspx, capture ViewState/EventValidation.
      2. Fill the date-range fields and submit.
      3. Parse the GridView result table row-by-row.
      4. Follow Next-page links until exhausted.
      5. If date-only search returns 0 rows, loop over SEARCH_TERMS.
    """

    def __init__(self, start: datetime, end: datetime):
        self.start    = start
        self.end      = end
        self._seen:   set[str] = set()
        self.raw:     list[dict] = []

    async def run(self) -> list[dict]:
        if not HAS_PLAYWRIGHT:
            log.error("Playwright not installed — clerk scraping skipped.")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=HEADLESS,
                args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu"],
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (X11; Linux x86_64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()
            page.set_default_timeout(30_000)

            try:
                # ── Strategy A: broad date-range search ───────────────────────
                log.info("Loading clerk portal: %s", CLERK_SEARCH)
                await page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=60_000)
                await page.wait_for_timeout(2_000)

                await self._fill_dates(page)
                clicked = await self._click_search(page)
                if clicked:
                    await page.wait_for_load_state("networkidle", timeout=30_000)
                    await page.wait_for_timeout(1_500)
                    recs = await self._harvest_pages(page)
                    self.raw.extend(recs)
                    log.info("Broad search: %d records.", len(recs))

                # ── Strategy B: per-term searches (always run for completeness) ──
                log.info("Running per-term searches (%d terms) …", len(SEARCH_TERMS))
                for term in SEARCH_TERMS:
                    try:
                        await page.goto(CLERK_SEARCH, wait_until="networkidle", timeout=30_000)
                        await page.wait_for_timeout(1_000)
                        await self._fill_instrument(page, term)
                        await self._fill_dates(page)
                        clicked = await self._click_search(page)
                        if not clicked:
                            continue
                        await page.wait_for_load_state("networkidle", timeout=30_000)
                        await page.wait_for_timeout(1_000)
                        recs = await self._harvest_pages(page)
                        log.info("  '%s' → %d records.", term, len(recs))
                        self.raw.extend(recs)
                    except Exception as e:
                        log.warning("Term '%s' failed: %s", term, e)

            except Exception as e:
                log.error("Clerk scraper fatal: %s", e, exc_info=True)
            finally:
                await browser.close()

        log.info("Clerk total raw: %d records (%d unique doc nums).",
                 len(self.raw), len(self._seen))
        return self.raw

    # ── form helpers ───────────────────────────────────────────────────────────

    async def _fill_dates(self, page):
        s = self.start.strftime("%m/%d/%Y")
        e = self.end.strftime("%m/%d/%Y")

        # Try every known field-name pattern used by tccsearch
        pairs = [
            ("txtFromDate",                                  s),
            ("txtToDate",                                    e),
            ("txtDateFrom",                                  s),
            ("txtDateTo",                                    e),
            ("ctl00$ContentPlaceHolder1$txtFromDate",        s),
            ("ctl00$ContentPlaceHolder1$txtToDate",          e),
            ("ctl00$ContentPlaceHolder1$txtDateFrom",        s),
            ("ctl00$ContentPlaceHolder1$txtDateTo",          e),
            ("ctl00$ContentPlaceHolder1$txtStartDate",       s),
            ("ctl00$ContentPlaceHolder1$txtEndDate",         e),
        ]
        filled = 0
        for name, val in pairs:
            try:
                el = page.locator(
                    f"input[name='{name}'], input[id='{name}']"
                ).first
                if await el.count() > 0:
                    await el.fill(val)
                    filled += 1
            except Exception:
                pass

        # Generic fallback: grab all text inputs with "date" in the id/name
        if filled < 2:
            date_inputs = page.locator(
                "input[type='text'][id*='ate' i], input[type='text'][name*='ate' i]"
            )
            n = await date_inputs.count()
            if n >= 2:
                try:
                    await date_inputs.nth(0).fill(s)
                    await date_inputs.nth(1).fill(e)
                except Exception:
                    pass

    async def _fill_instrument(self, page, term: str):
        selectors = [
            "input[name*='Instrument' i]",
            "input[name*='DocType' i]",
            "input[name*='InstrType' i]",
            "#txtInstrumentType",
            "#txtDocType",
            "input[id*='Instrument' i]",
        ]
        for sel in selectors:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.fill(term)
                    return
            except Exception:
                pass

        # Try a <select> dropdown
        for sel in ["select[name*='Instrument' i]", "select[name*='DocType' i]",
                    "#ddlInstrumentType", "#ddlDocType"]:
            try:
                dd = page.locator(sel).first
                if await dd.count() > 0:
                    opts = await dd.locator("option").all_text_contents()
                    match = next((o for o in opts if term in o.upper()), None)
                    if match:
                        await dd.select_option(label=match)
                    return
            except Exception:
                pass

    async def _click_search(self, page) -> bool:
        selectors = [
            "input[type='submit'][value*='Search' i]",
            "input[type='button'][value*='Search' i]",
            "button[type='submit']",
            "#btnSearch",
            "#ctl00_ContentPlaceHolder1_btnSearch",
            "a:has-text('Search')",
            "button:has-text('Search')",
        ]
        for sel in selectors:
            try:
                btn = page.locator(sel).first
                if await btn.count() > 0:
                    await btn.click()
                    return True
            except Exception:
                pass
        log.warning("Could not find search button.")
        return False

    # ── pagination + parsing ───────────────────────────────────────────────────

    async def _harvest_pages(self, page) -> list[dict]:
        records: list[dict] = []
        pg = 1
        while True:
            html = await page.content()
            batch = self._parse_html(html)
            records.extend(batch)

            if pg == 1 and not batch:
                break

            # next page
            advanced = False
            for sel in [
                "a:has-text('Next')", "a:has-text('>')",
                "input[value='Next']", "a[href*='Page$Next']",
                "a[title='Next Page']", ".pagination a:last-child",
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.count() > 0 and await btn.is_visible():
                        await btn.click()
                        await page.wait_for_load_state("networkidle", timeout=20_000)
                        await page.wait_for_timeout(800)
                        advanced = True
                        pg += 1
                        break
                except Exception:
                    pass

            if not advanced or pg > 100:
                break

        return records

    def _parse_html(self, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")

        # quick no-results check
        body = soup.get_text(" ", strip=True).lower()
        if any(p in body for p in ("no records found", "no results found",
                                    "0 records", "no data")):
            return []

        # find the best data table (most <td> rows)
        best_tbl, best_n = None, 0
        for tbl in soup.find_all("table"):
            n = len(tbl.find_all("tr"))
            if n > best_n:
                best_tbl, best_n = tbl, n

        if best_tbl and best_n >= 2:
            return self._parse_table(best_tbl)

        return self._parse_cards(soup)

    def _parse_table(self, tbl) -> list[dict]:
        rows = tbl.find_all("tr")
        if not rows:
            return []

        # header detection
        hdr_cells = rows[0].find_all(["th", "td"])
        headers   = [c.get_text(strip=True).upper() for c in hdr_cells]

        def cidx(*names: str) -> int | None:
            for name in names:
                for i, h in enumerate(headers):
                    if name in h:
                        return i
            return None

        i_num  = cidx("INSTRUMENT NO", "INSTRUMENT NUMBER", "DOC NO", "DOC NUMBER",
                       "INSTR NO", "NUMBER", "INSTRUMENT")
        i_type = cidx("INSTRUMENT TYPE", "INSTR TYPE", "DOC TYPE", "TYPE", "RECORD TYPE")
        i_date = cidx("DATE FILED", "FILED DATE", "FILE DATE", "RECORD DATE", "DATE")
        i_gror = cidx("GRANTOR", "GRANTORS", "OWNER", "FROM", "SELLER")
        i_gree = cidx("GRANTEE", "GRANTEES", "BUYER", "TO")
        i_legal= cidx("LEGAL", "LEGAL DESC", "DESCRIPTION", "REMARKS")
        i_amt  = cidx("AMOUNT", "CONSIDERATION", "DEBT", "VALUE")

        records: list[dict] = []
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells:
                continue
            try:
                def ct(idx: int | None) -> str:
                    return "" if (idx is None or idx >= len(cells)) \
                               else cells[idx].get_text(strip=True)

                # grab doc link from any <a> in the row
                clerk_url = ""
                for a in row.find_all("a", href=True):
                    href: str = a["href"]
                    if any(kw in href.lower() for kw in
                           ("view", "doc", "instr", "detail", "record", "id=")):
                        clerk_url = href if href.startswith("http") \
                                    else urljoin(CLERK_BASE, href)
                        break
                if not clerk_url:
                    a_any = row.find("a", href=True)
                    if a_any:
                        h = a_any["href"]
                        clerk_url = h if h.startswith("http") else urljoin(CLERK_BASE, h)

                raw_type = ct(i_type)
                raw_num  = ct(i_num)
                if not raw_num:
                    m = re.search(r"id=([^&\s]+)", clerk_url, re.I)
                    raw_num = m.group(1) if m else ""

                if not raw_num or raw_num in self._seen:
                    continue
                self._seen.add(raw_num)

                records.append({
                    "_raw_type": raw_type,
                    "doc_code":  map_instrument(raw_type),
                    "doc_num":   raw_num,
                    "filed":     norm_date(ct(i_date)),
                    "owner":     ct(i_gror),
                    "grantee":   ct(i_gree),
                    "legal":     ct(i_legal),
                    "amount":    parse_amount(ct(i_amt)),
                    "clerk_url": clerk_url or doc_url(raw_num),
                })
            except Exception as e:
                log.debug("Row parse error: %s", e)

        return records

    def _parse_cards(self, soup: BeautifulSoup) -> list[dict]:
        """Fallback: non-table result layouts."""
        records: list[dict] = []
        containers = soup.select(
            ".result-item, .search-result, .record-row, "
            "[class*='result'], [class*='record'], [id*='rpt']"
        )
        for div in containers:
            try:
                text  = div.get_text(" ", strip=True)
                m_num = re.search(r"(?:Instrument|Doc|No|#)[:\s#]*([A-Z0-9\-]+)", text, re.I)
                m_dt  = re.search(r"\d{1,2}/\d{1,2}/\d{4}", text)
                link  = div.find("a", href=True)
                href  = link["href"] if link else ""
                url   = href if href.startswith("http") else urljoin(CLERK_BASE, href)
                num   = m_num.group(1) if m_num else ""
                if not num or num in self._seen:
                    continue
                self._seen.add(num)
                records.append({
                    "_raw_type": "",
                    "doc_code":  None,
                    "doc_num":   num,
                    "filed":     norm_date(m_dt.group() if m_dt else ""),
                    "owner":     "",
                    "grantee":   "",
                    "legal":     text[:300],
                    "amount":    parse_amount(text),
                    "clerk_url": url or doc_url(num),
                })
            except Exception:
                pass
        return records


# ══════════════════════════════════════════════════════════════════════════════
#  FILTER, ENRICH & SCORE
# ══════════════════════════════════════════════════════════════════════════════

def filter_and_enrich(
    raw: list[dict],
    parcel: ParcelLookup,
    start: datetime,
    end: datetime,
) -> list[dict]:
    seen: set[str] = set()
    results: list[dict] = []

    for r in raw:
        try:
            code = r.get("doc_code")
            if not code or code not in TARGET_CODES:
                continue

            num = safe(r.get("doc_num"))
            if not num or num in seen:
                continue
            seen.add(num)

            filed = safe(r.get("filed"))
            if filed:
                try:
                    fd = datetime.strptime(filed, "%Y-%m-%d")
                    if not (start <= fd <= end):
                        continue
                except ValueError:
                    pass

            meta  = DOC_TYPES[code]
            owner = safe(r.get("owner"))
            pd    = parcel.lookup(owner) or {}

            rec: dict[str, Any] = {
                "doc_num":      num,
                "doc_type":     code,
                "filed":        filed,
                "cat":          meta["cat"],
                "cat_label":    meta["label"],
                "owner":        owner,
                "grantee":      safe(r.get("grantee")),
                "amount":       r.get("amount"),
                "legal":        safe(r.get("legal")),
                "prop_address": pd.get("prop_address", ""),
                "prop_city":    pd.get("prop_city", ""),
                "prop_state":   pd.get("prop_state", "TX"),
                "prop_zip":     pd.get("prop_zip", ""),
                "mail_address": pd.get("mail_address", ""),
                "mail_city":    pd.get("mail_city", ""),
                "mail_state":   pd.get("mail_state", "TX"),
                "mail_zip":     pd.get("mail_zip", ""),
                "clerk_url":    safe(r.get("clerk_url")),
                "flags":        [],
                "score":        0,
            }

            score, flags = score_record(rec)
            rec["score"] = score
            rec["flags"] = flags
            results.append(rec)

        except Exception as e:
            log.debug("Enrich error: %s", e)

    results.sort(key=lambda x: x["score"], reverse=True)
    log.info("Enriched: %d valid records from %d raw.", len(results), len(raw))
    return results


# ══════════════════════════════════════════════════════════════════════════════
#  OUTPUT WRITERS
# ══════════════════════════════════════════════════════════════════════════════

def write_json(records: list[dict], start: datetime, end: datetime):
    payload = {
        "fetched_at":   datetime.now(timezone.utc).isoformat(),
        "source":       "Austin County Clerk – tccsearch.org",
        "county":       "Austin County, TX",
        "date_range":   {"from": start.strftime("%Y-%m-%d"), "to": end.strftime("%Y-%m-%d")},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }
    body = json.dumps(payload, indent=2, default=str)
    for dest in (DASHBOARD / "records.json", DATA_DIR / "records.json"):
        dest.write_text(body, encoding="utf-8")
        log.info("Wrote %s  (%d records)", dest, len(records))


def write_ghl_csv(records: list[dict]):
    out = DATA_DIR / "ghl_export.csv"
    FIELDS = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    def split_name(full: str) -> tuple[str, str]:
        full = full.strip()
        if not full:
            return "", ""
        if "," in full:
            last, first = full.split(",", 1)
            return first.strip().title(), last.strip().title()
        parts = full.split()
        if len(parts) == 1:
            return parts[0].title(), ""
        return " ".join(parts[:-1]).title(), parts[-1].title()

    with open(out, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS)
        w.writeheader()
        for r in records:
            first, last = split_name(r.get("owner", ""))
            w.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        r.get("mail_address", ""),
                "Mailing City":           r.get("mail_city", ""),
                "Mailing State":          r.get("mail_state", ""),
                "Mailing Zip":            r.get("mail_zip", ""),
                "Property Address":       r.get("prop_address", ""),
                "Property City":          r.get("prop_city", ""),
                "Property State":         r.get("prop_state", ""),
                "Property Zip":           r.get("prop_zip", ""),
                "Lead Type":              r.get("cat_label", ""),
                "Document Type":          r.get("doc_type", ""),
                "Date Filed":             r.get("filed", ""),
                "Document Number":        r.get("doc_num", ""),
                "Amount/Debt Owed":       "" if r.get("amount") is None else r["amount"],
                "Seller Score":           r.get("score", 0),
                "Motivated Seller Flags": "; ".join(r.get("flags", [])),
                "Source":                 "Austin County Clerk – tccsearch.org",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info("GHL CSV: %s  (%d rows)", out, len(records))


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

async def main():
    log.info("━" * 60)
    log.info("  Austin County TX — Motivated Seller Lead Scraper")
    log.info("━" * 60)
    log.info("Lookback : %d days", LOOKBACK_DAYS)
    log.info("Headless : %s", HEADLESS)

    end   = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=LOOKBACK_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    log.info("Range    : %s → %s", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    log.info("━" * 60)
    log.info("Step 1/4 — Loading parcel data")
    parcel = ParcelLookup()
    parcel.load()

    log.info("━" * 60)
    log.info("Step 2/4 — Scraping clerk portal")
    scraper = ClerkScraper(start, end)
    raw     = await scraper.run()

    log.info("━" * 60)
    log.info("Step 3/4 — Filtering & enriching")
    records = filter_and_enrich(raw, parcel, start, end)

    log.info("━" * 60)
    log.info("Step 4/4 — Writing outputs")
    write_json(records, start, end)
    write_ghl_csv(records)

    log.info("━" * 60)
    log.info("DONE — %d motivated seller leads saved.", len(records))
    log.info("━" * 60)


if __name__ == "__main__":
    asyncio.run(main())
