"""
Austin County, TX — Motivated Seller Lead Scraper
==================================================
Clerk portal : https://www.tccsearch.org/  (Playwright async)
Parcel data  : Austin County Appraisal District + PTAD fallback (requests + dbfread)
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
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

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
LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "7"))
HEADLESS        = os.getenv("HEADLESS", "true").lower() != "false"
CLERK_BASE      = "https://www.tccsearch.org"
# Try the homepage first — let it redirect to search
CLERK_HOME      = "https://www.tccsearch.org"
CLERK_SEARCH    = "https://www.tccsearch.org/RealEstate/SearchEntry.aspx"
PAGE_TIMEOUT    = 120_000   # 2 minutes — site can be slow
NAV_TIMEOUT     = 90_000

# ── document types ─────────────────────────────────────────────────────────────
DOC_TYPES: dict[str, dict[str, Any]] = {
    "LP":       {"label": "Lis Pendens",            "cat": "lis_pendens", "flags": ["Lis pendens", "Pre-foreclosure"]},
    "NOFC":     {"label": "Notice of Foreclosure",  "cat": "foreclosure", "flags": ["Pre-foreclosure"]},
    "TAXDEED":  {"label": "Tax Deed",               "cat": "tax_deed",    "flags": ["Tax lien"]},
    "JUD":      {"label": "Judgment",               "cat": "judgment",    "flags": ["Judgment lien"]},
    "CCJ":      {"label": "Certified Judgment",     "cat": "judgment",    "flags": ["Judgment lien"]},
    "DRJUD":    {"label": "Domestic Judgment",      "cat": "judgment",    "flags": ["Judgment lien"]},
    "LNCORPTX": {"label": "Corp Tax Lien",          "cat": "lien",        "flags": ["Tax lien"]},
    "LNIRS":    {"label": "IRS Lien",               "cat": "lien",        "flags": ["Tax lien"]},
    "LNFED":    {"label": "Federal Lien",           "cat": "lien",        "flags": ["Tax lien"]},
    "LN":       {"label": "Lien",                   "cat": "lien",        "flags": []},
    "LNMECH":   {"label": "Mechanic Lien",          "cat": "lien",        "flags": ["Mechanic lien"]},
    "LNHOA":    {"label": "HOA Lien",               "cat": "lien",        "flags": []},
    "MEDLN":    {"label": "Medicaid Lien",          "cat": "lien",        "flags": []},
    "PRO":      {"label": "Probate",                "cat": "probate",     "flags": ["Probate / estate"]},
    "NOC":      {"label": "Notice of Commencement", "cat": "notice",      "flags": []},
    "RELLP":    {"label": "Release Lis Pendens",    "cat": "release",     "flags": []},
}
TARGET_CODES = set(DOC_TYPES.keys())

INSTRUMENT_MAP: dict[str, str] = {
    "LIS PENDENS": "LP", "LP": "LP", "LIS PENDEN": "LP",
    "NOTICE OF FORECLOSURE": "NOFC", "FORECLOSURE": "NOFC",
    "NOTICE OF TRUSTEE SALE": "NOFC", "SUBSTITUTE TRUSTEE": "NOFC",
    "SUBSTITUTE TRUSTEE'S DEED": "NOFC", "TRUSTEE'S DEED": "NOFC",
    "TRUSTEE DEED": "NOFC", "DEED OF TRUST - FORECLOSURE": "NOFC",
    "TAX DEED": "TAXDEED", "CONSTABLE TAX DEED": "TAXDEED",
    "SHERIFF DEED": "TAXDEED", "SHERIFF'S DEED": "TAXDEED",
    "ABSTRACT OF JUDGMENT": "JUD", "ABSTRACT OF JUDGEMENT": "JUD",
    "JUDGMENT": "JUD", "JUDGEMENT": "JUD",
    "FOREIGN JUDGMENT": "JUD", "FOREIGN JUDGEMENT": "JUD",
    "CERTIFIED COPY OF JUDGMENT": "CCJ", "CERTIFIED JUDGMENT": "CCJ",
    "DOMESTIC JUDGMENT": "DRJUD", "DOMESTIC RELATIONS ORDER": "DRJUD",
    "CORPORATE TAX LIEN": "LNCORPTX", "CORP TAX LIEN": "LNCORPTX",
    "STATE TAX LIEN": "LNCORPTX", "TWC LIEN": "LNCORPTX",
    "IRS LIEN": "LNIRS", "FEDERAL TAX LIEN": "LNIRS",
    "FED TAX LIEN": "LNIRS", "NOTICE OF FEDERAL TAX LIEN": "LNIRS",
    "FEDERAL LIEN": "LNFED",
    "LIEN": "LN", "MECHANIC LIEN": "LNMECH", "MECHANIC'S LIEN": "LNMECH",
    "MATERIALMAN LIEN": "LNMECH", "MATERIALMAN'S LIEN": "LNMECH",
    "HOA LIEN": "LNHOA", "HOMEOWNER ASSOCIATION LIEN": "LNHOA",
    "MEDICAID LIEN": "MEDLN", "MEDICAL ASSISTANCE LIEN": "MEDLN",
    "PROBATE": "PRO", "LETTERS TESTAMENTARY": "PRO",
    "LETTERS OF ADMINISTRATION": "PRO", "MUNIMENT OF TITLE": "PRO",
    "AFFIDAVIT OF HEIRSHIP": "PRO",
    "NOTICE OF COMMENCEMENT": "NOC",
    "RELEASE OF LIS PENDENS": "RELLP", "RELEASE LIS PENDENS": "RELLP",
}

SEARCH_TERMS = [
    "LIS PENDENS", "FORECLOSURE", "TRUSTEE", "TAX DEED",
    "JUDGMENT", "ABSTRACT", "IRS LIEN", "FEDERAL TAX LIEN",
    "MECHANIC LIEN", "HOA LIEN", "LIEN", "PROBATE",
    "LETTERS TESTAMENTARY", "NOTICE OF COMMENCEMENT", "RELEASE LIS PENDENS",
]

# ── helpers ────────────────────────────────────────────────────────────────────

def safe(v, default: str = "") -> str:
    return default if v is None else str(v).strip()

def parse_amount(text: str) -> float | None:
    cleaned = re.sub(r"[$,\s]", "", safe(text))
    m = re.search(r"\d+(?:\.\d{1,2})?", cleaned)
    return float(m.group()) if m else None

def map_instrument(raw: str) -> str | None:
    if not raw:
        return None
    upper = raw.strip().upper()
    if upper in INSTRUMENT_MAP:
        return INSTRUMENT_MAP[upper]
    for key, code in INSTRUMENT_MAP.items():
        if key in upper:
            return code
    return None

def norm_date(raw: str) -> str:
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

# ── scoring ────────────────────────────────────────────────────────────────────

def score_record(rec: dict) -> tuple[int, list[str]]:
    flags: list[str] = list(DOC_TYPES.get(rec.get("doc_type", ""), {}).get("flags", []))
    owner_up = safe(rec.get("owner", "")).upper()
    if re.search(r"\bLLC\b|\bINC\b|\bCORP\b|\bL\.P\.\b|\bLTD\b|\bTRUST\b|\bFUND\b|\bINVEST", owner_up):
        flags.append("LLC / corp owner")
    try:
        if (datetime.now() - datetime.strptime(rec.get("filed", ""), "%Y-%m-%d")).days <= 7:
            flags.append("New this week")
    except Exception:
        pass
    seen: set[str] = set()
    flags = [f for f in flags if not (f in seen or seen.add(f))]  # type: ignore[func-returns-value]

    score = 30
    DISTRESS = {"Lis pendens", "Pre-foreclosure", "Judgment lien",
                "Tax lien", "Mechanic lien", "Probate / estate", "LLC / corp owner"}
    score += sum(10 for f in flags if f in DISTRESS)
    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20
    amt = rec.get("amount")
    if amt:
        if   amt > 100_000: score += 15
        elif amt >  50_000: score += 10
    if "New this week" in flags: score += 5
    if rec.get("prop_address"):  score += 5
    return min(score, 100), flags

# ══════════════════════════════════════════════════════════════════════════════
#  PARCEL LOOKUP
# ══════════════════════════════════════════════════════════════════════════════

class ParcelLookup:
    ACAD_URLS = [
        "https://austincad.org/data-downloads",
        "https://austincad.org/downloads",
        "https://austincad.org/publicdata",
        "https://austincad.org/GIS",
        "https://austincad.org",
    ]

    def __init__(self):
        self._index: dict[str, dict] = {}

    def load(self):
        if not HAS_DBF:
            log.warning("dbfread not installed — address enrichment skipped.")
            return
        dbf = self._find_dbf()
        if dbf:
            self._build_index(dbf)
        else:
            log.warning("No parcel DBF found — records will have no addresses.")

    def lookup(self, name: str) -> dict | None:
        if not name:
            return None
        for v in name_variants(name):
            hit = self._index.get(v)
            if hit:
                return hit
        token = name.strip().upper().split()[0]
        if len(token) > 3:
            for key, val in self._index.items():
                if key.startswith(token):
                    return val
        return None

    def _find_dbf(self) -> Path | None:
        return self._try_acad() or self._try_ptad()

    def _try_acad(self) -> Path | None:
        cache = CACHE_DIR / "acad_parcels.dbf"
        if cache.exists() and (time.time() - cache.stat().st_mtime) < 86_400:
            log.info("Using cached ACAD DBF.")
            return cache
        headers = {"User-Agent": "Mozilla/5.0"}
        for url in self.ACAD_URLS:
            try:
                resp = requests.get(url, timeout=20, headers=headers)
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

    def _try_ptad(self) -> Path | None:
        cache = CACHE_DIR / "ptad_parcels.dbf"
        if cache.exists() and (time.time() - cache.stat().st_mtime) < 86_400:
            log.info("Using cached PTAD DBF.")
            return cache
        year = datetime.now().year
        for y in (year, year - 1):
            for pat in [
                "https://comptroller.texas.gov/taxes/property-tax/county-directory/data/austin-county-{y}.zip",
                "https://comptroller.texas.gov/taxes/property-tax/county-directory/data/austin-{y}.zip",
            ]:
                dl = self._download(pat.format(y=y), CACHE_DIR / f"ptad_{y}.zip")
                if dl:
                    dbf = self._unpack(dl)
                    if dbf:
                        return dbf
        log.warning("All parcel data sources exhausted.")
        return None

    def _download(self, url: str, dest: Path) -> Path | None:
        try:
            log.info("Downloading: %s", url)
            with requests.get(url, stream=True, timeout=60,
                              headers={"User-Agent": "Mozilla/5.0"}) as r:
                if r.status_code != 200:
                    return None
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(65_536):
                        f.write(chunk)
            size_kb = dest.stat().st_size / 1024
            return dest if size_kb > 2 else None
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
                        return None
                    dbf_names.sort(key=lambda n: zf.getinfo(n).file_size, reverse=True)
                    out = CACHE_DIR / Path(dbf_names[0]).name
                    with zf.open(dbf_names[0]) as src, open(out, "wb") as dst:
                        dst.write(src.read())
                    return out
            except zipfile.BadZipFile:
                return None
        return None

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

            c_own    = col("OWN1",    "OWNER",       "OWNER_NAME",  "OWNERNAME",  "NAME")
            c_site   = col("SITEADDR","SITE_ADDR",   "SITUS_ADDR",  "PROP_ADDR",  "SITE_ADDRESS")
            c_scity  = col("SITE_CITY","SITECITY",   "SITUS_CITY",  "PROP_CITY")
            c_szip   = col("SITE_ZIP", "SITEZIP",    "SITUS_ZIP",   "PROP_ZIP")
            c_mail1  = col("MAILADR1", "ADDR_1",     "MAIL_ADDR1",  "MAIL1",      "MAIL_ADDRESS")
            c_mcity  = col("MAILCITY", "CITY",       "MAIL_CITY",   "MCITY")
            c_mstate = col("STATE",    "MAIL_STATE", "MAILSTATE",   "MSTATE")
            c_mzip   = col("MAILZIP",  "ZIP",        "MAIL_ZIP",    "MZIP")

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
        log.info("Parcel index: %d owners, %d keys.", count, len(self._index))

# ══════════════════════════════════════════════════════════════════════════════
#  CLERK SCRAPER
# ══════════════════════════════════════════════════════════════════════════════

class ClerkScraper:
    def __init__(self, start: datetime, end: datetime):
        self.start = start
        self.end   = end
        self._seen: set[str] = set()
        self.raw:   list[dict] = []

    async def run(self) -> list[dict]:
        if not HAS_PLAYWRIGHT:
            log.error("Playwright not installed — skipping clerk scrape.")
            return []

        async with async_playwright() as pw:
            browser = await pw.chromium.launch(
                headless=HEADLESS,
                args=[
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-gpu",
                    "--disable-blink-features=AutomationControlled",
                ],
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 900},
            )
            page = await ctx.new_page()
            page.set_default_timeout(PAGE_TIMEOUT)

            try:
                # ── load the site homepage first to establish session ──────────
                log.info("Connecting to clerk portal …")
                try:
                    await page.goto(
                        CLERK_HOME,
                        wait_until="domcontentloaded",
                        timeout=NAV_TIMEOUT,
                    )
                    await page.wait_for_timeout(3_000)
                    log.info("Homepage loaded: %s", page.url)
                except Exception as e:
                    log.warning("Homepage load issue (continuing): %s", e)

                # ── navigate to search page ───────────────────────────────────
                try:
                    await page.goto(
                        CLERK_SEARCH,
                        wait_until="domcontentloaded",
                        timeout=NAV_TIMEOUT,
                    )
                    await page.wait_for_timeout(3_000)
                    log.info("Search page loaded: %s", page.url)
                except Exception as e:
                    log.warning("Search page load issue (continuing): %s", e)

                # ── broad date search ─────────────────────────────────────────
                await self._fill_dates(page)
                if await self._click_search(page):
                    await page.wait_for_load_state("domcontentloaded", timeout=NAV_TIMEOUT)
                    await page.wait_for_timeout(2_000)
                    recs = await self._harvest_pages(page)
                    self.raw.extend(recs)
                    log.info("Broad date search: %d records.", len(recs))

                # ── per-term searches ─────────────────────────────────────────
                log.info("Running %d per-term searches …", len(SEARCH_TERMS))
                for term in SEARCH_TERMS:
                    try:
                        await page.goto(
                            CLERK_SEARCH,
                            wait_until="domcontentloaded",
                            timeout=NAV_TIMEOUT,
                        )
                        await page.wait_for_timeout(1_500)
                        await self._fill_instrument(page, term)
                        await self._fill_dates(page)
                        if not await self._click_search(page):
                            continue
                        await page.wait_for_load_state("domcontentloaded", timeout=NAV_TIMEOUT)
                        await page.wait_for_timeout(1_500)
                        recs = await self._harvest_pages(page)
                        if recs:
                            log.info("  '%s' → %d records.", term, len(recs))
                        self.raw.extend(recs)
                    except Exception as e:
                        log.warning("Term '%s' failed: %s", term, e)
                        continue

            except Exception as e:
                log.error("Clerk scraper fatal: %s", e, exc_info=True)
            finally:
                await browser.close()

        log.info("Clerk scrape done: %d raw records.", len(self.raw))
        return self.raw

    # ── form helpers ───────────────────────────────────────────────────────────

    async def _fill_dates(self, page):
        s = self.start.strftime("%m/%d/%Y")
        e = self.end.strftime("%m/%d/%Y")

        # Try every known ASP.NET field name pattern
        pairs = [
            ("txtFromDate", s), ("txtToDate", e),
            ("txtDateFrom", s), ("txtDateTo", e),
            ("txtStartDate", s), ("txtEndDate", e),
            ("ctl00$ContentPlaceHolder1$txtFromDate", s),
            ("ctl00$ContentPlaceHolder1$txtToDate", e),
            ("ctl00$ContentPlaceHolder1$txtDateFrom", s),
            ("ctl00$ContentPlaceHolder1$txtDateTo", e),
            ("ctl00$ContentPlaceHolder1$txtStartDate", s),
            ("ctl00$ContentPlaceHolder1$txtEndDate", e),
        ]
        filled = 0
        for name, val in pairs:
            try:
                el = page.locator(f"input[name='{name}'], input[id='{name}']").first
                if await el.count() > 0:
                    await el.triple_click()
                    await el.fill(val)
                    filled += 1
            except Exception:
                pass

        # Generic fallback — any visible text input with "date" in its id/name
        if filled < 2:
            try:
                inputs = page.locator(
                    "input[type='text'][id*='ate' i], input[type='text'][name*='ate' i]"
                )
                n = await inputs.count()
                if n >= 2:
                    await inputs.nth(0).fill(s)
                    await inputs.nth(1).fill(e)
            except Exception:
                pass

    async def _fill_instrument(self, page, term: str):
        for sel in [
            "input[name*='Instrument' i]", "input[name*='DocType' i]",
            "input[name*='InstrType' i]",  "#txtInstrumentType",
            "#txtDocType",                  "input[id*='Instrument' i]",
        ]:
            try:
                el = page.locator(sel).first
                if await el.count() > 0:
                    await el.fill(term)
                    return
            except Exception:
                pass
        # dropdown fallback
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
        for sel in [
            "input[type='submit'][value*='Search' i]",
            "input[type='button'][value*='Search' i]",
            "button[type='submit']",
            "#btnSearch",
            "#ctl00_ContentPlaceHolder1_btnSearch",
            "a:has-text('Search')",
            "button:has-text('Search')",
        ]:
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
            html  = await page.content()
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
                        await page.wait_for_load_state("domcontentloaded", timeout=NAV_TIMEOUT)
                        await page.wait_for_timeout(1_000)
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
        body = soup.get_text(" ", strip=True).lower()
        if any(p in body for p in ("no records found", "no results", "0 records", "no data")):
            return []

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

        hdr = [c.get_text(strip=True).upper() for c in rows[0].find_all(["th", "td"])]

        def cidx(*names: str) -> int | None:
            for name in names:
                for i, h in enumerate(hdr):
                    if name in h:
                        return i
            return None

        i_num  = cidx("INSTRUMENT NO", "INSTRUMENT NUMBER", "DOC NO", "DOC NUMBER", "INSTR NO", "NUMBER")
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
        records: list[dict] = []
        for div in soup.select(
            ".result-item, .search-result, .record-row, "
            "[class*='result'], [class*='record'], [id*='rpt']"
        ):
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
#  FILTER & ENRICH
# ══════════════════════════════════════════════════════════════════════════════

def filter_and_enrich(
    raw: list[dict], parcel: ParcelLookup,
    start: datetime, end: datetime,
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
            rec["score"]  = score
            rec["flags"]  = flags
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
        if not full: return "", ""
        if "," in full:
            last, first = full.split(",", 1)
            return first.strip().title(), last.strip().title()
        parts = full.split()
        return (" ".join(parts[:-1]).title(), parts[-1].title()) if len(parts) > 1 else (parts[0].title(), "")

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
    log.info("━" * 55)
    log.info("  Austin County TX — Motivated Seller Lead Scraper")
    log.info("━" * 55)
    log.info("Lookback : %d days", LOOKBACK_DAYS)

    end   = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=LOOKBACK_DAYS)).replace(hour=0, minute=0, second=0, microsecond=0)
    log.info("Range    : %s → %s", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    log.info("Step 1/4 — Parcel data")
    parcel = ParcelLookup()
    parcel.load()

    log.info("Step 2/4 — Clerk portal")
    raw = await ClerkScraper(start, end).run()

    log.info("Step 3/4 — Filter & enrich")
    records = filter_and_enrich(raw, parcel, start, end)

    log.info("Step 4/4 — Write outputs")
    write_json(records, start, end)
    write_ghl_csv(records)

    log.info("━" * 55)
    log.info("DONE — %d leads saved.", len(records))
    log.info("━" * 55)

if __name__ == "__main__":
    asyncio.run(main())
