"""
Travis County, TX (Austin) — Motivated Seller Lead Scraper
===========================================================
Clerk portal : https://www.tccsearch.org/  (requests + BeautifulSoup, HTTP POST)
Parcel data  : Travis Central Appraisal District bulk export

Why requests instead of Playwright?
  tccsearch.org has a 60-second session timeout. By the time Playwright
  launches a browser, navigates, and tries to fill the form the session
  has already expired. Direct HTTP POST is ~10x faster and bypasses the
  timeout entirely.

Strategy:
  1. GET /RealEstate/SearchEntry.aspx  → grab __VIEWSTATE + session cookie
  2. POST the search form with date range + instrument type
  3. Parse the GridView HTML results table
  4. Follow "Next Page" __doPostBack pagination
  5. Repeat for each target instrument type keyword
"""

from __future__ import annotations

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
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
CLERK_BASE    = "https://www.tccsearch.org"
SEARCH_URL    = "https://www.tccsearch.org/RealEstate/SearchEntry.aspx"
RESULTS_URL   = "https://www.tccsearch.org/RealEstate/SearchResults.aspx"
REQUEST_DELAY = 1.5   # seconds between requests — be polite
MAX_PAGES     = 50    # per instrument type
RETRY_MAX     = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
}

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
    "TRUSTEE DEED": "NOFC",
    "TAX DEED": "TAXDEED", "CONSTABLE TAX DEED": "TAXDEED",
    "SHERIFF'S DEED": "TAXDEED", "SHERIFF DEED": "TAXDEED",
    "ABSTRACT OF JUDGMENT": "JUD", "ABSTRACT OF JUDGEMENT": "JUD",
    "JUDGMENT": "JUD", "JUDGEMENT": "JUD",
    "FOREIGN JUDGMENT": "JUD", "FOREIGN JUDGEMENT": "JUD",
    "CERTIFIED COPY OF JUDGMENT": "CCJ", "CERTIFIED JUDGMENT": "CCJ",
    "DOMESTIC JUDGMENT": "DRJUD", "DOMESTIC RELATIONS ORDER": "DRJUD",
    "CORPORATE TAX LIEN": "LNCORPTX", "CORP TAX LIEN": "LNCORPTX",
    "STATE TAX LIEN": "LNCORPTX", "TWC LIEN": "LNCORPTX",
    "TEXAS WORKFORCE": "LNCORPTX",
    "IRS LIEN": "LNIRS", "FEDERAL TAX LIEN": "LNIRS",
    "FED TAX LIEN": "LNIRS", "NOTICE OF FEDERAL TAX LIEN": "LNIRS",
    "FEDERAL LIEN": "LNFED",
    "LIEN": "LN",
    "MECHANIC LIEN": "LNMECH", "MECHANIC'S LIEN": "LNMECH",
    "MATERIALMAN LIEN": "LNMECH", "MATERIALMAN'S LIEN": "LNMECH",
    "HOA LIEN": "LNHOA", "HOMEOWNER ASSOCIATION LIEN": "LNHOA",
    "MEDICAID LIEN": "MEDLN", "MEDICAL ASSISTANCE LIEN": "MEDLN",
    "PROBATE": "PRO", "LETTERS TESTAMENTARY": "PRO",
    "LETTERS OF ADMINISTRATION": "PRO", "MUNIMENT OF TITLE": "PRO",
    "AFFIDAVIT OF HEIRSHIP": "PRO",
    "NOTICE OF COMMENCEMENT": "NOC",
    "RELEASE OF LIS PENDENS": "RELLP", "RELEASE LIS PENDENS": "RELLP",
}

# Instrument type keywords to search — maps search term → our code
# We search each one individually to maximise hits
SEARCH_TERMS: list[tuple[str, str]] = [
    ("LIS PENDENS",            "LP"),
    ("NOTICE OF FORECLOSURE",  "NOFC"),
    ("SUBSTITUTE TRUSTEE",     "NOFC"),
    ("TAX DEED",               "TAXDEED"),
    ("ABSTRACT OF JUDGMENT",   "JUD"),
    ("JUDGMENT",               "JUD"),
    ("FEDERAL TAX LIEN",       "LNIRS"),
    ("IRS LIEN",               "LNIRS"),
    ("STATE TAX LIEN",         "LNCORPTX"),
    ("MECHANIC",               "LNMECH"),
    ("HOA LIEN",               "LNHOA"),
    ("PROBATE",                "PRO"),
    ("LETTERS TESTAMENTARY",   "PRO"),
    ("NOTICE OF COMMENCEMENT", "NOC"),
    ("RELEASE LIS PENDENS",    "RELLP"),
    ("MEDICAID LIEN",          "MEDLN"),
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
                "%Y%m%d", "%m/%d/%y", "%b %d %Y", "%B %d, %Y"):
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

def retry_get(session: requests.Session, url: str, **kwargs) -> requests.Response | None:
    for i in range(RETRY_MAX):
        try:
            r = session.get(url, headers=HEADERS, timeout=30, **kwargs)
            if r.ok:
                return r
            log.warning("GET %s → HTTP %s", url, r.status_code)
        except Exception as e:
            log.warning("GET attempt %d failed: %s", i + 1, e)
        time.sleep(2 * (i + 1))
    return None

def retry_post(session: requests.Session, url: str, data: dict, **kwargs) -> requests.Response | None:
    for i in range(RETRY_MAX):
        try:
            r = session.post(url, data=data, headers={**HEADERS, "Referer": url}, timeout=30, **kwargs)
            if r.ok:
                return r
            log.warning("POST %s → HTTP %s", url, r.status_code)
        except Exception as e:
            log.warning("POST attempt %d failed: %s", i + 1, e)
        time.sleep(2 * (i + 1))
    return None

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
    if "New this week"    in flags: score += 5
    if rec.get("prop_address"):     score += 5
    return min(score, 100), flags

# ══════════════════════════════════════════════════════════════════════════════
#  PARCEL LOOKUP  (Travis Central Appraisal District)
# ══════════════════════════════════════════════════════════════════════════════

class ParcelLookup:
    """
    Travis CAD bulk export sources:
    1. https://traviscad.org/publicinformation  (official bulk data page)
    2. PTAD Texas Comptroller fallback
    """

    TCAD_URLS = [
        "https://traviscad.org/publicinformation",
        "https://traviscad.org/downloads",
        "https://traviscad.org/public-information",
        "https://traviscad.org",
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
        return self._try_tcad() or self._try_ptad()

    def _try_tcad(self) -> Path | None:
        cache = CACHE_DIR / "tcad_parcels.dbf"
        if cache.exists() and (time.time() - cache.stat().st_mtime) < 86_400:
            log.info("Using cached TCAD DBF.")
            return cache
        for url in self.TCAD_URLS:
            try:
                log.info("Checking TCAD: %s", url)
                resp = requests.get(url, timeout=20, headers=HEADERS)
                if not resp.ok:
                    continue
                soup = BeautifulSoup(resp.text, "lxml")
                for a in soup.find_all("a", href=True):
                    href: str = a["href"]
                    if any(kw in href.lower() for kw in
                           (".dbf", ".zip", "parcel", "apprais", "export", "download",
                            "bulk", "owner", "account")):
                        full = href if href.startswith("http") else urljoin(url, href)
                        dl   = self._download(full, CACHE_DIR / "tcad_raw")
                        if dl:
                            dbf = self._unpack(dl)
                            if dbf:
                                return dbf
            except Exception as e:
                log.debug("TCAD %s: %s", url, e)
        return None

    def _try_ptad(self) -> Path | None:
        cache = CACHE_DIR / "ptad_parcels.dbf"
        if cache.exists() and (time.time() - cache.stat().st_mtime) < 86_400:
            log.info("Using cached PTAD DBF.")
            return cache
        year = datetime.now().year
        for y in (year, year - 1):
            for pat in [
                "https://comptroller.texas.gov/taxes/property-tax/county-directory/data/travis-county-{y}.zip",
                "https://comptroller.texas.gov/taxes/property-tax/county-directory/data/travis-{y}.zip",
            ]:
                dl = self._download(pat.format(y=y), CACHE_DIR / f"ptad_travis_{y}.zip")
                if dl:
                    dbf = self._unpack(dl)
                    if dbf:
                        return dbf
        log.warning("All parcel sources exhausted.")
        return None

    def _download(self, url: str, dest: Path) -> Path | None:
        try:
            log.info("Downloading: %s", url)
            with requests.get(url, stream=True, timeout=90, headers=HEADERS) as r:
                if r.status_code != 200:
                    return None
                dest.parent.mkdir(parents=True, exist_ok=True)
                with open(dest, "wb") as f:
                    for chunk in r.iter_content(65_536):
                        f.write(chunk)
            size_kb = dest.stat().st_size / 1024
            return dest if size_kb > 2 else None
        except Exception as e:
            log.debug("Download %s: %s", url, e)
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
#  CLERK SCRAPER  — direct HTTP POST, no browser needed
# ══════════════════════════════════════════════════════════════════════════════

class ClerkScraper:
    """
    Scrapes tccsearch.org via direct HTTP POST requests.

    Flow per instrument type:
      1. GET SearchEntry.aspx  → extract __VIEWSTATE, __EVENTVALIDATION, session cookie
      2. POST SearchEntry.aspx with form data (dates + instrument type)
      3. If redirected to SearchResults.aspx, parse that page
      4. Follow __doPostBack pagination until no more pages
    """

    def __init__(self, start: datetime, end: datetime):
        self.start  = start
        self.end    = end
        self._seen: set[str] = set()
        self.raw:   list[dict] = []

    def run(self) -> list[dict]:
        log.info("Starting HTTP POST clerk scrape …")

        for term, hint_code in SEARCH_TERMS:
            try:
                recs = self._scrape_term(term, hint_code)
                if recs:
                    log.info("  %-30s → %d records", term, len(recs))
                self.raw.extend(recs)
                time.sleep(REQUEST_DELAY)
            except Exception as e:
                log.warning("Term '%s' failed: %s", term, e)
                continue

        log.info("Clerk scrape done: %d raw records.", len(self.raw))
        return self.raw

    def _scrape_term(self, instrument_term: str, hint_code: str) -> list[dict]:
        """Scrape one instrument type keyword."""
        session = requests.Session()
        records: list[dict] = []

        # ── Step 1: GET the search page to get ViewState + session ────────────
        resp = retry_get(session, SEARCH_URL)
        if not resp:
            log.warning("Could not reach search page for term '%s'", instrument_term)
            return []

        soup       = BeautifulSoup(resp.text, "lxml")
        form_state = self._extract_form_state(soup)

        if not form_state.get("__VIEWSTATE"):
            log.warning("No ViewState found for term '%s' — skipping.", instrument_term)
            return []

        # ── Step 2: Build and POST the search form ────────────────────────────
        start_str = self.start.strftime("%m/%d/%Y")
        end_str   = self.end.strftime("%m/%d/%Y")

        # Find the actual input field names from the form
        # tccsearch.org uses ContentPlaceHolder1 naming
        form_data = {
            **form_state,
            "__EVENTTARGET":   "",
            "__EVENTARGUMENT": "",
        }

        # Try to find date and instrument fields dynamically
        for inp in soup.find_all("input", {"type": ["text", "hidden"]}):
            name = inp.get("name", "")
            name_lower = name.lower()
            val  = inp.get("value", "")

            # Keep hidden fields (ViewState etc) as-is
            if inp.get("type") == "hidden":
                form_data[name] = val
                continue

            # Date from
            if any(k in name_lower for k in ("fromdate", "datebegin", "startdate",
                                              "datefrom", "fromdt", "begdate")):
                form_data[name] = start_str
            # Date to
            elif any(k in name_lower for k in ("todate", "dateend", "enddate",
                                                "dateto", "todt", "enddt")):
                form_data[name] = end_str
            # Instrument type
            elif any(k in name_lower for k in ("instrument", "instrtype", "doctype",
                                                "recordtype", "instrcode")):
                form_data[name] = instrument_term
            # Grantor / Grantee — leave blank for date+type search
            elif any(k in name_lower for k in ("grantor", "grantee", "name")):
                form_data[name] = ""

        # Also look for select dropdowns
        for sel in soup.find_all("select"):
            name = sel.get("name", "")
            name_lower = name.lower()
            if any(k in name_lower for k in ("instrument", "instrtype", "doctype")):
                # Try to find the best matching option
                best_opt = ""
                for opt in sel.find_all("option"):
                    opt_text = opt.get_text(strip=True).upper()
                    if instrument_term in opt_text:
                        best_opt = opt.get("value", opt.get_text(strip=True))
                        break
                form_data[name] = best_opt
            elif any(k in name_lower for k in ("county", "state")):
                # Keep default selected value
                selected = sel.find("option", selected=True)
                if selected:
                    form_data[name] = selected.get("value", "")

        # Find and click the search button
        search_btn = soup.find("input", {"type": "submit"}) or \
                     soup.find("input", {"type": "button", "value": re.compile(r"search", re.I)}) or \
                     soup.find("input", {"id": re.compile(r"search", re.I)})

        if search_btn:
            btn_name = search_btn.get("name", "")
            btn_val  = search_btn.get("value", "Search")
            if btn_name:
                form_data[btn_name] = btn_val

        time.sleep(REQUEST_DELAY)

        # ── Step 3: POST the form ─────────────────────────────────────────────
        post_resp = retry_post(session, SEARCH_URL, form_data)
        if not post_resp:
            return []

        # ── Step 4: Parse results + pagination ────────────────────────────────
        result_soup = BeautifulSoup(post_resp.text, "lxml")

        # Check if we got redirected to results page or results are inline
        page_num = 1
        current_soup = result_soup

        while page_num <= MAX_PAGES:
            batch = self._parse_results(current_soup, hint_code)
            records.extend(batch)

            if not batch and page_num == 1:
                break

            # Look for next page via __doPostBack
            next_data = self._find_next_page(current_soup, form_state)
            if not next_data:
                break

            time.sleep(REQUEST_DELAY)
            next_resp = retry_post(session, post_resp.url or SEARCH_URL, next_data)
            if not next_resp:
                break

            current_soup = BeautifulSoup(next_resp.text, "lxml")
            page_num += 1

        return records

    def _extract_form_state(self, soup: BeautifulSoup) -> dict:
        """Extract all hidden ASP.NET form fields."""
        state: dict[str, str] = {}
        for hidden in soup.find_all("input", {"type": "hidden"}):
            name = hidden.get("name", "")
            val  = hidden.get("value", "")
            if name:
                state[name] = val
        return state

    def _find_next_page(self, soup: BeautifulSoup, base_state: dict) -> dict | None:
        """Find __doPostBack call for next page link."""
        # Look for "Next" link or page number link
        next_patterns = [
            re.compile(r"next", re.I),
            re.compile(r">\s*$"),
            re.compile(r"Page\$Next"),
        ]
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True)
            if any(p.search(text) or p.search(href) for p in next_patterns):
                # Extract __doPostBack arguments
                m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
                if m:
                    state = dict(base_state)
                    state["__EVENTTARGET"]   = m.group(1)
                    state["__EVENTARGUMENT"] = m.group(2)
                    # Refresh ViewState from current page
                    new_vs = soup.find("input", {"id": "__VIEWSTATE"})
                    if new_vs:
                        state["__VIEWSTATE"] = new_vs.get("value", "")
                    new_ev = soup.find("input", {"id": "__EVENTVALIDATION"})
                    if new_ev:
                        state["__EVENTVALIDATION"] = new_ev.get("value", "")
                    return state

        # Also check for table pagination controls
        for td in soup.find_all("td"):
            for a in td.find_all("a"):
                href = a.get("href", "")
                if "Page$" in href or "__doPostBack" in href:
                    m = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
                    if m:
                        state = dict(base_state)
                        state["__EVENTTARGET"]   = m.group(1)
                        state["__EVENTARGUMENT"] = m.group(2)
                        new_vs = soup.find("input", {"id": "__VIEWSTATE"})
                        if new_vs:
                            state["__VIEWSTATE"] = new_vs.get("value", "")
                        return state

        return None

    def _parse_results(self, soup: BeautifulSoup, hint_code: str) -> list[dict]:
        """Parse results table from a response page."""
        body = soup.get_text(" ", strip=True).lower()
        if any(p in body for p in ("no records found", "no results", "0 records")):
            return []

        # Find best table
        best_tbl, best_n = None, 0
        for tbl in soup.find_all("table"):
            rows = tbl.find_all("tr")
            td_rows = [r for r in rows if r.find("td")]
            if len(td_rows) > best_n:
                best_tbl, best_n = tbl, len(td_rows)

        if not best_tbl or best_n < 1:
            return []

        rows = best_tbl.find_all("tr")
        if not rows:
            return []

        # Parse header
        hdr = [c.get_text(strip=True).upper() for c in rows[0].find_all(["th", "td"])]

        def cidx(*names: str) -> int | None:
            for name in names:
                for i, h in enumerate(hdr):
                    if name in h:
                        return i
            return None

        i_num  = cidx("INSTRUMENT NO", "INSTRUMENT NUMBER", "DOC NO", "DOC NUMBER",
                       "INSTR NO", "INSTR NUM", "NUMBER", "INSTRUMENT")
        i_type = cidx("INSTRUMENT TYPE", "INSTR TYPE", "DOC TYPE", "TYPE", "RECORD TYPE")
        i_date = cidx("DATE FILED", "FILED DATE", "FILE DATE", "RECORD DATE", "DATE")
        i_gror = cidx("GRANTOR", "GRANTORS", "OWNER", "FROM", "SELLER", "PARTY 1")
        i_gree = cidx("GRANTEE", "GRANTEES", "BUYER", "TO", "PARTY 2")
        i_legal= cidx("LEGAL", "LEGAL DESC", "DESCRIPTION", "REMARKS", "PROPERTY")
        i_amt  = cidx("AMOUNT", "CONSIDERATION", "DEBT", "VALUE", "TOTAL")

        records: list[dict] = []
        for row in rows[1:]:
            cells = row.find_all(["td", "th"])
            if not cells or len(cells) < 2:
                continue
            try:
                def ct(idx: int | None) -> str:
                    return "" if (idx is None or idx >= len(cells)) \
                               else cells[idx].get_text(strip=True)

                # grab doc link
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

                # Try extracting doc num from link if cell was empty
                if not raw_num and clerk_url:
                    m = re.search(r"id=([^&\s]+)", clerk_url, re.I)
                    raw_num = m.group(1) if m else ""

                # Skip empty or already-seen
                if not raw_num or raw_num in self._seen:
                    continue
                self._seen.add(raw_num)

                # Use hint_code if instrument type cell is empty
                doc_code = map_instrument(raw_type) or hint_code

                records.append({
                    "_raw_type": raw_type,
                    "doc_code":  doc_code,
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
        "source":       "Travis County Clerk – tccsearch.org",
        "county":       "Travis County, TX (Austin)",
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
        return (" ".join(parts[:-1]).title(), parts[-1].title()) if len(parts) > 1 \
               else (parts[0].title(), "")

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
                "Source":                 "Travis County Clerk – tccsearch.org",
                "Public Records URL":     r.get("clerk_url", ""),
            })
    log.info("GHL CSV: %s  (%d rows)", out, len(records))

# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    log.info("━" * 55)
    log.info("  Travis County TX (Austin) — Motivated Seller Leads")
    log.info("━" * 55)
    log.info("Lookback: %d days", LOOKBACK_DAYS)

    end   = datetime.now().replace(hour=23, minute=59, second=59, microsecond=0)
    start = (end - timedelta(days=LOOKBACK_DAYS)).replace(
        hour=0, minute=0, second=0, microsecond=0)
    log.info("Range   : %s → %s", start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d"))

    log.info("Step 1/4 — Parcel data")
    parcel = ParcelLookup()
    parcel.load()

    log.info("Step 2/4 — Clerk portal")
    raw = ClerkScraper(start, end).run()

    log.info("Step 3/4 — Filter & enrich")
    records = filter_and_enrich(raw, parcel, start, end)

    log.info("Step 4/4 — Write outputs")
    write_json(records, start, end)
    write_ghl_csv(records)

    log.info("━" * 55)
    log.info("DONE — %d leads saved.", len(records))
    log.info("━" * 55)

if __name__ == "__main__":
    main()
