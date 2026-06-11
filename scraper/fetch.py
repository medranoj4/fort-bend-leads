"""
Fort Bend County, TX — Motivated Seller Lead Scraper
======================================================
Sources:
  1. Fort Bend County Clerk Portal  (Playwright / async)
  2. FBCAD Bulk Parcel Data          (requests + dbfread)

Outputs:
  dashboard/records.json
  data/records.json
  data/ghl_export_<date>.csv
"""

from __future__ import annotations

import asyncio
import csv
import io
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

import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, Page, BrowserContext
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False
    logging.warning("playwright not installed – clerk scraping will be skipped")

try:
    from dbfread import DBF
    HAS_DBFREAD = True
except ImportError:
    HAS_DBFREAD = False
    logging.warning("dbfread not installed – parcel lookup will be skipped")

BASE_DIR        = Path(__file__).resolve().parent.parent
DASHBOARD_DIR   = BASE_DIR / "dashboard"
DATA_DIR        = BASE_DIR / "data"
LOOKBACK_DAYS   = int(os.getenv("LOOKBACK_DAYS", "7"))
HEADLESS        = os.getenv("HEADLESS", "true").lower() != "false"

CLERK_URL       = "https://www.fortbendcountytx.gov/government/courts/court-records-research"
FBCAD_DATA_URL  = "https://www.fbcad.org/data-files/"

DOC_TYPE_MAP: dict[str, tuple[str, str]] = {
    "LP":       ("LP",      "Lis Pendens"),
    "NOFC":     ("NOFC",    "Notice of Foreclosure"),
    "TAXDEED":  ("TAXDEED", "Tax Deed"),
    "JUD":      ("JUD",     "Judgment"),
    "CCJ":      ("JUD",     "Certified Judgment"),
    "DRJUD":    ("JUD",     "Domestic Relations Judgment"),
    "LNCORPTX": ("LN",      "Corp Tax Lien"),
    "LNIRS":    ("LN",      "IRS Lien"),
    "LNFED":    ("LN",      "Federal Lien"),
    "LN":       ("LN",      "Lien"),
    "LNMECH":   ("LN",      "Mechanic Lien"),
    "LNHOA":    ("LN",      "HOA Lien"),
    "MEDLN":    ("LN",      "Medicaid Lien"),
    "PRO":      ("PRO",     "Probate Document"),
    "NOC":      ("NOC",     "Notice of Commencement"),
    "RELLP":    ("RELLP",   "Release Lis Pendens"),
}
TARGET_CODES = list(DOC_TYPE_MAP.keys())

RETRY_ATTEMPTS = 3
RETRY_DELAY    = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("fb_scraper")


def retry(fn, *args, attempts: int = RETRY_ATTEMPTS, delay: float = RETRY_DELAY, **kwargs):
    for i in range(attempts):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            log.warning("Attempt %d/%d failed: %s", i + 1, attempts, exc)
            if i < attempts - 1:
                time.sleep(delay * (i + 1))
    return None


def safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        cleaned = re.sub(r"[^\d.]", "", str(val))
        return float(cleaned) if cleaned else None
    except Exception:
        return None


def parse_name(full_name: str) -> tuple[str, str]:
    full_name = full_name.strip()
    if not full_name:
        return ("", "")
    if "," in full_name:
        parts = full_name.split(",", 1)
        last  = parts[0].strip().title()
        first = parts[1].strip().split()[0].title() if parts[1].strip() else ""
        return (first, last)
    tokens = full_name.split()
    if len(tokens) == 1:
        return ("", tokens[0].title())
    return (tokens[0].title(), " ".join(tokens[1:]).title())


class ParcelLookup:
    def __init__(self):
        self._index: dict[str, dict] = {}

    def _download_dbf(self) -> bytes | None:
        log.info("Fetching FBCAD data-files page ...")
        resp = requests.get(FBCAD_DATA_URL, timeout=30)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        candidates = []
        for a in soup.find_all("a", href=True):
            href = a["href"]
            low  = href.lower()
            if re.search(r"\.(zip|dbf)$", low) and re.search(r"(parcel|real|prop)", low):
                candidates.append(href)

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "__doPostBack" in href and "parcel" in a.get_text(strip=True).lower():
                candidates.append(href)

        if not candidates:
            for a in soup.find_all("a", href=True):
                if a["href"].lower().endswith(".zip"):
                    candidates.append(a["href"])

        if not candidates:
            log.error("No parcel download link found on FBCAD page.")
            return None

        href = candidates[0]
        log.info("Parcel download candidate: %s", href)

        if "__doPostBack" in href:
            match = re.search(r"__doPostBack\('([^']+)','([^']*)'\)", href)
            if match:
                data = {
                    "__EVENTTARGET":   match.group(1),
                    "__EVENTARGUMENT": match.group(2),
                }
                vs  = soup.find("input", {"name": "__VIEWSTATE"})
                evv = soup.find("input", {"name": "__EVENTVALIDATION"})
                if vs:  data["__VIEWSTATE"]        = vs.get("value", "")
                if evv: data["__EVENTVALIDATION"]  = evv.get("value", "")
                r2 = requests.post(FBCAD_DATA_URL, data=data, timeout=60)
                r2.raise_for_status()
                return r2.content
            return None

        if href.startswith("/"):
            from urllib.parse import urlparse
            parsed = urlparse(FBCAD_DATA_URL)
            href = f"{parsed.scheme}://{parsed.netloc}{href}"
        elif not href.startswith("http"):
            href = FBCAD_DATA_URL.rstrip("/") + "/" + href

        r2 = requests.get(href, timeout=120)
        r2.raise_for_status()
        return r2.content

    def _load_dbf_bytes(self, raw: bytes) -> list[dict]:
        if not HAS_DBFREAD:
            log.warning("dbfread unavailable – skipping parcel load")
            return []

        dbf_bytes = raw
        if raw[:2] == b"PK":
            with zipfile.ZipFile(io.BytesIO(raw)) as zf:
                dbf_names = [n for n in zf.namelist() if n.lower().endswith(".dbf")]
                if not dbf_names:
                    log.error("No DBF found inside ZIP")
                    return []
                dbf_bytes = zf.read(dbf_names[0])

        tmp = Path("/tmp/fbcad_parcel.dbf")
        tmp.write_bytes(dbf_bytes)
        try:
            table = DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True)
            return [dict(r) for r in table]
        except Exception as exc:
            log.error("DBF parse error: %s", exc)
            return []

    def _normalise(self, name: str) -> str:
        return re.sub(r"\s+", " ", str(name or "").upper().strip())

    def _col(self, rec: dict, *keys: str) -> str:
        for k in keys:
            v = rec.get(k) or rec.get(k.upper()) or rec.get(k.lower())
            if v:
                return str(v).strip()
        return ""

    def build(self) -> "ParcelLookup":
        raw = retry(self._download_dbf)
        if not raw:
            log.warning("Could not download parcel data – address enrichment disabled.")
            return self

        records = self._load_dbf_bytes(raw)
        log.info("Loaded %d parcel records", len(records))

        for rec in records:
            owner = self._col(rec, "OWNER", "OWN1", "OWN_NAME")
            if not owner:
                continue

            parcel = {
                "site_addr":  self._col(rec, "SITE_ADDR", "SITEADDR", "PROP_ADDR"),
                "site_city":  self._col(rec, "SITE_CITY", "SITECITY"),
                "site_zip":   self._col(rec, "SITE_ZIP",  "SITEZIP"),
                "mail_addr":  self._col(rec, "ADDR_1", "MAILADR1", "MAIL_ADDR"),
                "mail_city":  self._col(rec, "CITY",   "MAILCITY", "MAIL_CITY"),
                "mail_state": self._col(rec, "STATE",  "MAILSTATE"),
                "mail_zip":   self._col(rec, "ZIP",    "MAILZIP",  "MAIL_ZIP"),
            }

            norm = self._normalise(owner)
            self._index[norm] = parcel

            if "," in norm:
                parts = norm.split(",", 1)
                alt   = parts[1].strip() + " " + parts[0].strip()
                self._index[alt] = parcel
            else:
                tokens = norm.split()
                if len(tokens) >= 2:
                    alt = tokens[-1] + " " + " ".join(tokens[:-1])
                    self._index[alt] = parcel

        log.info("Parcel index built: %d entries", len(self._index))
        return self

    def lookup(self, owner_name: str) -> dict | None:
        norm = self._normalise(owner_name)
        if norm in self._index:
            return self._index[norm]
        tokens = norm.split()
        if tokens:
            for key, val in self._index.items():
                if key.startswith(tokens[0]):
                    return val
        return None


def compute_score(record: dict) -> tuple[int, list[str]]:
    flags: list[str] = []
    score = 30

    cat   = record.get("cat", "")
    dtype = record.get("doc_type", "")
    amt   = safe_float(record.get("amount"))
    owner = str(record.get("owner", ""))
    filed = str(record.get("filed", ""))

    if cat == "LP":
        flags.append("Lis pendens")
        score += 10
    if cat == "NOFC" or dtype == "NOFC":
        flags.append("Pre-foreclosure")
        score += 10
    if cat == "JUD":
        flags.append("Judgment lien")
        score += 10
    if cat in ("LN",) and "TAX" in dtype.upper():
        flags.append("Tax lien")
        score += 10
    if dtype in ("LNMECH",):
        flags.append("Mechanic lien")
        score += 10
    if cat == "PRO":
        flags.append("Probate / estate")
        score += 10

    if "Lis pendens" in flags and "Pre-foreclosure" in flags:
        score += 20

    if amt:
        if amt > 100_000:
            flags.append("High debt (>$100k)")
            score += 15
        elif amt > 50_000:
            score += 10

    try:
        filed_dt = datetime.strptime(filed[:10], "%Y-%m-%d")
        if (datetime.utcnow() - filed_dt).days <= 7:
            flags.append("New this week")
            score += 5
    except Exception:
        pass

    if record.get("prop_address"):
        score += 5

    corp_keywords = ("LLC", "INC", "CORP", "LP ", "LTD", "TRUST", "FUND")
    if any(kw in owner.upper() for kw in corp_keywords):
        flags.append("LLC / corp owner")
        score += 10

    return min(score, 100), flags


async def _clerk_search_type(page: Page, doc_code: str, start_date: str, end_date: str) -> list[dict]:
    results = []
    try:
        await page.goto(CLERK_URL, wait_until="networkidle", timeout=60_000)
        await asyncio.sleep(2)

        frames = page.frames
        search_frame = None
        for f in frames:
            if "record" in f.url.lower() or "search" in f.url.lower() or f.url != CLERK_URL:
                search_frame = f
                break

        target = search_frame or page

        selectors_to_try = [
            "#DocumentType", "#docType", "select[name*='Type']",
            "input[name*='DocType']", "select[id*='Doc']",
        ]
        doc_sel = None
        for sel in selectors_to_try:
            try:
                el = await target.wait_for_selector(sel, timeout=3_000)
                if el:
                    doc_sel = sel
                    break
            except Exception:
                continue

        if doc_sel:
            await target.select_option(doc_sel, doc_code)

        for sel in ["#StartDate", "#startDate", "input[name*='Start']", "input[id*='From']"]:
            try:
                el = await target.query_selector(sel)
                if el:
                    await el.fill(start_date)
                    break
            except Exception:
                continue

        for sel in ["#EndDate", "#endDate", "input[name*='End']", "input[id*='To']"]:
            try:
                el = await target.query_selector(sel)
                if el:
                    await el.fill(end_date)
                    break
            except Exception:
                continue

        for sel in ["button[type='submit']", "input[type='submit']", "#btnSearch", "#searchBtn"]:
            try:
                btn = await target.query_selector(sel)
                if btn:
                    await btn.click()
                    break
            except Exception:
                continue

        await asyncio.sleep(3)
        await target.wait_for_load_state("networkidle")

        html   = await target.content()
        soup   = BeautifulSoup(html, "lxml")
        tables = soup.find_all("table")
        for tbl in tables:
            rows = tbl.find_all("tr")
            if len(rows) < 2:
                continue
            headers = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th", "td"])]
            if not headers:
                continue
            for row in rows[1:]:
                cells = row.find_all("td")
                if not cells:
                    continue
                row_data: dict[str, str] = {}
                for i, cell in enumerate(cells):
                    key = headers[i] if i < len(headers) else f"col{i}"
                    row_data[key] = cell.get_text(strip=True)
                    a = cell.find("a", href=True)
                    if a:
                        row_data[f"{key}_link"] = a["href"]
                row_data["_doc_code"] = doc_code
                results.append(row_data)

        log.info("  %s -> %d raw rows", doc_code, len(results))
    except Exception as exc:
        log.warning("Clerk search failed for %s: %s", doc_code, exc)

    return results


def _normalise_clerk_row(raw: dict) -> dict | None:
    try:
        doc_code  = raw.get("_doc_code", "")
        cat_info  = DOC_TYPE_MAP.get(doc_code, (doc_code, doc_code))

        def g(*keys):
            for k in keys:
                for actual_key in raw:
                    if k.lower() in actual_key.lower():
                        return raw[actual_key]
            return ""

        doc_num   = g("instrument", "doc #", "doc num", "number", "docnum")
        doc_type  = raw.get("_doc_code", g("type", "doc type"))
        filed     = g("filed", "date", "recorded", "file date")
        grantor   = g("grantor", "owner", "seller", "from")
        grantee   = g("grantee", "buyer", "to", "lender")
        legal     = g("legal", "description", "property")
        amount    = g("amount", "debt", "value", "$")
        link      = g("_link", "url", "view")

        filed_norm = ""
        for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y", "%d/%m/%Y"):
            try:
                filed_norm = datetime.strptime(filed[:10], fmt).strftime("%Y-%m-%d")
                break
            except Exception:
                continue
        if not filed_norm:
            filed_norm = filed[:10] if filed else ""

        base = "https://www.fortbendcountytx.gov"
        if link and link.startswith("http"):
            clerk_url = link
        elif link:
            clerk_url = base + link
        elif doc_num:
            clerk_url = f"{CLERK_URL}?doc={doc_num}"
        else:
            clerk_url = CLERK_URL

        return {
            "doc_num":      doc_num or "N/A",
            "doc_type":     doc_type,
            "filed":        filed_norm,
            "cat":          cat_info[0],
            "cat_label":    cat_info[1],
            "owner":        grantor.strip(),
            "grantee":      grantee.strip(),
            "amount":       safe_float(amount),
            "legal":        legal.strip(),
            "clerk_url":    clerk_url,
            "prop_address": "",
            "prop_city":    "Fort Bend",
            "prop_state":   "TX",
            "prop_zip":     "",
            "mail_address": "",
            "mail_city":    "",
            "mail_state":   "TX",
            "mail_zip":     "",
        }
    except Exception as exc:
        log.debug("Row normalise error: %s | row=%s", exc, raw)
        return None


async def scrape_clerk(start_date: str, end_date: str) -> list[dict]:
    if not HAS_PLAYWRIGHT:
        log.warning("Playwright unavailable – returning empty clerk results")
        return []

    records: list[dict] = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        ctx: BrowserContext = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0 Safari/537.36"
            )
        )
        page = await ctx.new_page()

        for doc_code in TARGET_CODES:
            log.info("Searching clerk for: %s", doc_code)
            raw_rows = []
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    raw_rows = await _clerk_search_type(page, doc_code, start_date, end_date)
                    break
                except Exception as exc:
                    log.warning("Attempt %d failed for %s: %s", attempt + 1, doc_code, exc)
                    await asyncio.sleep(RETRY_DELAY * (attempt + 1))

            for row in raw_rows:
                norm = _normalise_clerk_row(row)
                if norm and norm.get("doc_num"):
                    records.append(norm)

        await browser.close()

    log.info("Clerk scrape complete: %d records", len(records))
    return records


def scrape_clerk_static(start_date: str, end_date: str) -> list[dict]:
    records: list[dict] = []
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (compatible; FBCountyScraper/1.0)",
        "Accept": "application/json, text/html, */*",
    })

    tyler_endpoints = [
        "https://www.fortbendcountytx.gov/government/courts/court-records-research",
        "https://recordsearch.fortbendcountytx.gov/",
    ]

    for base in tyler_endpoints:
        try:
            r = session.get(base, timeout=20)
            soup = BeautifulSoup(r.text, "lxml")
            form = soup.find("form")
            if form:
                action = form.get("action", base)
                if not action.startswith("http"):
                    action = base.rstrip("/") + "/" + action.lstrip("/")
                inputs = {
                    i.get("name"): i.get("value", "")
                    for i in form.find_all("input")
                    if i.get("name")
                }
                for doc_code in TARGET_CODES:
                    payload = {**inputs, "DocType": doc_code,
                               "StartDate": start_date, "EndDate": end_date}
                    try:
                        r2 = session.post(action, data=payload, timeout=30)
                        soup2 = BeautifulSoup(r2.text, "lxml")
                        for tbl in soup2.find_all("table"):
                            rows = tbl.find_all("tr")
                            if len(rows) < 2:
                                continue
                            hdrs = [th.get_text(strip=True).lower()
                                    for th in rows[0].find_all(["th", "td"])]
                            for row in rows[1:]:
                                cells = row.find_all("td")
                                raw   = {hdrs[i] if i < len(hdrs) else f"col{i}": c.get_text(strip=True)
                                         for i, c in enumerate(cells)}
                                raw["_doc_code"] = doc_code
                                norm = _normalise_clerk_row(raw)
                                if norm:
                                    records.append(norm)
                    except Exception:
                        pass
        except Exception as exc:
            log.debug("Static scrape attempt failed for %s: %s", base, exc)

    log.info("Static clerk scrape: %d records", len(records))
    return records


def enrich_records(records: list[dict], parcel: ParcelLookup) -> list[dict]:
    enriched = 0
    for rec in records:
        owner = rec.get("owner", "")
        if not owner:
            continue
        hit = parcel.lookup(owner)
        if hit:
            rec["prop_address"] = hit["site_addr"]
            rec["prop_city"]    = hit["site_city"] or "Fort Bend"
            rec["prop_state"]   = "TX"
            rec["prop_zip"]     = hit["site_zip"]
            rec["mail_address"] = hit["mail_addr"]
            rec["mail_city"]    = hit["mail_city"]
            rec["mail_state"]   = hit["mail_state"] or "TX"
            rec["mail_zip"]     = hit["mail_zip"]
            enriched += 1
    log.info("Enriched %d/%d records with parcel addresses", enriched, len(records))
    return records


def deduplicate(records: list[dict]) -> list[dict]:
    seen: set[str] = set()
    out:  list[dict] = []
    for rec in records:
        key = f"{rec.get('doc_num','')}|{rec.get('doc_type','')}|{rec.get('filed','')}"
        if key not in seen:
            seen.add(key)
            out.append(rec)
    return out


def score_records(records: list[dict]) -> list[dict]:
    for rec in records:
        score, flags = compute_score(rec)
        rec["score"] = score
        rec["flags"] = flags
    return sorted(records, key=lambda r: r.get("score", 0), reverse=True)


def build_output(records: list[dict], start_date: str, end_date: str) -> dict:
    with_addr = sum(1 for r in records if r.get("prop_address"))
    return {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Fort Bend County Clerk + FBCAD",
        "date_range":   {"start": start_date, "end": end_date},
        "total":        len(records),
        "with_address": with_addr,
        "records":      records,
    }


def write_json(payload: dict, *paths: Path) -> None:
    for p in paths:
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info("Wrote %s  (%d records)", p, payload["total"])


def write_ghl_csv(records: list[dict], out_dir: Path) -> Path:
    out_dir.mkdir(parents=True, exist_ok=True)
    date_str  = datetime.utcnow().strftime("%Y%m%d")
    csv_path  = out_dir / f"ghl_export_{date_str}.csv"

    columns = [
        "First Name", "Last Name",
        "Mailing Address", "Mailing City", "Mailing State", "Mailing Zip",
        "Property Address", "Property City", "Property State", "Property Zip",
        "Lead Type", "Document Type", "Date Filed", "Document Number",
        "Amount/Debt Owed", "Seller Score", "Motivated Seller Flags",
        "Source", "Public Records URL",
    ]

    with csv_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for rec in records:
            first, last = parse_name(rec.get("owner", ""))
            writer.writerow({
                "First Name":             first,
                "Last Name":              last,
                "Mailing Address":        rec.get("mail_address", ""),
                "Mailing City":           rec.get("mail_city", ""),
                "Mailing State":          rec.get("mail_state", "TX"),
                "Mailing Zip":            rec.get("mail_zip", ""),
                "Property Address":       rec.get("prop_address", ""),
                "Property City":          rec.get("prop_city", "Fort Bend"),
                "Property State":         rec.get("prop_state", "TX"),
                "Property Zip":           rec.get("prop_zip", ""),
                "Lead Type":              rec.get("cat_label", ""),
                "Document Type":          rec.get("doc_type", ""),
                "Date Filed":             rec.get("filed", ""),
                "Document Number":        rec.get("doc_num", ""),
                "Amount/Debt Owed":       rec.get("amount", ""),
                "Seller Score":           rec.get("score", 0),
                "Motivated Seller Flags": " | ".join(rec.get("flags", [])),
                "Source":                 "Fort Bend County Clerk",
                "Public Records URL":     rec.get("clerk_url", ""),
            })

    log.info("GHL CSV -> %s  (%d rows)", csv_path, len(records))
    return csv_path


async def main() -> None:
    end_dt    = datetime.now(timezone.utc)
    start_dt  = end_dt - timedelta(days=LOOKBACK_DAYS)
    start_str = start_dt.strftime("%m/%d/%Y")
    end_str   = end_dt.strftime("%m/%d/%Y")
    log.info("Date range: %s -> %s", start_str, end_str)

    parcel = ParcelLookup()
    try:
        parcel.build()
    except Exception as exc:
        log.warning("Parcel build failed: %s", exc)

    records: list[dict] = []

    if HAS_PLAYWRIGHT:
        records = await scrape_clerk(start_str, end_str)

    if not records:
        log.info("Falling back to static clerk scrape ...")
        records = scrape_clerk_static(start_str, end_str)

    if not records:
        log.warning("No records found from clerk portal – creating empty output")

    records = enrich_records(records, parcel)
    records = deduplicate(records)
    records = score_records(records)

    iso_start = start_dt.strftime("%Y-%m-%d")
    iso_end   = end_dt.strftime("%Y-%m-%d")
    payload   = build_output(records, iso_start, iso_end)

    write_json(
        payload,
        DASHBOARD_DIR / "records.json",
        DATA_DIR      / "records.json",
    )

    write_ghl_csv(records, DATA_DIR)

    log.info("Done. Total: %d  |  With address: %d", payload["total"], payload["with_address"])


if __name__ == "__main__":
    asyncio.run(main())
