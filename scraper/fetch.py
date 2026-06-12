"""
Fort Bend County, TX — Motivated Seller Lead Scraper
Fixes: tight parcel matching (no false address repeats) + residential-only filter
"""
from __future__ import annotations
import asyncio, csv, io, json, logging, os, re, time, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import requests
from bs4 import BeautifulSoup

try:
    from playwright.async_api import async_playwright, Page
    HAS_PLAYWRIGHT = True
except ImportError:
    HAS_PLAYWRIGHT = False

try:
    from dbfread import DBF
    HAS_DBFREAD = True
except ImportError:
    HAS_DBFREAD = False

BASE_DIR      = Path(__file__).resolve().parent.parent
DASHBOARD_DIR = BASE_DIR / "dashboard"
DATA_DIR      = BASE_DIR / "data"
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "7"))
HEADLESS      = os.getenv("HEADLESS", "true").lower() != "false"

CLERK_BASE   = "http://ccweb.co.fort-bend.tx.us"
CLERK_SEARCH = "http://ccweb.co.fort-bend.tx.us/RealEstate/SearchEntry.aspx"
FBCAD_URL    = "https://www.fbcad.org/data-files/"

# Residential property type codes in FBCAD
RESIDENTIAL_TYPES = {
    "A",    # Single family residential
    "B",    # Multi-family residential
    "C1",   # Vacant residential land
    "D1",   # Qualified open-space land (ag)
    "E",    # Rural real estate
    "R",    # Residential (generic)
    "RS",   # Residential
    "RES",  # Residential
    "SF",   # Single family
    "MF",   # Multi-family
    "RESIDENTIAL",
    "",     # Keep blanks — field may not exist in all exports
}

DOC_TYPE_MAP = {
    "LP":       ("LP",      "Lis Pendens",                 ["LISPEN"]),
    "NOFC":     ("NOFC",    "Notice of Foreclosure",       ["NOTICE"]),
    "TAXDEED":  ("TAXDEED", "Tax Deed",                    ["DEED"]),
    "JUD":      ("JUD",     "Judgment",                    ["JUDGE"]),
    "CCJ":      ("JUD",     "Certified Judgment",          ["JUDGE"]),
    "DRJUD":    ("JUD",     "Domestic Relations Judgment", ["JUDGE"]),
    "LNCORPTX": ("LN",      "Corp Tax Lien",               ["FEDLIEN"]),
    "LNIRS":    ("LN",      "IRS Lien",                    ["FEDLIEN"]),
    "LNFED":    ("LN",      "Federal Lien",                ["FEDLIEN"]),
    "LN":       ("LN",      "Lien",                        ["LIEN"]),
    "LNMECH":   ("LN",      "Mechanic Lien",               ["LIEN"]),
    "LNHOA":    ("LN",      "HOA Lien",                    ["LIEN"]),
    "MEDLN":    ("LN",      "Medicaid Lien",               ["LIEN"]),
    "PRO":      ("PRO",     "Probate Document",            ["HEIRSHIP"]),
    "NOC":      ("NOC",     "Notice of Commencement",      ["NOTICE"]),
    "RELLP":    ("RELLP",   "Release Lis Pendens",         ["RELEASE"]),
}
TARGET_CODES   = list(DOC_TYPE_MAP.keys())
RETRY_ATTEMPTS = 3
RETRY_DELAY    = 3

logging.basicConfig(level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("fb_scraper")

def safe_float(v):
    try: return float(re.sub(r"[^\d.]","",str(v))) if v else None
    except: return None

def parse_name(full):
    full = (full or "").strip()
    if not full: return ("","")
    if "," in full:
        p = full.split(",",1)
        return (p[1].strip().split()[0].title() if p[1].strip() else "", p[0].strip().title())
    t = full.split()
    return (t[0].title(), " ".join(t[1:]).title()) if len(t)>1 else ("",t[0].title())

# ── Parcel lookup ─────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self):
        self._idx: dict[str, dict] = {}
        self._residential_addrs: set[str] = set()  # situs addresses confirmed residential

    def _norm(self, s): return re.sub(r"\s+"," ",str(s or "").upper().strip())

    def _is_residential(self, prop_type: str) -> bool:
        return prop_type.upper() in RESIDENTIAL_TYPES

    def _add(self, parcel: dict, owner: str):
        if not owner: return
        n = self._norm(owner)
        if not n: return
        self._idx[n] = parcel
        # Index name variants — exact only, no partial
        if "," in n:
            p = n.split(",",1)
            alt = p[1].strip() + " " + p[0].strip()
            self._idx[alt] = parcel
        else:
            tokens = n.split()
            if len(tokens) >= 2:
                # "LAST FIRST" variant
                alt = tokens[-1] + " " + " ".join(tokens[:-1])
                self._idx[alt] = parcel

    def build(self):
        log.info("Fetching FBCAD page ...")
        try:
            r = requests.get(FBCAD_URL, timeout=30); r.raise_for_status()
            soup = BeautifulSoup(r.text, "lxml")
            zips = []
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if h.lower().endswith(".zip"):
                    if not h.startswith("http"): h = "https://www.fbcad.org" + h
                    zips.append(h)

            def score_zip(url):
                u = url.lower(); s = 0
                if "2025" in u: s += 100
                if "2024" in u: s += 50
                if "supplement" in u and "redact" not in u: s += 30
                if any(x in u for x in ("segment","commercial","residential","sales",
                    "improvement","land","grandtotal","prelim","agent","entity",
                    "exemption","mobile")): s -= 50
                return s

            for url in sorted(zips, key=score_zip, reverse=True)[:8]:
                try:
                    log.info("Trying: %s", url.split("/")[-1])
                    r2 = requests.get(url, timeout=120); r2.raise_for_status()
                    count = self._load_zip(r2.content)
                    if count > 0:
                        log.info("Parcel index: %d residential entries", len(self._idx))
                        return self
                except Exception as e:
                    log.warning("ZIP failed: %s", e)
        except Exception as e:
            log.warning("FBCAD failed: %s", e)
        return self

    def _load_zip(self, raw: bytes) -> int:
        count = 0
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            names = zf.namelist()
            owner_files = [n for n in names if "owner" in n.lower()
                           and n.lower().endswith((".txt",".csv"))]
            prop_files  = [n for n in names if "property" in n.lower()
                           and n.lower().endswith((".txt",".csv"))]
            if not owner_files:
                return 0

            # ── Build address map from property file (residential only) ──
            addr_map: dict[str, dict] = {}
            if prop_files:
                try:
                    raw_csv = zf.read(prop_files[0]).decode("latin-1", errors="replace")
                    delim = "\t" if raw_csv.split("\n")[0].count("\t") > raw_csv.split("\n")[0].count(",") else ","
                    reader = csv.DictReader(io.StringIO(raw_csv), delimiter=delim)
                    cols = reader.fieldnames or []
log.info("Property file columns: %s", cols[:20])
                    res_count = 0
                    skip_count = 0
                    type_samples: set[str] = set()
                    all_rows = list(reader)
                    for row in all_rows:
                        pt = (row.get("PropertyTypeCode","") or "").strip()
                        if pt: type_samples.add(pt)
                    log.info("PropertyTypeCode values found: %s", sorted(type_samples)[:40])
                    for row in all_rows:
                        acct = row.get("PropertyQuickRefID","") or row.get("PropertyNumber","")
                        if not acct: continue

                        # Get property type — log all unique values seen
                        prop_type = (
                            row.get("PropertyTypeCode","") or
                            row.get("PropType","") or
                            row.get("PROPERTY_TYPE","") or
                            row.get("STATE_CODE","") or
                            ""
                        ).strip().upper()

                        # If we have a type field, filter to residential
                        if prop_type and not self._is_residential(prop_type):
                            skip_count += 1
                            continue

                        addr_map[str(acct).strip()] = {
                            "site_addr":  row.get("SitusStreetAddress","").strip(),
                            "site_city":  row.get("SitusCity","").strip(),
                            "site_zip":   row.get("SitusZip","").strip(),
                            "prop_type":  prop_type,
                        }
                        res_count += 1

                    log.info("Property file: %d residential, %d skipped (commercial/other)",
                             res_count, skip_count)
                except Exception as e:
                    log.warning("Property file error: %s", e)

            # ── Build owner index ──
            try:
                raw_csv = zf.read(owner_files[0]).decode("latin-1", errors="replace")
                delim = "\t" if raw_csv.split("\n")[0].count("\t") > raw_csv.split("\n")[0].count(",") else ","
                reader = csv.DictReader(io.StringIO(raw_csv), delimiter=delim)
                cols = reader.fieldnames or []
                log.info("Owner file columns: %s", cols[:20])

                for row in reader:
                    owner = row.get("OwnerName","").strip()
                    if not owner: continue
                    acct  = str(row.get("PropertyQuickRefID","")).strip()

                    # Only index owners whose property is in our residential addr_map
                    site = addr_map.get(acct)
                    if site is None:
                        # If addr_map is empty (no property file), include everyone
                        if addr_map:
                            continue
                        site = {}

                    self._add({
                        "site_addr":  site.get("site_addr",""),
                        "site_city":  site.get("site_city",""),
                        "site_zip":   site.get("site_zip",""),
                        "mail_addr":  row.get("OwnerAddress1","").strip(),
                        "mail_city":  row.get("OwnerCity","").strip(),
                        "mail_state": row.get("OwnerState","").strip(),
                        "mail_zip":   row.get("OwnerPostalCode","").strip(),
                    }, owner)
                    count += 1

                log.info("Loaded %d residential owner records", count)
            except Exception as e:
                log.warning("Owner file error: %s", e)

        return count

    def lookup(self, owner_name: str) -> dict | None:
        """
        Strict lookup — exact match only, no partial guessing.
        This prevents one owner's address from being assigned to a different owner.
        """
        if not owner_name:
            return None
        n = self._norm(owner_name)

        # 1. Exact match
        if n in self._idx:
            return self._idx[n]

        # 2. Comma-flipped: "SMITH JOHN" -> "JOHN SMITH"
        if "," in n:
            p = n.split(",",1)
            alt = p[1].strip() + " " + p[0].strip()
            if alt in self._idx:
                return self._idx[alt]
        else:
            tokens = n.split()
            if len(tokens) >= 2:
                alt = tokens[-1] + " " + " ".join(tokens[:-1])
                if alt in self._idx:
                    return self._idx[alt]

        # 3. No match — return None (do NOT guess)
        return None

# ── Scoring ───────────────────────────────────────────────────────────────────

def compute_score(rec):
    flags, score = [], 30
    cat, dtype = rec.get("cat",""), rec.get("doc_type","")
    amt   = safe_float(rec.get("amount"))
    owner = str(rec.get("owner",""))
    filed = str(rec.get("filed",""))
    if cat=="LP":    flags.append("Lis pendens");      score+=10
    if cat=="NOFC":  flags.append("Pre-foreclosure");  score+=10
    if cat=="JUD":   flags.append("Judgment lien");    score+=10
    if cat=="LN" and "TAX" in dtype.upper(): flags.append("Tax lien"); score+=10
    if dtype=="LNMECH": flags.append("Mechanic lien"); score+=10
    if cat=="PRO":   flags.append("Probate / estate"); score+=10
    if "Lis pendens" in flags and "Pre-foreclosure" in flags: score+=20
    if amt:
        if amt>100000: flags.append("High debt (>$100k)"); score+=15
        elif amt>50000: score+=10
    try:
        if (datetime.utcnow()-datetime.strptime(filed[:10],"%Y-%m-%d")).days<=7:
            flags.append("New this week"); score+=5
    except: pass
    if rec.get("prop_address"): score+=5
    if any(kw in owner.upper() for kw in ("LLC","INC","CORP","LP ","LTD","TRUST","FUND")):
        flags.append("LLC / corp owner"); score+=10
    return min(score,100), flags

# ── Playwright clerk scraper ──────────────────────────────────────────────────

def _telerik_state(date_str: str) -> str:
    return json.dumps({
        "enabled": True, "emptyMessage": "",
        "validationText": date_str, "valueAsString": date_str,
        "minDateStr": "1/1/1980 0:0:0", "maxDateStr": "1/1/2099 0:0:0",
        "lastSetTextBoxValue": date_str
    }, separators=(',',':'))


async def _set_dates_via_js(page: Page, start: str, end: str):
    state_from = _telerik_state(start)
    state_to   = _telerik_state(end)
    await page.evaluate(f"""
    (function() {{
        var fields = {{
            'cphNoMargin_f_ddcDateFiledFrom_ClientState': {json.dumps(state_from)},
            'cphNoMargin_f_ddcDateFiledTo_ClientState':   {json.dumps(state_to)},
            'cphNoMargin_f_ddcDateFiledFrom_dateInput_ClientState': {json.dumps(state_from)},
            'cphNoMargin_f_ddcDateFiledTo_dateInput_ClientState':   {json.dumps(state_to)},
        }};
        for (var id in fields) {{
            var el = document.getElementById(id);
            if (el) el.value = fields[id];
        }}
        var f = document.getElementById('cphNoMargin_f_ddcDateFiledFrom_dateInput');
        if (f) f.value = '{start}';
        var t = document.getElementById('cphNoMargin_f_ddcDateFiledTo_dateInput');
        if (t) t.value = '{end}';
    }})();
    """)


async def _check_boxes_via_js(page: Page, doc_type_index: dict,
                               doc_values: list[str]) -> int:
    checked = 0
    for val in doc_values:
        fname = doc_type_index.get(val.upper())
        if not fname: continue
        result = await page.evaluate(f"""
        (function() {{
            var inputs = document.querySelectorAll('input[type="checkbox"]');
            for (var i = 0; i < inputs.length; i++) {{
                if (inputs[i].name === '{fname}' && inputs[i].value === '{val}') {{
                    inputs[i].checked = true;
                    return 1;
                }}
            }}
            return 0;
        }})();
        """)
        if result: checked += 1
    return checked


def _parse_results(soup: BeautifulSoup) -> list[dict]:
    JUNK = ["get a free copy","sort by","results list","new search","refine search",
            "0records","please enter","logon","login","basket","criteria",
            "click here","search ins","combined name","clear form","select all",
            "view basket","welcome","birth","death","marriage","selection criteria",
            "fort bend county texas"]
    results = []
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 2: continue
        hdrs = [th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th","td"])]
        hdr_str = " ".join(hdrs)
        if not any(x in hdr_str for x in
                   ["inst","grantor","filed","consideration","book","grantee"]):
            continue
        if any(j in hdr_str for j in JUNK): continue
        for tr in rows[1:]:
            cells = tr.find_all("td")
            if len(cells) < 3: continue
            row_text = " ".join(c.get_text(strip=True) for c in cells).lower()
            if any(j in row_text for j in JUNK): continue
            if len(row_text.strip()) < 5: continue
            raw = {}
            for i, cell in enumerate(cells):
                k = hdrs[i] if i < len(hdrs) else f"c{i}"
                v = cell.get_text(strip=True)
                if v: raw[k] = v
                a = cell.find("a", href=True)
                if a:
                    href = a["href"]
                    if not href.startswith("http"):
                        href = CLERK_BASE + "/" + href.lstrip("/")
                    raw[k+"_href"] = href
            results.append(raw)
    return results


async def scrape_clerk(start_date: str, end_date: str) -> list[dict]:
    if not HAS_PLAYWRIGHT:
        log.warning("Playwright not available"); return []

    records = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width":1280,"height":900}
        )
        page = await ctx.new_page()

        log.info("Loading search page ...")
        await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)

        content = await page.content()
        soup = BeautifulSoup(content, "lxml")
        doc_type_index: dict[str,str] = {}
        for inp in soup.find_all("input", {"type":"checkbox"}):
            name = inp.get("name","")
            val  = inp.get("value","")
            if "dclDocType" in name and val:
                doc_type_index[val.upper()] = name

        log.info("Doc type index: %d entries", len(doc_type_index))

        searched: set[tuple] = set()
        for doc_code, (cat, label, portal_vals) in DOC_TYPE_MAP.items():
            combo = tuple(sorted(portal_vals))
            if combo in searched:
                continue
            searched.add(combo)

            log.info("Searching %s (%s)", doc_code, portal_vals)
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    await page.goto(CLERK_SEARCH,
                                    wait_until="domcontentloaded", timeout=20000)
                    await asyncio.sleep(1)

                    checked = await _check_boxes_via_js(
                        page, doc_type_index, portal_vals)
                    if not checked:
                        log.warning("  No checkboxes found for %s", portal_vals)
                        break

                    await _set_dates_via_js(page, start_date, end_date)
                    log.info("  Checked %d boxes, dates set", checked)

                    for sel in ["#cphNoMargin_SearchButtons1_btnSearch",
                                "input[id*='btnSearch']",
                                "input[value='Search']"]:
                        try:
                            btn = await page.query_selector(sel)
                            if btn:
                                await btn.click()
                                break
                        except: pass

                    await asyncio.sleep(2)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=15000)
                    except: pass

                    log.info("  Result URL: %s", page.url)
                    content = await page.content()
                    result_soup = BeautifulSoup(content, "lxml")
                    raw_rows = _parse_results(result_soup)
                    log.info("  Found %d rows for %s", len(raw_rows), portal_vals)

                    for row in raw_rows:
                        row["_code"] = doc_code
                        rec = _make_record(row)
                        if rec:
                            records.append(rec)
                    break

                except Exception as e:
                    log.warning("  Attempt %d: %s", attempt+1, e)
                    await asyncio.sleep(RETRY_DELAY)

        await browser.close()

    log.info("Clerk done: %d records", len(records))
    return records


def _make_record(raw: dict) -> dict | None:
    try:
        code = raw.get("_code","")
        info = DOC_TYPE_MAP.get(code,(code,code,[]))
        cat, label = info[0], info[1]

        def g(*ks):
            for k in ks:
                for ak in raw:
                    if k.lower() in ak.lower() and not ak.endswith("_href"):
                        v = raw[ak]
                        if v and len(str(v)) < 200: return str(v)
            return ""
        def gh(*ks):
            for k in ks:
                for ak in raw:
                    if k.lower() in ak.lower() and ak.endswith("_href"):
                        return raw[ak]
            return ""

        doc_num = g("inst #","inst#","instrument","c0")
        filed   = g("filed","date filed","c3","c4")
        grantor = g("grantor","c5","c6")
        grantee = g("grantee","c6","c7")
        legal   = g("legal","c7","c8")
        amount  = g("consideration","amount","c8","c9")
        link    = gh("inst","view","c0")

        doc_num = (doc_num or "").strip()
        if not doc_num or len(doc_num) > 30: return None
        if not re.search(r'\d', doc_num): return None

        fn = ""
        for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y"):
            try: fn=datetime.strptime(filed[:10],fmt).strftime("%Y-%m-%d"); break
            except: pass

        if not link: link = CLERK_SEARCH
        elif not link.startswith("http"):
            link = CLERK_BASE+"/"+link.lstrip("/")

        return {"doc_num":doc_num,"doc_type":code,"filed":fn,
                "cat":cat,"cat_label":label,"owner":grantor.strip(),
                "grantee":grantee.strip(),"amount":safe_float(amount),
                "legal":legal.strip(),"clerk_url":link,
                "prop_address":"","prop_city":"Fort Bend","prop_state":"TX","prop_zip":"",
                "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":""}
    except Exception as e:
        log.debug("Record error: %s", e); return None

# ── Pipeline ──────────────────────────────────────────────────────────────────

def enrich(records, parcel):
    enriched = 0
    for rec in records:
        hit = parcel.lookup(rec.get("owner",""))
        if hit:
            rec.update({
                "prop_address": hit["site_addr"],
                "prop_city":    hit["site_city"] or "Fort Bend",
                "prop_state":   "TX",
                "prop_zip":     hit["site_zip"],
                "mail_address": hit["mail_addr"],
                "mail_city":    hit["mail_city"],
                "mail_state":   hit["mail_state"] or "TX",
                "mail_zip":     hit["mail_zip"],
            })
            enriched += 1
    log.info("Enriched %d/%d records", enriched, len(records))
    return records

def dedup(records):
    seen, out = set(), []
    for r in records:
        k = f"{r.get('doc_num')}|{r.get('doc_type')}|{r.get('filed')}"
        if k not in seen:
            seen.add(k); out.append(r)
    return out

def score_all(records):
    for r in records: r["score"], r["flags"] = compute_score(r)
    return sorted(records, key=lambda r: r.get("score",0), reverse=True)

def write_json(payload, *paths):
    for p in paths:
        Path(p).parent.mkdir(parents=True, exist_ok=True)
        Path(p).write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
        log.info("Wrote %s (%d records)", p, payload["total"])

def write_csv(records, out_dir):
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir / f"ghl_export_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    cols = ["First Name","Last Name","Mailing Address","Mailing City","Mailing State",
            "Mailing Zip","Property Address","Property City","Property State","Property Zip",
            "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
            "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    with p.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        for rec in records:
            fn, ln = parse_name(rec.get("owner",""))
            w.writerow({
                "First Name":             fn,
                "Last Name":              ln,
                "Mailing Address":        rec.get("mail_address",""),
                "Mailing City":           rec.get("mail_city",""),
                "Mailing State":          rec.get("mail_state","TX"),
                "Mailing Zip":            rec.get("mail_zip",""),
                "Property Address":       rec.get("prop_address",""),
                "Property City":          rec.get("prop_city","Fort Bend"),
                "Property State":         rec.get("prop_state","TX"),
                "Property Zip":           rec.get("prop_zip",""),
                "Lead Type":              rec.get("cat_label",""),
                "Document Type":          rec.get("doc_type",""),
                "Date Filed":             rec.get("filed",""),
                "Document Number":        rec.get("doc_num",""),
                "Amount/Debt Owed":       rec.get("amount",""),
                "Seller Score":           rec.get("score",0),
                "Motivated Seller Flags": " | ".join(rec.get("flags",[])),
                "Source":                 "Fort Bend County Clerk",
                "Public Records URL":     rec.get("clerk_url",""),
            })
    log.info("CSV -> %s (%d rows)", p, len(records))

async def main():
    end_dt   = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=LOOKBACK_DAYS)
    s = start_dt.strftime("%m/%d/%Y")
    e = end_dt.strftime("%m/%d/%Y")
    log.info("Range: %s -> %s", s, e)

    parcel = ParcelLookup()
    try: parcel.build()
    except Exception as ex: log.warning("Parcel failed: %s", ex)

    records = await scrape_clerk(s, e)
    records = enrich(dedup(score_all(records)), parcel)
    records = score_all(records)

    iso_start = start_dt.strftime("%Y-%m-%d")
    iso_end   = end_dt.strftime("%Y-%m-%d")
    payload = {
        "fetched_at":   datetime.utcnow().isoformat() + "Z",
        "source":       "Fort Bend County Clerk + FBCAD (Residential Only)",
        "date_range":   {"start": iso_start, "end": iso_end},
        "total":        len(records),
        "with_address": sum(1 for r in records if r.get("prop_address")),
        "records":      records,
    }

    write_json(payload, DASHBOARD_DIR/"records.json", DATA_DIR/"records.json")
    write_csv(records, DATA_DIR)
    log.info("Done. Total:%d  With address:%d", payload["total"], payload["with_address"])

if __name__ == "__main__":
    asyncio.run(main())
