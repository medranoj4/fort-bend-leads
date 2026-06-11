"""
Fort Bend County, TX — Motivated Seller Lead Scraper
Clerk: ccweb.co.fort-bend.tx.us (Kofile portal)
Parcel: FBCAD OwnerExport.txt + PropertyExport.txt
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

CLERK_BASE = "https://ccweb.co.fort-bend.tx.us"
FBCAD_URL  = "https://www.fbcad.org/data-files/"

DOC_TYPE_MAP = {
    "LP":("LP","Lis Pendens"), "NOFC":("NOFC","Notice of Foreclosure"),
    "TAXDEED":("TAXDEED","Tax Deed"), "JUD":("JUD","Judgment"),
    "CCJ":("JUD","Certified Judgment"), "DRJUD":("JUD","Domestic Relations Judgment"),
    "LNCORPTX":("LN","Corp Tax Lien"), "LNIRS":("LN","IRS Lien"),
    "LNFED":("LN","Federal Lien"), "LN":("LN","Lien"),
    "LNMECH":("LN","Mechanic Lien"), "LNHOA":("LN","HOA Lien"),
    "MEDLN":("LN","Medicaid Lien"), "PRO":("PRO","Probate Document"),
    "NOC":("NOC","Notice of Commencement"), "RELLP":("RELLP","Release Lis Pendens"),
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
# Confirmed column names from log line 18-19:
# Property: RecordType, PropertyQuickRefID, PropertyNumber, SitusStreetAddress,
#           SitusCity, SitusState, SitusZip, LegalDesc
# Owner:    RecordType, PropertyQuickRefID, OwnerName, OwnerAddress1,
#           OwnerAddress2, OwnerAddress3, OwnerCity, OwnerState, OwnerPostalCode

class ParcelLookup:
    def __init__(self): self._idx: dict[str,dict] = {}

    def _norm(self, s): return re.sub(r"\s+"," ",str(s or "").upper().strip())

    def _add(self, parcel, owner):
        if not owner: return
        n = self._norm(owner)
        if not n: return
        self._idx[n] = parcel
        if "," in n:
            p = n.split(",",1); self._idx[p[1].strip()+" "+p[0].strip()] = parcel
        else:
            t = n.split()
            if len(t)>=2: self._idx[t[-1]+" "+" ".join(t[:-1])] = parcel

    def build(self):
        log.info("Fetching FBCAD page ...")
        try:
            r = requests.get(FBCAD_URL, timeout=30); r.raise_for_status()
            soup = BeautifulSoup(r.text,"lxml")
            zips = []
            for a in soup.find_all("a", href=True):
                h = a["href"]
                if h.lower().endswith(".zip"):
                    if not h.startswith("http"): h = "https://www.fbcad.org"+h
                    zips.append(h)

            # Prefer ZIPs that have "Supplement" or "Export" and are recent (2025 first)
            def score_zip(url):
                u = url.lower()
                s = 0
                if "2025" in u: s += 100
                if "2024" in u: s += 50
                if "supplement" in u: s += 20
                if "export" in u and "redact" not in u: s += 30
                if any(x in u for x in ("segment","commercial","residential","sales",
                                         "improvement","land","grandtotal","prelim",
                                         "agent","entity","exemption","mobile")): s -= 50
                return s

            zips_sorted = sorted(zips, key=score_zip, reverse=True)
            log.info("Will try %d ZIPs, top 3: %s", len(zips_sorted),
                     [z.split("/")[-1] for z in zips_sorted[:3]])

            for url in zips_sorted[:8]:
                try:
                    log.info("Trying: %s", url.split("/")[-1])
                    r2 = requests.get(url, timeout=120); r2.raise_for_status()
                    count = self._load_zip(r2.content)
                    if count > 0:
                        log.info("Parcel index: %d entries", len(self._idx))
                        return self
                except Exception as e:
                    log.warning("ZIP failed: %s", e)
        except Exception as e:
            log.warning("FBCAD failed: %s", e)
        log.warning("Parcel lookup unavailable")
        return self

    def _load_zip(self, raw):
        count = 0
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            names = zf.namelist()
            owner_files = [n for n in names if "owner" in n.lower()
                           and n.lower().endswith((".txt",".csv"))]
            prop_files  = [n for n in names if "property" in n.lower()
                           and n.lower().endswith((".txt",".csv"))]

            if not owner_files:
                return 0

            # Load property file → account# to situs address map
            addr_map: dict[str,dict] = {}
            if prop_files:
                try:
                    raw_csv = zf.read(prop_files[0]).decode("latin-1",errors="replace")
                    delim   = "\t" if raw_csv.split("\n")[0].count("\t") > raw_csv.split("\n")[0].count(",") else ","
                    reader  = csv.DictReader(io.StringIO(raw_csv), delimiter=delim)
                    for row in reader:
                        # Use confirmed column names
                        acct = row.get("PropertyQuickRefID","") or row.get("PropertyNumber","")
                        if not acct: continue
                        addr_map[str(acct).strip()] = {
                            "site_addr": row.get("SitusStreetAddress","").strip(),
                            "site_city": row.get("SitusCity","").strip(),
                            "site_zip":  row.get("SitusZip","").strip(),
                        }
                    log.info("Loaded %d property records", len(addr_map))
                except Exception as e:
                    log.warning("Property file error: %s", e)

            # Load owner file
            try:
                raw_csv = zf.read(owner_files[0]).decode("latin-1",errors="replace")
                delim   = "\t" if raw_csv.split("\n")[0].count("\t") > raw_csv.split("\n")[0].count(",") else ","
                reader  = csv.DictReader(io.StringIO(raw_csv), delimiter=delim)
                for row in reader:
                    # Confirmed columns: OwnerName, PropertyQuickRefID,
                    # OwnerAddress1, OwnerCity, OwnerState, OwnerPostalCode
                    owner = row.get("OwnerName","").strip()
                    if not owner: continue
                    acct  = str(row.get("PropertyQuickRefID","")).strip()
                    site  = addr_map.get(acct, {})
                    parcel = {
                        "site_addr":  site.get("site_addr",""),
                        "site_city":  site.get("site_city",""),
                        "site_zip":   site.get("site_zip",""),
                        "mail_addr":  row.get("OwnerAddress1","").strip(),
                        "mail_city":  row.get("OwnerCity","").strip(),
                        "mail_state": row.get("OwnerState","").strip(),
                        "mail_zip":   row.get("OwnerPostalCode","").strip(),
                    }
                    self._add(parcel, owner)
                    count += 1
                log.info("Loaded %d owner records", count)
            except Exception as e:
                log.warning("Owner file error: %s", e)
        return count

    def lookup(self, owner):
        n = self._norm(owner)
        if n in self._idx: return self._idx[n]
        t = n.split()
        if t:
            for k,v in self._idx.items():
                if k.startswith(t[0]): return v
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

# ── Clerk scraper ─────────────────────────────────────────────────────────────

async def scrape_clerk(start_date: str, end_date: str) -> list[dict]:
    if not HAS_PLAYWRIGHT:
        log.warning("Playwright not available"); return []

    records = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        ctx = await browser.new_context(
            user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            viewport={"width":1280,"height":800}
        )
        page = await ctx.new_page()

        log.info("Loading clerk portal ...")
        try:
            await page.goto(CLERK_BASE+"/recorder/web/login.jsp",
                            wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(1)
        except Exception: pass

        # Navigate to search page
        try:
            await page.goto(CLERK_BASE+"/recorder/web/docSearch.jsp",
                            wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)
        except Exception as e:
            log.warning("Could not load search page: %s", e)
            await browser.close(); return []

        log.info("Portal title: %s | URL: %s", await page.title(), page.url)

        # Dump the page HTML so we can see the actual form fields
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        # Log all form fields to understand the search form
        forms = soup.find_all("form")
        log.info("Found %d form(s) on search page", len(forms))
        for i, form in enumerate(forms):
            inputs = form.find_all(["input","select"])
            log.info("Form %d action=%s fields=%s", i,
                     form.get("action",""),
                     [(el.get("name",""),el.get("type",""),el.get("value",""))
                      for el in inputs[:10]])

        # Also log any select options for doc type
        for sel in soup.find_all("select"):
            opts = [o.get("value","") for o in sel.find_all("option")]
            log.info("Select name=%s options=%s", sel.get("name","?"), opts[:20])

        # Try submitting the search form via Playwright
        for doc_code in TARGET_CODES:
            log.info("Searching: %s", doc_code)
            rows = []
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    rows = await _kofile_search(page, doc_code, start_date, end_date)
                    break
                except Exception as e:
                    log.warning("  attempt %d: %s", attempt+1, e)
                    await asyncio.sleep(RETRY_DELAY*(attempt+1))
            records.extend(rows)

        await browser.close()
    log.info("Clerk done: %d records", len(records))
    return records


async def _kofile_search(page: Page, doc_code: str, start: str, end: str) -> list[dict]:
    """
    Try multiple search URL formats used by Kofile/Tyler portals.
    Dates in MM/DD/YYYY format.
    """
    results = []

    # Format 1: Standard Kofile OPR search
    urls_to_try = [
        (f"{CLERK_BASE}/recorder/web/docSearch.jsp"
         f"?searchType=DTR&RecordType=OPR"
         f"&docType={doc_code}&beginDate={start}&endDate={end}&submit=Search"),

        # Format 2: Without RecordType
        (f"{CLERK_BASE}/recorder/web/docSearch.jsp"
         f"?searchType=DTR"
         f"&docType={doc_code}&beginDate={start}&endDate={end}&submit=Search"),

        # Format 3: instrument type search
        (f"{CLERK_BASE}/recorder/web/docSearch.jsp"
         f"?searchType=ITR"
         f"&instrumentType={doc_code}&startDate={start}&endDate={end}&submit=Search"),
    ]

    for url in urls_to_try:
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(1)

            if "login" in page.url.lower():
                log.warning("  %s: login redirect", doc_code)
                break

            content = await page.content()
            soup = BeautifulSoup(content, "lxml")

            # Log page structure for first doc type to debug
            if doc_code == "LP":
                title = await page.title()
                log.info("  LP search page title: %s | url: %s", title, page.url)
                tbls = soup.find_all("table")
                log.info("  Tables found: %d", len(tbls))
                for t in tbls[:3]:
                    rows = t.find_all("tr")
                    if rows:
                        hdrs = [th.get_text(strip=True)[:30] for th in rows[0].find_all(["th","td"])]
                        log.info("  Table headers: %s", hdrs)
                # Also log any result count text
                for el in soup.find_all(string=re.compile(r"result|record|found|total", re.I)):
                    log.info("  Result text: %s", str(el)[:100])

            # Parse tables
            for tbl in soup.find_all("table"):
                rows = tbl.find_all("tr")
                if len(rows) < 2: continue
                hdrs = [th.get_text(strip=True).lower()
                        for th in rows[0].find_all(["th","td"])]
                if not any(h in " ".join(hdrs)
                           for h in ["grantor","grantee","document","instrument",
                                     "filed","recorded","name","date"]):
                    continue
                found_data = False
                for tr in rows[1:]:
                    cells = tr.find_all("td")
                    if not cells: continue
                    raw = {}
                    for i,cell in enumerate(cells):
                        k = hdrs[i] if i<len(hdrs) else f"c{i}"
                        raw[k] = cell.get_text(strip=True)
                        a = cell.find("a",href=True)
                        if a: raw[k+"_href"] = a["href"]
                    raw["_code"] = doc_code
                    rec = _make_record(raw)
                    if rec:
                        results.append(rec)
                        found_data = True

            if results:
                break  # Found results with this URL format

        except Exception as e:
            log.warning("  URL format failed: %s", e)

    log.info("  %s -> %d", doc_code, len(results))
    return results


def _make_record(raw):
    try:
        code = raw.get("_code","")
        cat, label = DOC_TYPE_MAP.get(code,(code,code))
        def g(*ks):
            for k in ks:
                for ak in raw:
                    if k in ak.lower() and not ak.endswith("_href"):
                        v=raw[ak]
                        if v: return v
            return ""
        def gh(*ks):
            for k in ks:
                for ak in raw:
                    if k in ak.lower() and ak.endswith("_href"):
                        return raw[ak]
            return ""
        doc_num = g("instrument","doc #","docnum","number")
        filed   = g("filed","recorded","date")
        grantor = g("grantor","owner","from","name")
        grantee = g("grantee","to","buyer","lender")
        legal   = g("legal","description")
        amount  = g("amount","consideration","debt")
        link    = gh("instrument","doc","view") or g("url","link")
        fn = ""
        for fmt in ("%m/%d/%Y","%Y-%m-%d","%m-%d-%Y"):
            try: fn=datetime.strptime(filed[:10],fmt).strftime("%Y-%m-%d"); break
            except: pass
        if link and not link.startswith("http"):
            link = CLERK_BASE+"/"+link.lstrip("/")
        if not link:
            link = f"{CLERK_BASE}/recorder/web/docSearch.jsp"
        return {"doc_num":doc_num or "N/A","doc_type":code,"filed":fn,
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
            rec.update({"prop_address":hit["site_addr"],
                        "prop_city":hit["site_city"] or "Fort Bend",
                        "prop_state":"TX","prop_zip":hit["site_zip"],
                        "mail_address":hit["mail_addr"],"mail_city":hit["mail_city"],
                        "mail_state":hit["mail_state"] or "TX","mail_zip":hit["mail_zip"]})
            enriched += 1
    log.info("Enriched %d/%d records", enriched, len(records))
    return records

def dedup(records):
    seen,out = set(),[]
    for r in records:
        k=f"{r.get('doc_num')}|{r.get('doc_type')}|{r.get('filed')}"
        if k not in seen: seen.add(k); out.append(r)
    return out

def score_all(records):
    for r in records: r["score"],r["flags"] = compute_score(r)
    return sorted(records, key=lambda r:r.get("score",0), reverse=True)

def write_json(payload, *paths):
    for p in paths:
        Path(p).parent.mkdir(parents=True, exist_ok=True)
        Path(p).write_text(json.dumps(payload,indent=2,default=str),encoding="utf-8")
        log.info("Wrote %s (%d records)", p, payload["total"])

def write_csv(records, out_dir):
    out_dir = Path(out_dir); out_dir.mkdir(parents=True, exist_ok=True)
    p = out_dir/f"ghl_export_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    cols=["First Name","Last Name","Mailing Address","Mailing City","Mailing State",
          "Mailing Zip","Property Address","Property City","Property State","Property Zip",
          "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
          "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    with p.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=cols); w.writeheader()
        for rec in records:
            fn,ln=parse_name(rec.get("owner",""))
            w.writerow({"First Name":fn,"Last Name":ln,
                "Mailing Address":rec.get("mail_address",""),
                "Mailing City":rec.get("mail_city",""),
                "Mailing State":rec.get("mail_state","TX"),
                "Mailing Zip":rec.get("mail_zip",""),
                "Property Address":rec.get("prop_address",""),
                "Property City":rec.get("prop_city","Fort Bend"),
                "Property State":rec.get("prop_state","TX"),
                "Property Zip":rec.get("prop_zip",""),
                "Lead Type":rec.get("cat_label",""),
                "Document Type":rec.get("doc_type",""),
                "Date Filed":rec.get("filed",""),
                "Document Number":rec.get("doc_num",""),
                "Amount/Debt Owed":rec.get("amount",""),
                "Seller Score":rec.get("score",0),
                "Motivated Seller Flags":" | ".join(rec.get("flags",[])),
                "Source":"Fort Bend County Clerk",
                "Public Records URL":rec.get("clerk_url","")})
    log.info("CSV -> %s (%d rows)", p, len(records))

# ── Main ──────────────────────────────────────────────────────────────────────

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
    payload = {"fetched_at":datetime.utcnow().isoformat()+"Z",
               "source":"Fort Bend County Clerk + FBCAD",
               "date_range":{"start":iso_start,"end":iso_end},
               "total":len(records),
               "with_address":sum(1 for r in records if r.get("prop_address")),
               "records":records}

    write_json(payload, DASHBOARD_DIR/"records.json", DATA_DIR/"records.json")
    write_csv(records, DATA_DIR)
    log.info("Done. Total:%d  With address:%d", payload["total"], payload["with_address"])

if __name__=="__main__":
    asyncio.run(main())
