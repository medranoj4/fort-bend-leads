"""
Fort Bend County, TX — Motivated Seller Lead Scraper
Clerk source: ccweb.co.fort-bend.tx.us (Kofile portal)
Parcel source: FBCAD CSV export
"""
from __future__ import annotations
import asyncio, csv, io, json, logging, os, re, time, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
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

CLERK_BASE    = "https://ccweb.co.fort-bend.tx.us"
FBCAD_URL     = "https://www.fbcad.org/data-files/"

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
    try:
        return float(re.sub(r"[^\d.]","",str(v))) if v else None
    except Exception:
        return None

def parse_name(full):
    full = (full or "").strip()
    if not full: return ("","")
    if "," in full:
        p = full.split(",",1)
        return (p[1].strip().split()[0].title() if p[1].strip() else "", p[0].strip().title())
    t = full.split()
    return (t[0].title(), " ".join(t[1:]).title()) if len(t)>1 else ("",t[0].title())

# ── Parcel lookup ────────────────────────────────────────────────────────────

class ParcelLookup:
    def __init__(self): self._idx: dict[str,dict] = {}

    def _norm(self, s): return re.sub(r"\s+"," ",str(s or "").upper().strip())

    def _add(self, parcel, owner):
        if not owner: return
        n = self._norm(owner)
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
            log.info("Found ZIP links: %s", zips)
            for url in zips:
                try:
                    log.info("Trying: %s", url)
                    r2 = requests.get(url, timeout=120); r2.raise_for_status()
                    count = self._load_zip(r2.content)
                    if count > 0:
                        log.info("Parcel index: %d entries", len(self._idx))
                        return self
                except Exception as e:
                    log.warning("ZIP failed: %s", e)
        except Exception as e:
            log.warning("FBCAD fetch failed: %s", e)
        log.warning("Parcel lookup unavailable — addresses will be empty")
        return self

    def _load_zip(self, raw):
        count = 0
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            names = zf.namelist()
            log.info("ZIP contents: %s", names)

            # DBF first
            for fn in names:
                if fn.lower().endswith(".dbf") and HAS_DBFREAD:
                    tmp = Path("/tmp/fbcad.dbf")
                    tmp.write_bytes(zf.read(fn))
                    try:
                        for rec in DBF(str(tmp), encoding="latin-1", ignore_missing_memofile=True):
                            d = dict(rec)
                            owner = next((str(d[k]).strip() for k in d
                                          if any(x in str(k).upper() for x in ("OWNER","OWN1"))), "")
                            def fc(*kws):
                                return next((str(d[k]).strip() for k in d
                                             if any(x in str(k).upper() for x in kws) and d[k]),"")
                            p = {"site_addr":fc("SITUS","SITE_ADDR","SITEADDR"),
                                 "site_city":fc("SITE_CITY","SITECITY"),
                                 "site_zip":fc("SITE_ZIP","SITEZIP"),
                                 "mail_addr":fc("ADDR_1","MAILADR","MAIL_ADDR"),
                                 "mail_city":fc("MAIL_CITY","MAILCITY","CITY"),
                                 "mail_state":fc("MAIL_STATE","MAILSTATE","STATE"),
                                 "mail_zip":fc("MAIL_ZIP","MAILZIP","ZIP")}
                            self._add(p, owner); count += 1
                        if count: return count
                    except Exception as e:
                        log.warning("DBF error: %s", e)

            # CSV fallback
            for fn in sorted(names, key=lambda x: -zf.getinfo(x).file_size):
                if not fn.lower().endswith(".csv"): continue
                try:
                    text = zf.read(fn).decode("latin-1", errors="replace")
                    reader = csv.DictReader(io.StringIO(text))
                    for row in reader:
                        owner = next((row[k] for k in row
                                      if any(x in k.upper() for x in ("OWNER","OWN1")) and row[k]),"")
                        def fc(*kws):
                            return next((str(row[k]).strip() for k in row
                                         if any(x in k.upper() for x in kws) and row[k]),"")
                        p = {"site_addr":fc("SITUS","SITE_ADDR","SITEADDR","PROPERTY_ADDR"),
                             "site_city":fc("SITE_CITY","SITECITY","PROP_CITY"),
                             "site_zip":fc("SITE_ZIP","SITEZIP","PROP_ZIP"),
                             "mail_addr":fc("MAIL_ADDR","MAILADR","ADDR_1","MAILING_ADDR"),
                             "mail_city":fc("MAIL_CITY","MAILCITY","MAILING_CITY"),
                             "mail_state":fc("MAIL_STATE","MAILSTATE","MAILING_STATE"),
                             "mail_zip":fc("MAIL_ZIP","MAILZIP","MAILING_ZIP")}
                        self._add(p, owner); count += 1
                    if count > 500:
                        log.info("Loaded %d from CSV: %s", count, fn)
                        return count
                except Exception as e:
                    log.warning("CSV error %s: %s", fn, e)
        return count

    def lookup(self, owner):
        n = self._norm(owner)
        if n in self._idx: return self._idx[n]
        t = n.split()
        if t:
            for k,v in self._idx.items():
                if k.startswith(t[0]): return v
        return None

# ── Scoring ──────────────────────────────────────────────────────────────────

def compute_score(rec):
    flags, score = [], 30
    cat, dtype = rec.get("cat",""), rec.get("doc_type","")
    amt, owner, filed = safe_float(rec.get("amount")), str(rec.get("owner","")), str(rec.get("filed",""))
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

# ── Clerk scraper (Playwright) ───────────────────────────────────────────────

async def scrape_clerk(start_date: str, end_date: str) -> list[dict]:
    """
    Fort Bend uses Kofile portal at ccweb.co.fort-bend.tx.us.
    We navigate to the OPR search, set date range + doc type, parse results.
    """
    if not HAS_PLAYWRIGHT:
        log.warning("Playwright not available"); return []

    records = []
    # Dates in MM/DD/YYYY format
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=HEADLESS)
        ctx = await browser.new_context(user_agent=(
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"))
        page = await ctx.new_page()

        # Load the search page first to establish session
        log.info("Loading clerk portal ...")
        try:
            await page.goto(CLERK_BASE+"/recorder/web/login.jsp",
                            wait_until="networkidle", timeout=30000)
        except Exception:
            pass
        try:
            await page.goto(CLERK_BASE+"/recorder/web/docSearch.jsp",
                            wait_until="networkidle", timeout=30000)
        except Exception:
            pass
        await asyncio.sleep(2)

        for doc_code in TARGET_CODES:
            log.info("Searching: %s", doc_code)
            rows = []
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    rows = await _kofile_search(page, doc_code, start_date, end_date)
                    break
                except Exception as e:
                    log.warning("  attempt %d failed: %s", attempt+1, e)
                    await asyncio.sleep(RETRY_DELAY*(attempt+1))
            records.extend(rows)

        await browser.close()
    log.info("Clerk done: %d records", len(records))
    return records


async def _kofile_search(page: Page, doc_code: str, start: str, end: str) -> list[dict]:
    results = []

    # Try navigating directly to search with params
    url = (f"{CLERK_BASE}/recorder/web/docSearch.jsp"
           f"?RecordType=OPR&searchType=DTR"
           f"&docType={doc_code}&beginDate={start}&endDate={end}&submit=Search")
    await page.goto(url, wait_until="domcontentloaded", timeout=45000)
    await asyncio.sleep(1)

    # If redirected to login, the portal needs session — try form submission
    if "login" in page.url.lower():
        # Try to find and use the search form
        await page.goto(CLERK_BASE+"/recorder/web/docSearch.jsp",
                        wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(1)

    content = await page.content()
    soup = BeautifulSoup(content, "lxml")

    # Look for results in any table
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 2: continue
        hdrs = [th.get_text(strip=True).lower() for th in rows[0].find_all(["th","td"])]
        # Check it's a results table
        if not any(h in " ".join(hdrs) for h in ["grantor","grantee","document","instrument"]):
            continue
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
            if rec: results.append(rec)

    log.info("  %s -> %d", doc_code, len(results))
    return results


def _make_record(raw: dict) -> dict | None:
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
        grantor = g("grantor","owner","from")
        grantee = g("grantee","to","buyer")
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
            link = f"{CLERK_BASE}/recorder/web/docSearch.jsp?doc={doc_num}" if doc_num else CLERK_BASE

        return {"doc_num":doc_num or "N/A","doc_type":code,"filed":fn,
                "cat":cat,"cat_label":label,"owner":grantor.strip(),
                "grantee":grantee.strip(),"amount":safe_float(amount),
                "legal":legal.strip(),"clerk_url":link,
                "prop_address":"","prop_city":"Fort Bend","prop_state":"TX","prop_zip":"",
                "mail_address":"","mail_city":"","mail_state":"TX","mail_zip":""}
    except Exception as e:
        log.debug("Record error: %s", e); return None

# ── Pipeline ─────────────────────────────────────────────────────────────────

def enrich(records, parcel):
    for rec in records:
        hit = parcel.lookup(rec.get("owner",""))
        if hit:
            rec.update({"prop_address":hit["site_addr"],"prop_city":hit["site_city"] or "Fort Bend",
                        "prop_state":"TX","prop_zip":hit["site_zip"],
                        "mail_address":hit["mail_addr"],"mail_city":hit["mail_city"],
                        "mail_state":hit["mail_state"] or "TX","mail_zip":hit["mail_zip"]})
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
    out_dir = Path(out_dir); out_dir.mkdir(parents=True,exist_ok=True)
    p = out_dir/f"ghl_export_{datetime.utcnow().strftime('%Y%m%d')}.csv"
    cols=["First Name","Last Name","Mailing Address","Mailing City","Mailing State","Mailing Zip",
          "Property Address","Property City","Property State","Property Zip",
          "Lead Type","Document Type","Date Filed","Document Number","Amount/Debt Owed",
          "Seller Score","Motivated Seller Flags","Source","Public Records URL"]
    with p.open("w",newline="",encoding="utf-8") as f:
        w=csv.DictWriter(f,fieldnames=cols); w.writeheader()
        for rec in records:
            fn,ln=parse_name(rec.get("owner",""))
            w.writerow({"First Name":fn,"Last Name":ln,
                "Mailing Address":rec.get("mail_address",""),"Mailing City":rec.get("mail_city",""),
                "Mailing State":rec.get("mail_state","TX"),"Mailing Zip":rec.get("mail_zip",""),
                "Property Address":rec.get("prop_address",""),"Property City":rec.get("prop_city","Fort Bend"),
                "Property State":rec.get("prop_state","TX"),"Property Zip":rec.get("prop_zip",""),
                "Lead Type":rec.get("cat_label",""),"Document Type":rec.get("doc_type",""),
                "Date Filed":rec.get("filed",""),"Document Number":rec.get("doc_num",""),
                "Amount/Debt Owed":rec.get("amount",""),"Seller Score":rec.get("score",0),
                "Motivated Seller Flags":" | ".join(rec.get("flags",[])),
                "Source":"Fort Bend County Clerk","Public Records URL":rec.get("clerk_url","")})
    log.info("CSV -> %s (%d rows)", p, len(records))

# ── Main ─────────────────────────────────────────────────────────────────────

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
    records = score_all(records)  # re-score after address enrichment

    payload = {"fetched_at":datetime.utcnow().isoformat()+"Z",
               "source":"Fort Bend County Clerk + FBCAD",
               "date_range":{"start":start_dt.strftime("%Y-%m-%d"),
                              "end":end_dt.strftime("%Y-%m-%d")},
               "total":len(records),
               "with_address":sum(1 for r in records if r.get("prop_address")),
               "records":records}

    write_json(payload, DASHBOARD_DIR/"records.json", DATA_DIR/"records.json")
    write_csv(records, DATA_DIR)
    log.info("Done. Total:%d  With address:%d", payload["total"], payload["with_address"])

if __name__=="__main__":
    asyncio.run(main())
