"""
Fort Bend County, TX — Motivated Seller Lead Scraper
Clerk: ccweb.co.fort-bend.tx.us/RealEstate/
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

CLERK_BASE    = "http://ccweb.co.fort-bend.tx.us"
CLERK_SEARCH  = "http://ccweb.co.fort-bend.tx.us/RealEstate/SearchEntry.aspx"
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

            def score_zip(url):
                u = url.lower()
                s = 0
                if "2025" in u: s += 100
                if "2024" in u: s += 50
                if "supplement" in u and "redact" not in u: s += 30
                if any(x in u for x in ("segment","commercial","residential","sales",
                                         "improvement","land","grandtotal","prelim",
                                         "agent","entity","exemption","mobile")): s -= 50
                return s

            for url in sorted(zips, key=score_zip, reverse=True)[:8]:
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
        return self

    def _load_zip(self, raw):
        count = 0
        with zipfile.ZipFile(io.BytesIO(raw)) as zf:
            names = zf.namelist()
            owner_files = [n for n in names if "owner" in n.lower()
                           and n.lower().endswith((".txt",".csv"))]
            prop_files  = [n for n in names if "property" in n.lower()
                           and n.lower().endswith((".txt",".csv"))]
            if not owner_files: return 0

            addr_map: dict[str,dict] = {}
            if prop_files:
                try:
                    raw_csv = zf.read(prop_files[0]).decode("latin-1",errors="replace")
                    delim = "\t" if raw_csv.split("\n")[0].count("\t") > raw_csv.split("\n")[0].count(",") else ","
                    for row in csv.DictReader(io.StringIO(raw_csv), delimiter=delim):
                        acct = row.get("PropertyQuickRefID","") or row.get("PropertyNumber","")
                        if acct:
                            addr_map[str(acct).strip()] = {
                                "site_addr": row.get("SitusStreetAddress","").strip(),
                                "site_city": row.get("SitusCity","").strip(),
                                "site_zip":  row.get("SitusZip","").strip(),
                            }
                except Exception as e:
                    log.warning("Property file error: %s", e)

            try:
                raw_csv = zf.read(owner_files[0]).decode("latin-1",errors="replace")
                delim = "\t" if raw_csv.split("\n")[0].count("\t") > raw_csv.split("\n")[0].count(",") else ","
                for row in csv.DictReader(io.StringIO(raw_csv), delimiter=delim):
                    owner = row.get("OwnerName","").strip()
                    if not owner: continue
                    acct  = str(row.get("PropertyQuickRefID","")).strip()
                    site  = addr_map.get(acct,{})
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
# The Fort Bend portal SearchEntry.aspx has these search tabs:
# 1. Name Search (grantor/grantee by name)
# 2. Instrument # Range search
# 3. Legal Description search
# 4. Date Filed Range by Doc Type  <-- THIS IS WHAT WE NEED
#
# The "Date Filed" search by doc type is on a DIFFERENT tab.
# From the form fields we see "txtLDLot", "txtLDBook" etc = Legal Description tab
# The doc type + date range search uses __doPostBack to switch tabs.
# Table 12 showed "ABANDONMENT" with 86 rows = that IS the doc type list!
# We need to click the correct tab first, then select doc type + dates.

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
            resp = await page.goto(CLERK_SEARCH,
                                   wait_until="domcontentloaded", timeout=30000)
            log.info("Page loaded: %d | %s", resp.status if resp else 0, await page.title())
        except Exception as e:
            log.warning("Load failed: %s", e)
            await browser.close(); return []

        await asyncio.sleep(2)

        # Find and click the "Document Type" or "Filed Date" search tab
        # The portal uses tabs/links to switch search modes
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        # Log all links and buttons to find the tab
        tab_found = False
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            href = a.get("href","")
            if any(x in txt for x in ("document type","doc type","filed date",
                                        "instrument type","date filed")):
                log.info("Found tab link: text=%s href=%s", txt, href[:80])
                try:
                    await page.click(f"text={a.get_text(strip=True)}", timeout=5000)
                    await asyncio.sleep(1)
                    tab_found = True
                    log.info("Clicked tab: %s", a.get_text(strip=True))
                    break
                except: pass

        # Also try clicking tabs by looking for tab-like elements
        if not tab_found:
            for tab_text in ["Document Type", "Doc Type", "Filed Date",
                              "Date Filed", "Instrument Type", "Type"]:
                try:
                    el = await page.query_selector(f"text={tab_text}")
                    if el:
                        await el.click(timeout=3000)
                        await asyncio.sleep(1)
                        tab_found = True
                        log.info("Clicked tab by text: %s", tab_text)
                        break
                except: pass

        # Re-read page after potential tab click
        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        # Log ALL select elements and their options to find doc type dropdown
        log.info("=== ALL SELECTS AFTER TAB CLICK ===")
        for sel in soup.find_all("select"):
            opts = [(o.get("value",""), o.get_text(strip=True)[:25])
                    for o in sel.find_all("option")]
            log.info("SELECT id=%s name=%s | %d opts | first=%s",
                     sel.get("id","?"), sel.get("name","?"),
                     len(opts), opts[:5])

        # Log table with doc types (Table 12 had ABANDONMENT)
        for i, tbl in enumerate(soup.find_all("table")):
            rows = tbl.find_all("tr")
            if len(rows) > 10:
                first_vals = [td.get_text(strip=True)[:20]
                              for td in rows[1].find_all("td")]
                if any(x in " ".join(first_vals).upper()
                       for x in ["LP","LIEN","DEED","PENDENS","FORE"]):
                    log.info("DOC TYPE TABLE at index %d: sample=%s rows=%d",
                             i, first_vals[:5], len(rows))

        for doc_code in TARGET_CODES:
            log.info("Searching: %s", doc_code)
            rows = []
            for attempt in range(RETRY_ATTEMPTS):
                try:
                    rows = await _doc_type_date_search(
                        page, doc_code, start_date, end_date)
                    break
                except Exception as e:
                    log.warning("  attempt %d: %s", attempt+1, e)
                    await asyncio.sleep(RETRY_DELAY*(attempt+1))
                    try:
                        await page.goto(CLERK_SEARCH,
                                        wait_until="domcontentloaded", timeout=20000)
                        await asyncio.sleep(2)
                    except: pass
            records.extend(rows)

        await browser.close()
    log.info("Clerk done: %d records", len(records))
    return records


async def _doc_type_date_search(page: Page, doc_code: str,
                                 start: str, end: str) -> list[dict]:
    results = []

    # Strategy: use the Document Type search tab
    # From form inspection we know:
    # - txtInstrumentNoFrom / txtInstrumentNoTo = instrument range
    # - The doc type list table (Table 12) has clickable doc type rows
    # - Date fields: look for filed date inputs

    content = await page.content()
    soup = BeautifulSoup(content, "lxml")

    # Find doc type in the list table (Table 12 "ABANDONMENT" style)
    # These are clickable links/rows in a table
    doc_type_clicked = False

    # Look for a table containing document type codes
    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 5: continue
        # Check if this table has doc type codes
        all_text = " ".join(td.get_text(strip=True) for tr in rows
                            for td in tr.find_all("td"))
        if doc_code in all_text or any(x in all_text for x in
                                        ["LIS PENDENS","FORECLOSURE","JUDGMENT","LIEN"]):
            # Find the specific row with our doc code
            for tr in rows:
                cells = tr.find_all("td")
                row_text = " ".join(c.get_text(strip=True) for c in cells)
                if doc_code in row_text.split() or doc_code == row_text.strip():
                    a = tr.find("a", href=True)
                    if a:
                        try:
                            href = a["href"]
                            if not href.startswith("http"):
                                href = CLERK_BASE + "/" + href.lstrip("/")
                            await page.goto(href, wait_until="domcontentloaded",
                                            timeout=15000)
                            await asyncio.sleep(1)
                            doc_type_clicked = True
                            log.info("  Clicked doc type link: %s", doc_code)
                            break
                        except: pass
                    # Try clicking the row itself
                    if not doc_type_clicked:
                        try:
                            await page.click(f"text={doc_code}", timeout=3000)
                            await asyncio.sleep(1)
                            doc_type_clicked = True
                            break
                        except: pass
            if doc_type_clicked:
                break

    # After selecting doc type, fill in date range
    content = await page.content()
    soup2 = BeautifulSoup(content, "lxml")

    # Find date fields
    date_fields = []
    for inp in soup2.find_all("input"):
        iid = (inp.get("id","") + inp.get("name","")).lower()
        itype = inp.get("type","text").lower()
        if itype in ("text","") and any(x in iid for x in
                                         ("date","filed","from","begin","start","to","end","thru")):
            date_fields.append((inp.get("id") or inp.get("name"), iid))

    log.info("  %s: doc_clicked=%s date_fields=%s",
             doc_code, doc_type_clicked, date_fields[:4])

    # Fill dates
    filled_start = filled_end = False
    for fid, flow in date_fields:
        if not fid: continue
        if not filled_start and any(x in flow for x in
                                     ("from","begin","start","date1","filed1")):
            try:
                await page.fill(f"#{fid}, [name='{fid}']", start)
                filled_start = True
            except: pass
        elif not filled_end and any(x in flow for x in
                                     ("to","end","thru","date2","filed2")):
            try:
                await page.fill(f"#{fid}, [name='{fid}']", end)
                filled_end = True
            except: pass

    # Click search
    for btn_sel in ["#cphNoMargin_SearchButtons1_btnSearch",
                    "input[value='Search']","input[id*='btnSearch']",
                    "button:has-text('Search')"]:
        try:
            btn = await page.query_selector(btn_sel)
            if btn:
                await btn.click()
                log.info("  Clicked search")
                break
        except: pass

    await asyncio.sleep(2)
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except: pass

    # Parse results
    content = await page.content()
    soup3 = BeautifulSoup(content, "lxml")
    log.info("  Result URL: %s", page.url)

    results = _parse_results_page(soup3, doc_code)

    # Go back
    try:
        await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=15000)
        await asyncio.sleep(1)
    except: pass

    log.info("  %s -> %d", doc_code, len(results))
    return results


def _parse_results_page(soup: BeautifulSoup, doc_code: str) -> list[dict]:
    """
    Parse the SearchResults.aspx page.
    The real data table has columns like:
    Inst #  |  Book  |  Page  |  Filed  |  Doc Type  |  Grantor  |  Grantee  |  Legal  |  Consideration
    """
    results = []
    JUNK = ["get a free copy","sort by","results list","new search","refine search",
            "0records","please enter","logon","login","basket","criteria","click here",
            "search ins","combined name","clear form","june 2026","may 2026","july 2026"]

    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 2: continue

        hdrs = [th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th","td"])]
        hdr_str = " ".join(hdrs)

        # Must have instrument/grantor type columns
        if not any(x in hdr_str for x in
                   ["inst","grantor","filed","consideration","grantee","book"]):
            continue

        # Skip if first header looks like junk
        if any(j in hdr_str for j in JUNK):
            continue

        log.info("  Parsing table hdrs=%s rows=%d", hdrs[:8], len(rows))

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

            raw["_code"] = doc_code
            rec = _make_record(raw)
            if rec:
                results.append(rec)

    return results


def _make_record(raw):
    try:
        code = raw.get("_code","")
        cat, label = DOC_TYPE_MAP.get(code,(code,code))

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

        doc_num = g("inst #","inst#","instrument","instno","inst no","c0")
        filed   = g("filed","date filed","c3","c4")
        grantor = g("grantor","c5","c6")
        grantee = g("grantee","c6","c7")
        legal   = g("legal","c7","c8")
        amount  = g("consideration","amount","c8","c9")
        link    = gh("inst","view","c0")

        doc_num = (doc_num or "").strip()
        if not doc_num or len(doc_num) > 30: return None

        # Must look like a real instrument number (mostly digits/letters, short)
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
