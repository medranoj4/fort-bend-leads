"""
Fort Bend County, TX — Motivated Seller Lead Scraper
Uses direct HTTP POST to ccweb.co.fort-bend.tx.us
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
CLERK_RESULTS = "http://ccweb.co.fort-bend.tx.us/RealEstate/SearchResults.aspx"
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
            owner_files = [n for n in names if "owner" in n.lower() and n.lower().endswith((".txt",".csv"))]
            prop_files  = [n for n in names if "property" in n.lower() and n.lower().endswith((".txt",".csv"))]
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

# ── Clerk scraper — direct HTTP POST ─────────────────────────────────────────
#
# The Fort Bend portal is a standard ASP.NET WebForms app.
# We use requests.Session to:
# 1. GET the search page to capture __VIEWSTATE etc
# 2. POST with doc type + date range fields
# 3. Parse the HTML results table
#
# From form inspection we know the real fields:
# - cphNoMargin_f_txtGrantor         = grantor name search
# - cphNoMargin_f_txtInstrumentNoFrom = instrument # from
# - cphNoMargin_f_txtInstrumentNoTo   = instrument # to
# - cphNoMargin_f_txtDataTextEdit1    = filed date FROM
# - cphNoMargin_f_txtLDLot            = legal lot
#
# For doc type + date search, the portal uses a DIFFERENT page:
# The "Document Type" tab uses __doPostBack to load a doc type picker
# then a date range. Let's use Playwright to dump the full
# SearchResults page HTML for one doc type so we can see what's there.

async def scrape_clerk(start_date: str, end_date: str) -> list[dict]:
    """
    Use Playwright to navigate the portal and dump page HTML.
    Then parse the actual results.
    """
    if not HAS_PLAYWRIGHT:
        return scrape_clerk_requests(start_date, end_date)

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
        await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)

        # Dump the FULL page HTML once so we can study it
        content = await page.content()
        log.info("Page title: %s | url: %s", await page.title(), page.url)

        # Log ALL links on the page (tabs, navigation)
        soup = BeautifulSoup(content, "lxml")
        links = [(a.get_text(strip=True)[:40], a.get("href","")[:80])
                 for a in soup.find_all("a", href=True)]
        log.info("ALL PAGE LINKS (%d): %s", len(links), links[:30])

        # Log ALL input fields
        inputs = [(i.get("name","?")[:40], i.get("id","?")[:40],
                   i.get("type","?"), i.get("value","?")[:30])
                  for i in soup.find_all("input")]
        log.info("ALL INPUTS (%d): %s", len(inputs), inputs[:30])

        # Now try the Document Type search tab via __doPostBack
        # Find doPostBack calls in page source
        postbacks = re.findall(r"__doPostBack\('([^']+)','([^']*)'\)", content)
        log.info("doPostBack calls (%d): %s", len(postbacks), postbacks[:15])

        # Try clicking the "Document Type" tab (it appeared in previous log line 20)
        # and then look for a date range form
        doc_type_tab_found = False
        for link_text, href in links:
            if "document type" in link_text.lower() or "doc type" in link_text.lower():
                log.info("Clicking doc type tab: %s / %s", link_text, href)
                try:
                    if "__doPostBack" in href:
                        await page.evaluate(f"javascript:{href}")
                    else:
                        await page.click(f"text={link_text}", timeout=5000)
                    await asyncio.sleep(2)
                    doc_type_tab_found = True
                    break
                except Exception as e:
                    log.warning("Tab click failed: %s", e)

        # After tab click, dump page again
        content2 = await page.content()
        soup2 = BeautifulSoup(content2, "lxml")
        log.info("After tab: title=%s url=%s", await page.title(), page.url)

        # Log all inputs again
        inputs2 = [(i.get("name","?")[:50], i.get("id","?")[:50],
                    i.get("type","?"), i.get("value","?")[:30])
                   for i in soup2.find_all("input")]
        log.info("INPUTS AFTER TAB (%d): %s", len(inputs2), inputs2)

        # Log all selects after tab
        for sel in soup2.find_all("select"):
            opts = [(o.get("value","")[:20], o.get_text(strip=True)[:25])
                    for o in sel.find_all("option")]
            log.info("SELECT after tab: id=%s name=%s opts=%s",
                     sel.get("id","?"), sel.get("name","?"), opts[:20])

        # Log all text content of the page (condensed)
        page_text = soup2.get_text(separator=" ", strip=True)
        log.info("PAGE TEXT (first 500 chars): %s", page_text[:500])

        # Try to find and use a date range search
        # Look for any inputs with "date" in name/id
        date_inputs = [(i.get("name",""), i.get("id",""))
                       for i in soup2.find_all("input")
                       if "date" in (i.get("name","") + i.get("id","")).lower()]
        log.info("DATE INPUTS: %s", date_inputs)

        # Also try: the "Filed Date Range" search which may be a separate tab
        # Let's try ALL doPostBack calls to find the right tab
        postbacks2 = re.findall(r"__doPostBack\('([^']+)','([^']*)'\)", content2)
        log.info("doPostBack after tab (%d): %s", len(postbacks2), postbacks2[:20])

        # ── Attempt actual searches ──
        # Strategy: use the grantor name field with a wildcard for broad search
        # combined with date range, then filter by doc type client-side
        # OR find the correct doc type + date search mechanism

        # First, let's try searching by doc type code in the grantor field
        # with the date range — this is a common workaround
        all_records = await _try_all_search_strategies(page, soup2, start_date, end_date)
        records.extend(all_records)

        await browser.close()

    log.info("Clerk done: %d records", len(records))
    return records


async def _try_all_search_strategies(page: Page, soup: BeautifulSoup,
                                      start: str, end: str) -> list[dict]:
    """Try multiple search approaches and return whatever works."""
    records = []

    content = await page.content()

    # Extract ASP.NET hidden fields
    vs   = soup.find("input", {"name":"__VIEWSTATE"})
    evv  = soup.find("input", {"name":"__EVENTVALIDATION"})
    vsg  = soup.find("input", {"name":"__VIEWSTATEGENERATOR"})

    vs_val  = vs.get("value","")  if vs  else ""
    evv_val = evv.get("value","") if evv else ""
    vsg_val = vsg.get("value","") if vsg else ""

    log.info("VIEWSTATE length: %d | EVENTVALIDATION length: %d",
             len(vs_val), len(evv_val))

    # Strategy: POST directly to SearchResults with doc type + date range
    # The field names from the form:
    # cphNoMargin_f_txtDataTextEdit1 = might be a date field
    # Let's try using requests to POST the form

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0",
        "Referer": CLERK_SEARCH,
    })

    # First GET to establish session
    try:
        r = session.get(CLERK_SEARCH, timeout=20)
        search_soup = BeautifulSoup(r.text, "lxml")
        vs2   = search_soup.find("input", {"name":"__VIEWSTATE"})
        evv2  = search_soup.find("input", {"name":"__EVENTVALIDATION"})
        vsg2  = search_soup.find("input", {"name":"__VIEWSTATEGENERATOR"})
        vs_val  = vs2.get("value","")  if vs2  else vs_val
        evv_val = evv2.get("value","") if evv2 else evv_val
        vsg_val = vsg2.get("value","") if vsg2 else vsg_val
        log.info("HTTP session established. VS=%d chars", len(vs_val))

        # Log ALL input fields from fresh GET
        all_fields = {i.get("name",""): i.get("value","")
                      for i in search_soup.find_all("input") if i.get("name")}
        log.info("Form fields from GET: %s", list(all_fields.keys())[:30])

        # Log all text on search page to find date field hints
        page_text = search_soup.get_text(separator="|", strip=True)
        log.info("Search page text (first 300): %s", page_text[:300])

    except Exception as e:
        log.warning("HTTP GET failed: %s", e)

    # Try searching for each doc type using the grantor/name field
    # with a broad search + date range POST
    for doc_code in TARGET_CODES:
        log.info("Searching: %s", doc_code)
        found = await _playwright_doc_search(page, doc_code, start, end)
        records.extend(found)

    return records


async def _playwright_doc_search(page: Page, doc_code: str,
                                  start: str, end: str) -> list[dict]:
    """
    Search for a specific doc type using Playwright.
    We navigate back to the search page each time and try to use
    the correct search fields.
    """
    results = []

    try:
        await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(1)

        content = await page.content()
        soup = BeautifulSoup(content, "lxml")

        # Try to find and use a "Document Type" search
        # The portal may have a doc type field under a specific tab
        # Let's try clicking each tab and looking for doc type + date fields

        # Find ALL clickable tabs/links
        tab_links = []
        for a in soup.find_all("a", href=True):
            txt = a.get_text(strip=True).lower()
            href = a.get("href","")
            if "__dopostback" in href.lower() or "javascript" in href.lower():
                tab_links.append((a.get_text(strip=True), href))

        # Try each tab to find one with date range + doc type
        for tab_name, tab_href in tab_links[:10]:
            try:
                log.info("  Trying tab: %s", tab_name)
                if "__doPostBack" in tab_href or "doPostBack" in tab_href:
                    await page.evaluate(f"javascript:{tab_href}")
                else:
                    await page.goto(CLERK_BASE + tab_href, wait_until="domcontentloaded", timeout=10000)
                await asyncio.sleep(1)

                new_content = await page.content()
                new_soup = BeautifulSoup(new_content, "lxml")

                # Check for date inputs and doc type selects
                date_ins = [i for i in new_soup.find_all("input")
                            if "date" in (i.get("name","") + i.get("id","")).lower()]
                doc_sels = [s for s in new_soup.find_all("select")
                            if any(v in [o.get("value","") for o in s.find_all("option")]
                                   for v in TARGET_CODES)]

                if date_ins or doc_sels:
                    log.info("  TAB '%s' HAS date_inputs=%d doc_selects=%d",
                             tab_name, len(date_ins), len(doc_sels))

                    # Try to fill doc type and dates
                    for ds in doc_sels:
                        sel_id = ds.get("id") or ds.get("name")
                        if sel_id:
                            try:
                                await page.select_option(
                                    f"#{sel_id},[name='{sel_id}']",
                                    value=doc_code, timeout=3000)
                            except: pass

                    for di in date_ins[:2]:
                        di_id = di.get("id") or di.get("name")
                        if di_id:
                            val = start if date_ins.index(di) == 0 else end
                            try:
                                await page.fill(f"#{di_id},[name='{di_id}']", val)
                            except: pass

                    # Click search
                    for btn_sel in ["input[value='Search']",
                                    "#cphNoMargin_SearchButtons1_btnSearch",
                                    "input[id*='btnSearch']"]:
                        try:
                            btn = await page.query_selector(btn_sel)
                            if btn:
                                await btn.click()
                                break
                        except: pass

                    await asyncio.sleep(2)
                    result_content = await page.content()
                    result_soup = BeautifulSoup(result_content, "lxml")
                    found = _parse_results(result_soup, doc_code)
                    if found:
                        log.info("  %s: found %d via tab '%s'", doc_code, len(found), tab_name)
                        results.extend(found)
                        break

            except Exception as e:
                log.debug("  Tab '%s' error: %s", tab_name, e)
                try:
                    await page.goto(CLERK_SEARCH, wait_until="domcontentloaded", timeout=10000)
                    await asyncio.sleep(1)
                except: pass

        # If no results from tabs, try direct URL with doc type as grantor search
        if not results:
            results = await _try_direct_search(page, doc_code, start, end)

    except Exception as e:
        log.warning("  %s search error: %s", doc_code, e)

    log.info("  %s -> %d", doc_code, len(results))
    return results


async def _try_direct_search(page: Page, doc_code: str,
                              start: str, end: str) -> list[dict]:
    """
    Try direct URL approaches for the Fort Bend portal.
    The portal may support URL parameters or a specific POST format.
    """
    results = []

    # The portal is ASP.NET — try POSTing directly with requests
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/124.0",
    })

    for attempt in range(2):
        try:
            # GET search page for fresh hidden fields
            r = session.get(CLERK_SEARCH, timeout=15)
            soup = BeautifulSoup(r.text, "lxml")

            hidden = {i["name"]: i.get("value","")
                      for i in soup.find_all("input", {"type":"hidden"})
                      if i.get("name")}

            # Log form structure once
            if doc_code == "LP" and attempt == 0:
                all_selects = {s.get("name","?"):
                               [o.get("value","") for o in s.find_all("option")]
                               for s in soup.find_all("select")}
                all_text_inputs = [(i.get("name",""), i.get("id",""))
                                   for i in soup.find_all("input", {"type":"text"})]
                log.info("REQUESTS - Selects: %s", all_selects)
                log.info("REQUESTS - Text inputs: %s", all_text_inputs)
                log.info("REQUESTS - Hidden fields: %s", list(hidden.keys()))

            # Build POST payload — try different field name combinations
            # Based on form fields seen: txtDataTextEdit1 is likely a date field
            payload = {
                **hidden,
                "ctl00$cphNoMargin$SearchButtons1$btnSearch": "Search",
            }

            # Try different date/doctype field combinations
            date_field_combos = [
                # (start_field, end_field, doctype_field, doctype_value)
                ("ctl00$cphNoMargin$f$txtDataTextEdit1",
                 "ctl00$cphNoMargin$f$txtDataTextEdit1",
                 None, None),
            ]

            # Find actual date/doc type fields by looking at all inputs
            for inp in soup.find_all("input", {"type":"text"}):
                name = inp.get("name","").lower()
                if "date" in name or "filed" in name or "from" in name:
                    log.info("DATE FIELD FOUND: %s", inp.get("name",""))

            r2 = session.post(CLERK_SEARCH, data=payload, timeout=20)
            result_soup = BeautifulSoup(r2.text, "lxml")
            found = _parse_results(result_soup, doc_code)
            if found:
                results.extend(found)
                log.info("  %s: %d via HTTP POST", doc_code, len(found))
            break
        except Exception as e:
            log.warning("  HTTP POST attempt %d failed: %s", attempt+1, e)
            time.sleep(2)

    return results


def _parse_results(soup: BeautifulSoup, doc_code: str) -> list[dict]:
    """Parse any results table from the portal HTML."""
    results = []
    JUNK = ["get a free copy","sort by","results list","new search","refine search",
            "0records","please enter","logon","login","basket","criteria","click here",
            "search ins","combined name","clear form","june 2026","may 2026",
            "july 2026","april 2026","march 2026","select all","view basket"]

    for tbl in soup.find_all("table"):
        rows = tbl.find_all("tr")
        if len(rows) < 2: continue

        hdrs = [th.get_text(strip=True).lower()
                for th in rows[0].find_all(["th","td"])]
        hdr_str = " ".join(hdrs)

        if not any(x in hdr_str for x in
                   ["inst","grantor","filed","consideration","book","grantee"]):
            continue
        if any(j in hdr_str for j in JUNK):
            continue

        log.info("  Results table: hdrs=%s rows=%d", hdrs[:8], len(rows)-1)

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
            if rec: results.append(rec)

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


def scrape_clerk_requests(start_date: str, end_date: str) -> list[dict]:
    """Fallback: HTTP-only scrape."""
    return []

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
