import os, io, json, tarfile, re
import requests
from lxml import etree
from datetime import datetime, date

from dotenv import load_dotenv
load_dotenv()

from google.oauth2 import service_account
from googleapiclient.discovery import build


# =========================
# CONFIG
# =========================
BASE = "https://api.lovdata.no/v1"
LAWS_PKG = "gjeldende-lover.tar.bz2"
REGS_PKG = "gjeldende-sentrale-forskrifter.tar.bz2"

SHEET_ID = os.environ.get("GOOGLE_SHEET_ID")
TAB_NAME = "Norway Trial"
LIMIT = 800


# =========================
# UTILS
# =========================
def log(msg=""):
    print(msg, flush=True)

def first_text_any(root, names):
    """
    Find first non-empty text among any tag local-names in `names`.
    Tries exact name and lowercase name to be robust to casing.
    """
    for n in names:
        for cand in (n, n.lower()):
            els = root.xpath(f".//*[local-name()='{cand}']")
            for el in els:
                if el is not None and el.text and el.text.strip():
                    return el.text.strip()
    return ""

def all_text_any(root, names):
    out = []
    for n in names:
        for cand in (n, n.lower()):
            els = root.xpath(f".//*[local-name()='{cand}']")
            for el in els:
                if el is not None and el.text and el.text.strip():
                    out.append(el.text.strip())
    return out

def parse_iso_date(d):
    if not d:
        return None
    try:
        return datetime.strptime(d.strip(), "%Y-%m-%d").date()
    except Exception:
        return None

def any_future_date(dates_list):
    for d in dates_list:
        dd = parse_iso_date(d)
        if dd and dd > date.today():
            return True
    return False

def any_past_or_today_date(dates_list):
    for d in dates_list:
        dd = parse_iso_date(d)
        if dd and dd <= date.today():
            return True
    return False


# Norwegian month names for "Fra 1. januar 2026"
MONTH_MAP = {
    "januar": "01",
    "februar": "02",
    "mars": "03",
    "april": "04",
    "mai": "05",
    "juni": "06",
    "juli": "07",
    "august": "08",
    "september": "09",
    "oktober": "10",
    "november": "11",
    "desember": "12",
}

def find_effective_dates_in_text(text_full):
    """
    Pull effective dates from Norwegian free text.
    Returns list of ISO dates YYYY-MM-DD.
    Handles missing whitespace like "I kraft fra2025-10-01".
    Also supports multiple dates separated by comma.
    Accepts both Bokmål "fra" and Nynorsk "frå".
    """
    found = []

    for m in re.finditer(
        r"I\s*kraft\s*(?:fra|frå)\s*(\d{4}-\d{2}-\d{2})",
        text_full,
        flags=re.IGNORECASE
    ):
        found.append(m.group(1))

    for m in re.finditer(
        r"I\s*kraft\s*(?:fra|frå)\s*((?:\d{4}-\d{2}-\d{2})(?:\s*,\s*\d{4}-\d{2}-\d{2})+)",
        text_full,
        flags=re.IGNORECASE
    ):
        chunk = m.group(1)
        for iso in re.findall(r"\d{4}-\d{2}-\d{2}", chunk):
            found.append(iso)

    for m in re.finditer(
        r"Fra\s+(\d{1,2})\.\s*([A-Za-zæøåÆØÅ]+)\s+(\d{4})",
        text_full,
        flags=re.IGNORECASE
    ):
        day = m.group(1).zfill(2)
        month_name = m.group(2).lower()
        year = m.group(3)

        mm = MONTH_MAP.get(month_name)
        if mm:
            found.append(f"{year}-{mm}-{day}")

    return found


def text_says_effective_date_not_fixed(text_full: str) -> bool:
    """
    Detect Norwegian formulations meaning not yet in force because
    entry-into-force is set later by the King / not fixed.
    """
    t = (text_full or "").lower()

    patterns = [
        r"i\s*kraft\s*(?:fra|frå)\s*kongen\s*fastset",
        r"i\s*kraft\s*(?:fra|frå)\s*kongen\s*fastsetter",
        r"(?:frå|fra)\s*den\s*tid\s*kongen\s*fastset",
        r"(?:frå|fra)\s*den\s*tid\s*kongen\s*fastsetter",
        r"(?:frå|fra)\s*den\s*tid\s*kongen\s*bestemmer",
        r"kongen\s*bestemmer",
        r"kongen\s*fastset",
        r"kongen\s*fastsetter",
    ]

    for p in patterns:
        if re.search(p, t):
            return True
    return False


# =========================
# LOVDATA DOWNLOAD/EXTRACT
# =========================
def download_pkg(filename):
    url = f"{BASE}/publicData/get/{filename}"
    log(f"[download_pkg] GET {url}")
    r = requests.get(url, timeout=300)
    r.raise_for_status()
    log(f"[download_pkg] downloaded {len(r.content)} bytes for {filename}")
    return r.content

def extract_tar_bz2(blob, label):
    log(f"[extract_tar_bz2] extracting {label} ...")
    tar = tarfile.open(fileobj=io.BytesIO(blob), mode="r:bz2")
    members = tar.getmembers()
    log(f"[extract_tar_bz2] {label} tar members: {len(members)}")

    files = {}
    for m in members:
        if m.isfile() and m.name.endswith(".xml"):
            files[m.name] = tar.extractfile(m).read()

    log(f"[extract_tar_bz2] {label} xml files extracted: {len(files)}")
    return files


# =========================
# FILENAME-BASED IDS/URLS
# =========================
def derive_date_and_suffix_from_filename(filename: str, prefix: str):
    """
    prefix: "nl" or "sf"
    expects: nl-YYYYMMDD-<num>.xml or sf-YYYYMMDD-<num>.xml
    <num> can be 1+ digits (e.g., 003, 44, 1413).
    returns (YYYY-MM-DD, "<num_str>")
    """
    if not filename:
        return ("", "")

    fn = filename.lower().split("/")[-1]
    m = re.search(rf"{prefix}-(\d{{8}})-(\d+)\.xml$", fn)
    if not m:
        return ("", "")

    yyyymmdd = m.group(1)
    suffix_str = m.group(2)
    yyyy, mm, dd = yyyymmdd[:4], yyyymmdd[4:6], yyyymmdd[6:8]
    date_iso = f"{yyyy}-{mm}-{dd}"
    return (date_iso, suffix_str)


def derive_law_id_from_filename(filename: str):
    date_iso, suffix_str = derive_date_and_suffix_from_filename(filename, "nl")
    if not date_iso:
        return ""

    base = f"lov/{date_iso}"
    if suffix_str and int(suffix_str) != 0:
        n = str(int(suffix_str))
        return f"{base}-{n}"
    return base


def build_public_law_url(doc_id: str = "", filename: str = ""):
    """
    Always prefer filename-derived pattern:
    https://lovdata.no/dokument/NL/lov/YYYY-MM-DD(-n)
    """
    if filename:
        ref = derive_law_id_from_filename(filename)
        if ref:
            return f"https://lovdata.no/dokument/NL/{ref}"

    if doc_id:
        doc_id = doc_id.strip().replace(" ", "")
        if doc_id.startswith("NL/lov/"):
            return f"https://lovdata.no/dokument/{doc_id}"
        if doc_id.startswith("lov/"):
            return f"https://lovdata.no/dokument/NL/{doc_id}"
        if doc_id.startswith("NL/"):
            return f"https://lovdata.no/dokument/NL/{doc_id}"
        return f"https://lovdata.no/dokument/NL/{doc_id}"

    return ""


def derive_reg_id_from_filename(filename: str):
    """
    sf/sf-YYYYMMDD-<num>.xml -> forskrift/YYYY-MM-DD(-n)
    """
    date_iso, suffix_str = derive_date_and_suffix_from_filename(filename, "sf")
    if not date_iso:
        return ""

    base = f"forskrift/{date_iso}"
    if suffix_str and int(suffix_str) != 0:
        n = str(int(suffix_str))
        return f"{base}-{n}"
    return base


def derive_reg_date_from_filename(filename: str):
    date_iso, _ = derive_date_and_suffix_from_filename(filename, "sf")
    return date_iso


def build_public_reg_url(doc_id: str = "", filename: str = ""):
    """
    Filename-derived regulation URL:
    https://lovdata.no/dokument/SF/forskrift/YYYY-MM-DD(-n)
    """
    if filename:
        rid = derive_reg_id_from_filename(filename)
        if rid:
            return f"https://lovdata.no/dokument/SF/{rid}"

    if not doc_id:
        return ""
    doc_id = doc_id.strip()

    if doc_id.startswith("SF/"):
        return f"https://lovdata.no/dokument/{doc_id}"
    if doc_id.startswith("sf/"):
        return f"https://lovdata.no/dokument/SF/{doc_id}"
    if doc_id.startswith("forskrift/"):
        return f"https://lovdata.no/dokument/SF/{doc_id}"

    return f"https://lovdata.no/dokument/{doc_id}"


# =========================
# PARSING: LAWS
# =========================
def parse_law_xml(xml_bytes):
    root = etree.fromstring(xml_bytes)

    title = first_text_any(root, ["title", "Titel", "Tittel"])
    short_title = first_text_any(root, ["shortTitle", "Korttittel"])

    dok_id = first_text_any(root, ["dokID", "DokumentID"])
    ref_id = first_text_any(root, ["refID", "RefID"])
    any_id = first_text_any(root, ["id"])
    chosen_id = dok_id or ref_id or any_id

    date_promulgated = first_text_any(root, ["datePromulgated", "Datokode", "datokode"])
    corrected_date   = first_text_any(root, ["correctedDate"])
    last_amended_by  = first_text_any(root, ["lastAmendedBy", "Sist endret ved", "Sist endra ved"])
    last_amended_ifr = first_text_any(root, ["lastAmendedInForceFrom", "Ikrafttredelse av siste endring", "Ikrafttreding av siste endring"])

    department       = first_text_any(root, ["department", "Departement"])
    legal_area       = first_text_any(root, ["legalArea", "Rettsområde"])
    document_note    = first_text_any(root, ["documentNote", "Annet om dokumentet", "Anna om dokumentet"])
    content_heading  = first_text_any(root, ["contentHeading", "Innhold"])
    access_removed_date = first_text_any(root, ["accessRemovedDate", "access_removedDate"])

    in_force_raw = first_text_any(root, ["inForce", "Ikke i kraft", "Ikkje i kraft", "Ikrafttredelse", "Ikrafttreding"])

    text_full = etree.tostring(root, encoding="unicode", method="text")
    text_lc = (text_full or "").lower()

    # -------------------------
    # 1) EFFECTIVE CANDIDATES
    # -------------------------
    # (A) POSITIVE entry-into-force tags only (exclude last amendment)
    positive_tag_candidates = all_text_any(
        root,
        [
            "inForceFrom", "effectiveFrom", "ikrafttredelse",
            "ikraftFra", "ikraftFraDato", "iKraftFra", "ikrafttredelseDato"
        ]
    )

    # (B) POSITIVE entry-into-force from free text
    positive_text_candidates = find_effective_dates_in_text(text_full)

    positive_candidates = positive_tag_candidates + positive_text_candidates
    positive_candidates = list(dict.fromkeys([c.strip() for c in positive_candidates if c and c.strip()]))

    # (C) Keep a full list for debugging (includes last amendment etc.)
    effective_candidates = positive_candidates[:]
    effective_candidates.extend(all_text_any(
        root,
        ["lastAmendedInForceFrom", "Ikrafttredelse av siste endring", "Ikrafttreding av siste endring"]
    ))
    effective_candidates = list(dict.fromkeys([c.strip() for c in effective_candidates if c and c.strip()]))

    # -------------------------
    # 2) EXPLICIT NOT-IN-FORCE DETECTION (Gate 0)
    # -------------------------
    explicit_not_in_force = (
        "ikke i kraft" in text_lc or
        "ikkje i kraft" in text_lc or
        (in_force_raw or "").lower().strip() in ("false", "0", "no", "nei")
    )

    # -------------------------
    # 3) STATUS LOGIC
    # -------------------------
    # Gate 0: explicit "not in force" wins unless a real positive in-force date <= today exists
    if explicit_not_in_force and not any_past_or_today_date(positive_candidates):
        status = "not_in_force"
        reason = "explicit ikke/ikkje i kraft and no past/today positive entry-into-force date"

    # Gate 1: any positive effective date in the future -> future
    elif any_future_date(positive_candidates):
        status = "future"
        reason = "future positive entry-into-force date found (tags or text)"

    # Gate 2: "Kongen fastsetter/bestemmer" with no positive past effective date -> future
    elif text_says_effective_date_not_fixed(text_full) and not any_past_or_today_date(positive_candidates):
        status = "future"
        reason = "entry into force not fixed (Kongen fastsetter/bestemmer)"

    # Gate 3: any positive past/today effective date -> in force
    elif any_past_or_today_date(positive_candidates):
        status = "in_force"
        reason = "positive entry-into-force date <= today found (tags or text)"

    else:
        raw_lc = (in_force_raw or "").lower().strip()

        raw_has_not_in_force = (
            "ikke i kraft" in raw_lc or "ikkje i kraft" in raw_lc
        )
        raw_has_in_force = (
            raw_lc in ("true", "1", "yes", "ja") or
            ("i kraft" in raw_lc and "ikke" not in raw_lc and "ikkje" not in raw_lc)
        )

        if raw_has_not_in_force or (access_removed_date or "").strip():
            status = "not_in_force"
            reason = "raw inForce indicates not in force / accessRemovedDate"
        elif raw_has_in_force:
            status = "in_force"
            reason = "raw inForce indicates in force"
        else:
            status = "ambiguous"
            reason = "no clear inForce + no effective date match"

    return {
        "title": title,
        "shortTitle": short_title,
        "dokID": dok_id,
        "refID": ref_id,
        "id": chosen_id,
        "datePromulgated": date_promulgated,
        "correctedDate": corrected_date,
        "lastAmendedBy": last_amended_by,
        "lastAmendedInForceFrom": last_amended_ifr,
        "department": department,
        "legalArea": legal_area,
        "documentNote": document_note,
        "contentHeading": content_heading,
        "inForceRaw": in_force_raw,
        "effectiveCandidates": effective_candidates,   # debug: full list
        "positiveCandidates": positive_candidates,     # debug: real entry-into-force only
        "accessRemovedDate": access_removed_date,
        "status": status,
        "reason": reason,
    }


# =========================
# PARSING: REGULATIONS
# =========================
def normalize_reg_datokode(datokode: str) -> str:
    """
    Datokode often like: FOR-2019-10-24-1413
    Return ISO date YYYY-MM-DD if present.
    """
    if not datokode:
        return ""
    m = re.search(r"(\d{4}-\d{2}-\d{2})", datokode)
    return m.group(1) if m else ""

def parse_reg_xml(xml_bytes):
    root = etree.fromstring(xml_bytes)

    title = first_text_any(root, ["title", "Titel", "Tittel"])
    short_title = first_text_any(root, ["shortTitle", "Korttittel"])

    dok_id = first_text_any(root, ["dokID", "DokumentID", "dokumentID"])
    ref_id = first_text_any(root, ["refID", "RefID"])
    any_id = first_text_any(root, ["id"])
    chosen_id = dok_id or ref_id or any_id

    datokode_raw = first_text_any(root, ["datePromulgated", "Datokode", "datokode"])
    date_promulgated = normalize_reg_datokode(datokode_raw) or datokode_raw

    return {
        "title": title,
        "shortTitle": short_title,
        "datePromulgated": date_promulgated,  # now ISO if possible
        "dokID": dok_id,
        "refID": ref_id,
        "id": chosen_id
    }

def find_law_refs_in_regulation(xml_bytes):
    text = xml_bytes.decode("utf-8", errors="ignore")
    matches = re.findall(
        r"(?:NL/)?lov/\d{4}-\d{2}-\d{2}(?:-\d+)?",
        text
    )
    return {m.replace("NL/", "") for m in matches}


# =========================
# FINAL DEBUG PRINTS
# =========================
def print_final_insertion_plan(kept_laws, reg_map):
    log("")
    log("FINAL INSERTION PLAN (this is what goes to the sheet)")
    log("=====================================================")

    row_counter = 0
    for i, law in enumerate(kept_laws, start=1):
        row_counter += 1
        law_id_key = (law.get("id") or "").replace("NL/", "")
        regs_for_law = reg_map.get(law_id_key, [])

        log(f"[ROW {row_counter}] LAW #{i} (status={law['status']})")
        for k in [
            "filename","title","shortTitle",
            "dokID","refID","id",
            "date","datePromulgated","correctedDate",
            "department","legalArea",
            "lastAmendedBy","lastAmendedInForceFrom",
            "documentNote","contentHeading",
            "inForceRaw","effectiveCandidates",
            "accessRemovedDate",
            "reason","url"
        ]:
            log(f"  {k}: {law.get(k,'')}")
        log(f"  regs_attached: {len(regs_for_law)}")
        log("-" * 50)

        for j, reg in enumerate(regs_for_law, start=1):
            row_counter += 1
            log(f"[ROW {row_counter}]   REG #{j} under LAW #{i}")
            log(f"  filename: {reg.get('filename','')}")
            log(f"  title: {reg.get('title','')}")
            log(f"  shortTitle: {reg.get('shortTitle','')}")
            log(f"  id: {reg.get('id','')}")
            log(f"  dokID: {reg.get('dokID','')}")
            log(f"  refID: {reg.get('refID','')}")
            log(f"  date: {reg.get('date','')}")
            log(f"  url: {reg.get('url','')}")
            log("-" * 50)

    log("=====================================================")
    log("END FINAL INSERTION PLAN")
    log("")


# =========================
# GOOGLE SHEETS HELPERS
# =========================
def sheets_service():
    log("[sheets_service] building Google Sheets client")

    sa_file = os.environ.get("GOOGLE_SA_JSON_FILE")
    if sa_file:
        log(f"[sheets_service] using SA file: {sa_file}")
        with open(sa_file, "r", encoding="utf-8") as f:
            sa_json = json.load(f)
    else:
        raw = os.environ.get("GOOGLE_SA_JSON", "")
        log(f"[sheets_service] GOOGLE_SA_JSON length: {len(raw)}")
        sa_json = json.loads(raw)

    creds = service_account.Credentials.from_service_account_info(
        sa_json,
        scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    return build("sheets", "v4", credentials=creds)

def get_sheet_id(svc):
    ss = svc.spreadsheets().get(spreadsheetId=SHEET_ID).execute()
    for s in ss["sheets"]:
        if s["properties"]["title"] == TAB_NAME:
            return s["properties"]["sheetId"]
    raise ValueError(f"Tab '{TAB_NAME}' not found.")

def get_last_row(svc):
    res = svc.spreadsheets().values().get(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!A:A"
    ).execute()
    values = res.get("values", [])
    last = len(values)
    if last < 1:
        last = 1
    log(f"[get_last_row] last used row = {last}")
    return last

def insert_rows_with_format(svc, sheet_id, start_row_1based, count):
    if count <= 0:
        return

    log(f"[insert_rows_with_format] inserting {count} rows at row {start_row_1based}")
    start_index = start_row_1based - 1
    end_index = start_index + count

    body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": start_index,
                        "endIndex": end_index
                    },
                    "inheritFromBefore": False
                }
            },
            {
                "copyPaste": {
                    "source": {
                        "sheetId": sheet_id,
                        "startRowIndex": 1,
                        "endRowIndex": 2
                    },
                    "destination": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_index,
                        "endRowIndex": end_index
                    },
                    "pasteType": "PASTE_FORMAT",
                    "pasteOrientation": "NORMAL"
                }
            }
        ]
    }

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body=body
    ).execute()

    log("[insert_rows_with_format] ✅ rows inserted + formatted")

def write_block(svc, start_row_1based, rows):
    """
    Writes titles to C, dates to H, urls to S, regs marker to T.
    Laws + regs both get dates in H.
    """
    n = len(rows)
    if n == 0:
        return

    titles = [[r.get("title", "")] for r in rows]
    dates  = [[r.get("date", "")] for r in rows]
    urls   = [[r.get("url", "")] for r in rows]
    tvals  = [[2] if r.get("type") == "reg" else [""] for r in rows]

    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!C{start_row_1based}:C{start_row_1based+n-1}",
        valueInputOption="RAW",
        body={"values": titles}
    ).execute()

    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!H{start_row_1based}:H{start_row_1based+n-1}",
        valueInputOption="RAW",
        body={"values": dates}
    ).execute()

    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!S{start_row_1based}:S{start_row_1based+n-1}",
        valueInputOption="RAW",
        body={"values": urls}
    ).execute()

    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!T{start_row_1based}:T{start_row_1based+n-1}",
        valueInputOption="RAW",
        body={"values": tvals}
    ).execute()

    log(f"[write_block] ✅ wrote {n} rows starting at row {start_row_1based}")

def color_rows_orange(svc, sheet_id, row_numbers_1based):
    if not row_numbers_1based:
        return

    log(f"[color_rows_orange] coloring {len(row_numbers_1based)} ambiguous rows orange")

    requests_body = []
    for r in row_numbers_1based:
        start_index = r - 1
        end_index = r
        requests_body.append({
            "repeatCell": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_index,
                    "endRowIndex": end_index
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 1.0,
                            "green": 0.85,
                            "blue": 0.4
                        }
                    }
                },
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": requests_body}
    ).execute()

    log("[color_rows_orange] ✅ done")


# =========================
# MAIN
# =========================
def handler(request=None):
    log("=======================================")
    log("START Norway Lovdata public scrape run")
    log("=======================================")

    laws_blob = download_pkg(LAWS_PKG)
    regs_blob = download_pkg(REGS_PKG)

    laws_files = extract_tar_bz2(laws_blob, "laws")
    regs_files = extract_tar_bz2(regs_blob, "regulations")

    # ---- Parse regs -> map by law ref ----
    reg_map = {}
    for rname, rb in regs_files.items():
        try:
            reg_item = parse_reg_xml(rb)

            reg_date = reg_item.get("datePromulgated") or derive_reg_date_from_filename(rname)
            reg_item["date"] = reg_date

            if not reg_item.get("id"):
                reg_item["id"] = derive_reg_id_from_filename(rname)

            reg_item["url"]  = build_public_reg_url(reg_item.get("id", ""), filename=rname)
            reg_item["filename"] = rname

            law_refs = find_law_refs_in_regulation(rb)
            for law_id in law_refs:
                reg_map.setdefault(law_id, []).append(reg_item)

        except Exception as e:
            log(f"[reg parse fail] {rname}: {e}")

    # ---- Parse all laws ----
    candidate_laws = []
    for lname, lb in laws_files.items():
        try:
            law = parse_law_xml(lb)

            date_iso, _ = derive_date_and_suffix_from_filename(lname, "nl")
            law["date"] = law.get("datePromulgated") or date_iso

            if not law.get("id"):
                law["id"] = derive_law_id_from_filename(lname)

            law["url"] = build_public_law_url(law.get("id", ""), filename=lname)
            law["filename"] = lname

            candidate_laws.append(law)

        except Exception as e:
            log(f"[law parse fail] {lname}: {e}")

    log(f"[handler] total laws parsed: {len(candidate_laws)}")

    kept_laws = [l for l in candidate_laws if l["status"] in ("in_force", "ambiguous")]
    log(f"[handler] kept in-force+ambiguous laws: {len(kept_laws)}")

    kept_laws.sort(
        key=lambda x: parse_iso_date(x.get("date")) or date.min,
        reverse=True
    )

    kept_laws = kept_laws[:LIMIT]
    log(f"[handler] limiting to {len(kept_laws)} laws")

    print_final_insertion_plan(kept_laws, reg_map)

    output_rows = []
    ambiguous_positions = []

    for law in kept_laws:
        pos = len(output_rows)
        output_rows.append({
            "type": "law",
            "title": law.get("title", ""),
            "date": law.get("date", ""),
            "url": law.get("url", ""),
            "status": law.get("status")
        })
        if law["status"] == "ambiguous":
            ambiguous_positions.append(pos)

        law_id_key = (law.get("id") or "").replace("NL/", "")
        regs_for_law = reg_map.get(law_id_key, [])

        for reg in regs_for_law:
            output_rows.append({
                "type": "reg",
                "title": reg.get("title", ""),
                "date": reg.get("date", ""),  # regs date -> H col
                "url": reg.get("url", "")     # regs SF url -> S col
            })

    log(f"[handler] total rows to append: {len(output_rows)}")

    svc = sheets_service()
    sheet_id = get_sheet_id(svc)
    last_row = get_last_row(svc)
    start_row = last_row + 1

    insert_rows_with_format(svc, sheet_id, start_row, len(output_rows))
    write_block(svc, start_row, output_rows)

    ambiguous_rows = [start_row + idx for idx in ambiguous_positions]
    color_rows_orange(svc, sheet_id, ambiguous_rows)

    log("=======================================")
    log("DONE")
    log("=======================================")

    return {
        "status": "appended",
        "laws_kept": len(kept_laws),
        "rows_written": len(output_rows),
        "ambiguous_colored": len(ambiguous_rows)
    }


if __name__ == "__main__":
    print(handler(None))
