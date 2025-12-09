import os, io, json, tarfile, re
import requests
from lxml import etree
from datetime import datetime, date

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

    positive_tag_candidates = all_text_any(
        root,
        [
            "inForceFrom", "effectiveFrom", "ikrafttredelse",
            "ikraftFra", "ikraftFraDato", "iKraftFra", "ikrafttredelseDato"
        ]
    )

    positive_text_candidates = find_effective_dates_in_text(text_full)

    positive_candidates = positive_tag_candidates + positive_text_candidates
    positive_candidates = list(dict.fromkeys([c.strip() for c in positive_candidates if c and c.strip()]))

    effective_candidates = positive_candidates[:]
    effective_candidates.extend(all_text_any(
        root,
        ["lastAmendedInForceFrom", "Ikrafttredelse av siste endring", "Ikrafttreding av siste endring"]
    ))
    effective_candidates = list(dict.fromkeys([c.strip() for c in effective_candidates if c and c.strip()]))

    explicit_not_in_force = (
        "ikke i kraft" in text_lc or
        "ikkje i kraft" in text_lc or
        (in_force_raw or "").lower().strip() in ("false", "0", "no", "nei")
    )

    if explicit_not_in_force and not any_past_or_today_date(positive_candidates):
        status = "not_in_force"
        reason = "explicit ikke/ikkje i kraft and no past/today positive entry-into-force date"

    elif any_future_date(positive_candidates):
        status = "future"
        reason = "future positive entry-into-force date found (tags or text)"

    elif text_says_effective_date_not_fixed(text_full) and not any_past_or_today_date(positive_candidates):
        status = "future"
        reason = "entry into force not fixed (Kongen fastsetter/bestemmer)"

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
        "effectiveCandidates": effective_candidates,
        "positiveCandidates": positive_candidates,
        "accessRemovedDate": access_removed_date,
        "status": status,
        "reason": reason,
    }


# =========================
# PARSING: REGULATIONS
# =========================
def normalize_reg_datokode(datokode: str) -> str:
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
        "datePromulgated": date_promulgated,
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
# GOOGLE SHEETS HELPERS
# =========================
def sheets_service():
    log("[sheets_service] building Google Sheets client")

    raw = os.environ.get("GOOGLE_SA_JSON", "")
    if not raw:
        raise ValueError("Missing GOOGLE_SA_JSON env var")

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
    n = len(rows)
    if n == 0:
        return

    titles = [[r.get("title", "")] for r in rows]
    dates  = [[r.get("date", "")] for r in rows]
    urls   = [[r.get("url", "")] for r in rows]
    tvals  = [[2] if r.get("type") == "reg" else [""] for r in rows]

    # NEW: dropdown values
    mvals = [["Secondary"] if r.get("type") == "reg" else ["Primary"] for r in rows]
    evals = [["Rule/Regulation (non-EU)"] if r.get("type") == "reg" else [""] for r in rows]

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

    # NEW: write E and M dropdown columns
    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!E{start_row_1based}:E{start_row_1based+n-1}",
        valueInputOption="RAW",
        body={"values": evals}
    ).execute()

    svc.spreadsheets().values().update(
        spreadsheetId=SHEET_ID,
        range=f"{TAB_NAME}!M{start_row_1based}:M{start_row_1based+n-1}",
        valueInputOption="RAW",
        body={"values": mvals}
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

def read_existing_rows(svc):
    """
    Reads existing Title (C), Date (H), URL (S) from row 2 down to last row.
    Returns:
      existing_map: {(title_lc, date_iso): {"row": r, "url": url}}
      existing_urls: set(urls)
      last_row: int
    """
    last_row = get_last_row(svc)
    if last_row < 2:
        return {}, set(), last_row

    # Batch get C, H, S for existing rows
    res = svc.spreadsheets().values().batchGet(
        spreadsheetId=SHEET_ID,
        ranges=[
            f"{TAB_NAME}!C2:C{last_row}",
            f"{TAB_NAME}!H2:H{last_row}",
            f"{TAB_NAME}!S2:S{last_row}"
        ]
    ).execute()

    c_vals = res["valueRanges"][0].get("values", [])
    h_vals = res["valueRanges"][1].get("values", [])
    s_vals = res["valueRanges"][2].get("values", [])

    def v_at(vals, i):
        if i < len(vals) and vals[i]:
            return vals[i][0]
        return ""

    existing_map = {}
    existing_urls = set()

    n = max(len(c_vals), len(h_vals), len(s_vals))
    for i in range(n):
        title = v_at(c_vals, i).strip()
        datev = v_at(h_vals, i).strip()
        url   = v_at(s_vals, i).strip()

        if not title and not datev and not url:
            continue

        row_num = 2 + i  # because we started at row 2
        key = (title.lower(), datev)

        # keep first occurrence; you can change if you prefer last occurrence
        if key not in existing_map:
            existing_map[key] = {"row": row_num, "url": url}

        if url:
            existing_urls.add(url)

    return existing_map, existing_urls, last_row


def update_existing_row_fields(svc, row_number_1based, new_url, row_type):
    """Update S always, and M/E depending on row_type."""
    data = [
        {
            "range": f"{TAB_NAME}!S{row_number_1based}",
            "values": [[new_url]]
        },
        {
            "range": f"{TAB_NAME}!M{row_number_1based}",
            "values": [["Secondary"] if row_type == "reg" else ["Primary"]]
        }
    ]

    if row_type == "reg":
        data.append({
            "range": f"{TAB_NAME}!E{row_number_1based}",
            "values": [["Rule/Regulation (non-EU)"]]
        })

    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={
            "valueInputOption": "RAW",
            "data": data
        }
    ).execute()



def color_rows_black(svc, sheet_id, row_numbers_1based):
    if not row_numbers_1based:
        return

    log(f"[color_rows_black] coloring {len(row_numbers_1based)} stale rows black")

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
                            "red": 0.0,
                            "green": 0.0,
                            "blue": 0.0
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

    log("[color_rows_black] ✅ done")




# =========================
# MAIN SCRAPE ENTRYPOINT
# =========================
def run_scrape(request=None):
    log("=======================================")
    log("START Norway Lovdata public scrape run")
    log("=======================================")

    laws_blob = download_pkg(LAWS_PKG)
    regs_blob = download_pkg(REGS_PKG)

    laws_files = extract_tar_bz2(laws_blob, "laws")
    regs_files = extract_tar_bz2(regs_blob, "regulations")

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
                "date": reg.get("date", ""),
                "url": reg.get("url", "")
            })

    log(f"[handler] total rows to append: {len(output_rows)}")

    svc = sheets_service()
    sheet_id = get_sheet_id(svc)

    # Read current sheet state
    existing_map, existing_urls, last_row = read_existing_rows(svc)

    scraped_urls = set()
    rows_to_append = []
    ambiguous_positions_new = []

    # Build deduped / update-aware append set
    for row in output_rows:
        title = (row.get("title") or "").strip()
        datev = (row.get("date") or "").strip()
        url   = (row.get("url") or "").strip()

        if url:
            scraped_urls.add(url)

        key = (title.lower(), datev)

        if key in existing_map:
            existing_row = existing_map[key]
            existing_url = (existing_row.get("url") or "").strip()

            # Rule 1: H and S already exist => skip
            if existing_url == url:
                continue

            # Rule 2: H exists but S different => update S
            if url and existing_row["row"]:
                log(f"[dedupe] updating S/M/E at row {existing_row['row']} for key={key}")
                update_existing_row_fields(svc, existing_row["row"], url, row.get("type"))
                # also update our local map so later comparisons are correct
                existing_map[key]["url"] = url
                existing_urls.add(url)
            continue

        # Not found => append new row
        pos_new = len(rows_to_append)
        rows_to_append.append(row)

        if row.get("status") == "ambiguous":
            ambiguous_positions_new.append(pos_new)

    # Append only new rows
    log(f"[handler] new rows to append after dedupe: {len(rows_to_append)}")

    if rows_to_append:
        start_row = last_row + 1
        insert_rows_with_format(svc, sheet_id, start_row, len(rows_to_append))
        write_block(svc, start_row, rows_to_append)

        ambiguous_rows = [start_row + idx for idx in ambiguous_positions_new]
        color_rows_orange(svc, sheet_id, ambiguous_rows)
    else:
        ambiguous_rows = []

    # Rule 3: If S in sheet not in scrape => black row
    stale_rows = []
    for key, info in existing_map.items():
        url_existing = (info.get("url") or "").strip()
        row_num = info.get("row")

        if url_existing and url_existing not in scraped_urls:
            stale_rows.append(row_num)

    color_rows_black(svc, sheet_id, stale_rows)


    log("=======================================")
    log("DONE")
    log("=======================================")

    return {
        "status": "appended",
        "laws_kept": len(kept_laws),
        "rows_written": len(output_rows),
        "ambiguous_colored": len(ambiguous_rows)
    }

# Alias for compatibility
handler = run_scrape

if __name__ == "__main__":
    print(run_scrape(None))
