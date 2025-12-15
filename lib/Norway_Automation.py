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
    last_amended_ifr = first_text_any(
        root,
        ["lastAmendedInForceFrom", "Ikrafttredelse av siste endring", "Ikrafttreding av siste endring"]
    )

    department       = first_text_any(root, ["department", "Departement"])
    legal_area       = first_text_any(root, ["legalArea", "Rettsområde"])
    document_note    = first_text_any(root, ["documentNote", "Annet om dokumentet", "Anna om dokumentet"])
    content_heading  = first_text_any(root, ["contentHeading", "Innhold"])
    access_removed_date = first_text_any(root, ["accessRemovedDate", "access_removedDate"])

    in_force_raw = first_text_any(
        root,
        ["inForce", "Ikke i kraft", "Ikkje i kraft", "Ikrafttredelse", "Ikrafttreding"]
    )

    text_full = etree.tostring(root, encoding="unicode", method="text")
    text_lc = (text_full or "").lower()

    positive_tag_candidates = all_text_any(
        root,
        [
            "inForceFrom", "effectiveFrom", "ikrafttredelse",
            "ikraftFra", "ikraftFraDato", "iKraftFra", "ikrafttredelseDato",
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
        "ikke i kraft" in text_lc
        or "ikkje i kraft" in text_lc
        or (in_force_raw or "").lower().strip() in ("false", "0", "no", "nei")
    )

    has_past_or_today = any_past_or_today_date(positive_candidates)
    has_future = any_future_date(positive_candidates)

    if explicit_not_in_force and not has_past_or_today:
        status = "not_in_force"
        reason = "explicit ikke/ikkje i kraft and no past/today positive entry-into-force date"
    elif has_past_or_today:
        status = "in_force"
        reason = "positive entry-into-force date <= today found (tags or text)"
    elif text_says_effective_date_not_fixed(text_full):
        status = "future"
        reason = "entry into force not fixed (Kongen fastsetter/bestemmer)"
    elif has_future:
        status = "future"
        reason = "future positive entry-into-force date found (tags or text)"
    else:
        raw_lc = (in_force_raw or "").lower().strip()
        raw_has_not_in_force = ("ikke i kraft" in raw_lc or "ikkje i kraft" in raw_lc)
        raw_has_in_force = (
            raw_lc in ("true", "1", "yes", "ja")
            or ("i kraft" in raw_lc and "ikke" not in raw_lc and "ikkje" not in raw_lc)
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
    matches = re.findall(r"(?:NL/)?lov/\d{4}-\d{2}-\d{2}(?:-\d+)?", text)
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
    """
    Last used row = last row where ANY of these columns has a value:
    C, H, S, M, E.
    Minimum returned row is 2 (template row).
    """
    ranges = [
        f"{TAB_NAME}!C:C",
        f"{TAB_NAME}!H:H",
        f"{TAB_NAME}!S:S",
        f"{TAB_NAME}!M:M",
        f"{TAB_NAME}!E:E",
    ]

    res = svc.spreadsheets().values().batchGet(
        spreadsheetId=SHEET_ID,
        ranges=ranges
    ).execute()

    cols = [vr.get("values", []) for vr in res.get("valueRanges", [])]

    def cell(col_vals, i):
        if i < len(col_vals) and col_vals[i]:
            return str(col_vals[i][0]).strip()
        return ""

    max_len = max((len(c) for c in cols), default=0)

    last_used = 2
    for i in range(max_len):
        if any(cell(col, i) for col in cols):
            last_used = max(last_used, i + 1)

    log(f"[get_last_row] last used row (any value in C/H/S/M/E) = {last_used}")
    return last_used


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
                "range": {"sheetId": sheet_id, "startRowIndex": start_index, "endRowIndex": end_index},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 1.0, "green": 0.85, "blue": 0.4}}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": requests_body}
    ).execute()

    log("[color_rows_orange] ✅ done")


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
                "range": {"sheetId": sheet_id, "startRowIndex": start_index, "endRowIndex": end_index},
                "cell": {"userEnteredFormat": {"backgroundColor": {"red": 0.0, "green": 0.0, "blue": 0.0}}},
                "fields": "userEnteredFormat.backgroundColor"
            }
        })

    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": requests_body}
    ).execute()

    log("[color_rows_black] ✅ done")


def read_existing_context(svc):
    """
    Reads existing context from row 3 down.

    Returns:
      initial_last_row: int
      primary_pairs: set((date, url)) for Primary rows only
      regs_under_primary: dict{primary_url: set(reg_url)} based on M blocks
      all_url_rows: list of (row_num, url) for blackening
      primary_blocks: list of dicts:
          { "url": primary_url, "row": primary_row, "end_row": end_row }
    """
    initial_last_row = get_last_row(svc)
    if initial_last_row < 3:
        return initial_last_row, set(), {}, [], []

    res = svc.spreadsheets().values().batchGet(
        spreadsheetId=SHEET_ID,
        ranges=[
            f"{TAB_NAME}!H3:H{initial_last_row}",  # date
            f"{TAB_NAME}!S3:S{initial_last_row}",  # url
            f"{TAB_NAME}!M3:M{initial_last_row}",  # Primary/Secondary marker
        ]
    ).execute()

    h_vals = res["valueRanges"][0].get("values", [])
    s_vals = res["valueRanges"][1].get("values", [])
    m_vals = res["valueRanges"][2].get("values", [])

    def v_at(vals, i):
        if i < len(vals) and vals[i]:
            return str(vals[i][0]).strip()
        return ""

    primary_pairs = set()
    regs_under_primary = {}
    all_url_rows = []
    primary_rows = []

    current_primary_url = None

    n = max(len(h_vals), len(s_vals), len(m_vals))
    for i in range(n):
        row_num = 3 + i
        d = v_at(h_vals, i)
        u = v_at(s_vals, i)
        m = v_at(m_vals, i).lower()

        if u:
            all_url_rows.append((row_num, u))

        is_primary = (m == "primary")

        if is_primary:
            current_primary_url = u or None
            if d and u:
                primary_pairs.add((d, u))
            regs_under_primary.setdefault(current_primary_url, set())
            primary_rows.append({"url": current_primary_url, "row": row_num})
        else:
            if current_primary_url and u:
                regs_under_primary.setdefault(current_primary_url, set()).add(u)

    primary_blocks = []
    for idx, p in enumerate(primary_rows):
        start_row = p["row"]
        if idx + 1 < len(primary_rows):
            end_row = primary_rows[idx + 1]["row"] - 1
        else:
            end_row = initial_last_row
        primary_blocks.append({"url": p["url"], "row": start_row, "end_row": end_row})

    return initial_last_row, primary_pairs, regs_under_primary, all_url_rows, primary_blocks


# =========================
# BATCHED INSERT + BATCHED WRITE (#1, #2, #3)
# =========================
def batch_insert_rows_with_format(svc, sheet_id, insert_ops_existing, append_op):
    """
    #2: One spreadsheets.batchUpdate for ALL row inserts + formatting:
      - all inserts-under-existing-laws (bottom-up)
      - then append block at bottom
    """
    requests_body = []

    # existing inserts bottom->top
    for start_row_1based, count in sorted(insert_ops_existing, key=lambda x: x[0], reverse=True):
        if count <= 0:
            continue
        start_index = start_row_1based - 1
        end_index = start_index + count

        requests_body.append({
            "insertDimension": {
                "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": start_index, "endIndex": end_index},
                "inheritFromBefore": False
            }
        })
        requests_body.append({
            "copyPaste": {
                "source": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
                "destination": {"sheetId": sheet_id, "startRowIndex": start_index, "endRowIndex": end_index},
                "pasteType": "PASTE_FORMAT",
                "pasteOrientation": "NORMAL"
            }
        })

    # append insert last
    if append_op:
        start_row_1based, count = append_op
        if count > 0:
            start_index = start_row_1based - 1
            end_index = start_index + count

            requests_body.append({
                "insertDimension": {
                    "range": {"sheetId": sheet_id, "dimension": "ROWS", "startIndex": start_index, "endIndex": end_index},
                    "inheritFromBefore": False
                }
            })
            requests_body.append({
                "copyPaste": {
                    "source": {"sheetId": sheet_id, "startRowIndex": 1, "endRowIndex": 2},
                    "destination": {"sheetId": sheet_id, "startRowIndex": start_index, "endRowIndex": end_index},
                    "pasteType": "PASTE_FORMAT",
                    "pasteOrientation": "NORMAL"
                }
            })

    if not requests_body:
        return

    log(f"[batch_insert_rows_with_format] batchUpdate requests: {len(requests_body)}")
    svc.spreadsheets().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"requests": requests_body}
    ).execute()
    log("[batch_insert_rows_with_format] ✅ done")


def shift_new_segment_start_after_inserts(segment_start_row_1based, insert_ops_existing):
    """
    IMPORTANT (fix for blank inserted rows):
    For a *newly inserted segment* planned at start_row, its FINAL start row is shifted only by
    inserts that happen ABOVE it (strictly smaller start_row), NOT by itself.

    final_start = start + sum(count for (ins_start, count) where ins_start < start)
    """
    shift = 0
    for ins_start, count in insert_ops_existing:
        if ins_start < segment_start_row_1based:
            shift += count
    return segment_start_row_1based + shift


def shift_existing_row_after_inserts(row_num_1based, insert_ops_existing):
    """
    For an *existing row* already in the sheet before inserts, if we insert at its row index,
    it gets pushed down. So we use <=.

    new_row = row + sum(count for (ins_start, count) where ins_start <= row)
    """
    shift = 0
    for ins_start, count in insert_ops_existing:
        if ins_start <= row_num_1based:
            shift += count
    return row_num_1based + shift


def batch_write_segments_values(svc, segments_existing, segments_append, insert_ops_existing):
    """
    #1 + #3:
      - ONE values.batchUpdate for ALL writes (C/H/S/M/E)
      - segments_existing start rows are shifted using insert_ops_existing (< rule).
      - segments_append start rows are already FINAL coordinates.
    """
    data = []

    def add_segment(start_row, rows):
        n = len(rows)
        if n <= 0:
            return
        end_row = start_row + n - 1

        titles = [[r.get("title", "")] for r in rows]
        dates  = [[r.get("date", "")] for r in rows]
        urls   = [[r.get("url", "")] for r in rows]
        mvals  = [["Secondary"] if r.get("type") == "reg" else ["Primary"] for r in rows]
        evals  = [["Rule/Regulation (non-EU)"] if r.get("type") == "reg" else [""] for r in rows]

        data.append({"range": f"{TAB_NAME}!C{start_row}:C{end_row}", "values": titles})
        data.append({"range": f"{TAB_NAME}!H{start_row}:H{end_row}", "values": dates})
        data.append({"range": f"{TAB_NAME}!S{start_row}:S{end_row}", "values": urls})
        data.append({"range": f"{TAB_NAME}!M{start_row}:M{end_row}", "values": mvals})
        data.append({"range": f"{TAB_NAME}!E{start_row}:E{end_row}", "values": evals})

    # existing-law insert segments: shift starts properly
    for orig_start, rows in segments_existing:
        final_start = shift_new_segment_start_after_inserts(orig_start, insert_ops_existing)
        add_segment(final_start, rows)

    # append segment(s): already final
    for final_start, rows in segments_append:
        add_segment(final_start, rows)

    if not data:
        return

    log(f"[batch_write_segments_values] batchUpdate ranges: {len(data)}")
    svc.spreadsheets().values().batchUpdate(
        spreadsheetId=SHEET_ID,
        body={"valueInputOption": "RAW", "data": data}
    ).execute()
    log("[batch_write_segments_values] ✅ done")


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

    # ---- Build regulation map keyed by law id ----
    reg_map = {}
    for rname, rb in regs_files.items():
        try:
            reg_item = parse_reg_xml(rb)
            reg_date = reg_item.get("datePromulgated") or derive_reg_date_from_filename(rname)
            reg_item["date"] = reg_date

            if not reg_item.get("id"):
                reg_item["id"] = derive_reg_id_from_filename(rname)

            reg_item["url"] = build_public_reg_url(reg_item.get("id", ""), filename=rname)
            reg_item["filename"] = rname

            law_refs = find_law_refs_in_regulation(rb)
            for law_id in law_refs:
                reg_map.setdefault(law_id, []).append(reg_item)

        except Exception as e:
            log(f"[reg parse fail] {rname}: {e}")

    # ---- Parse laws ----
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

    kept_laws.sort(key=lambda x: parse_iso_date(x.get("date")) or date.min, reverse=True)
    log(f"[handler] total kept laws (no LIMIT applied): {len(kept_laws)}")

    # ---- Read sheet context once ----
    svc = sheets_service()
    sheet_id = get_sheet_id(svc)

    (
        initial_last_row,
        primary_pairs,
        regs_under_primary,
        all_url_rows,
        primary_blocks
    ) = read_existing_context(svc)

    primary_block_by_url = {b["url"]: b for b in primary_blocks if b["url"]}

    # ---- Build rows to append + rows to insert under existing laws ----
    output_rows_to_append = []
    regs_to_insert_under_existing = {}
    ambiguous_positions_new = []
    scraped_urls = set()

    for law in kept_laws:
        law_title = law.get("title", "")
        law_date  = law.get("date", "")
        law_url   = (law.get("url") or "").strip()
        law_status = law.get("status")

        if law_url:
            scraped_urls.add(law_url)

        law_is_existing = bool(law_date and law_url and (law_date, law_url) in primary_pairs)

        # PRIMARY dedupe basis: (H=date, S=url)
        if not law_is_existing:
            output_rows_to_append.append({
                "type": "law",
                "title": law_title,
                "date": law_date,
                "url": law_url,
                "status": law_status
            })
            if law_status == "ambiguous":
                ambiguous_positions_new.append(len(output_rows_to_append) - 1)

            if law_date and law_url:
                primary_pairs.add((law_date, law_url))

        # SECONDARY dedupe: only within same primary block (until next primary)
        law_id_key = (law.get("id") or "").replace("NL/", "")
        regs_for_law = reg_map.get(law_id_key, [])

        seen_reg_urls_for_this_law_run = set()
        existing_regs_for_this_law_sheet = regs_under_primary.get(law_url, set())

        for reg in regs_for_law:
            reg_title = reg.get("title", "")
            reg_date  = reg.get("date", "")
            reg_url   = (reg.get("url") or "").strip()

            if reg_url:
                scraped_urls.add(reg_url)

            if reg_url and reg_url in existing_regs_for_this_law_sheet:
                continue
            if reg_url and reg_url in seen_reg_urls_for_this_law_run:
                continue

            reg_row = {"type": "reg", "title": reg_title, "date": reg_date, "url": reg_url}

            if law_is_existing:
                regs_to_insert_under_existing.setdefault(law_url, []).append(reg_row)
            else:
                output_rows_to_append.append(reg_row)

            if reg_url:
                seen_reg_urls_for_this_law_run.add(reg_url)

    log(f"[handler] rows to append at bottom: {len(output_rows_to_append)}")
    log(f"[handler] laws with new regs to insert under existing primaries: {len(regs_to_insert_under_existing)}")

    # ---- Plan existing inserts ----
    insert_ops_existing = []     # (orig_start_row, count)
    segments_existing = []       # (orig_start_row, rows_list)

    blocks_with_regs = []
    for law_url, reg_rows in regs_to_insert_under_existing.items():
        block = primary_block_by_url.get(law_url)
        if not block:
            log(f"[insert-existing] primary block not found for URL {law_url}, will append regs at bottom instead")
            output_rows_to_append.extend(reg_rows)
            continue
        insertion_row = block["end_row"] + 1
        blocks_with_regs.append((insertion_row, law_url, reg_rows))

    # IMPORTANT: we still insert bottom->top
    blocks_with_regs.sort(key=lambda x: x[0], reverse=True)

    for insertion_row, law_url, reg_rows in blocks_with_regs:
        count = len(reg_rows)
        if count <= 0:
            continue
        insert_ops_existing.append((insertion_row, count))
        segments_existing.append((insertion_row, reg_rows))

        for r in reg_rows:
            u = r.get("url")
            if u:
                regs_under_primary.setdefault(law_url, set()).add(u)

    total_inserted_existing = sum(c for _, c in insert_ops_existing)

    # ---- Plan append (final coordinates) ----
    segments_append = []
    append_start_row = None
    append_op = None

    if output_rows_to_append:
        append_start_row = initial_last_row + total_inserted_existing + 1
        if append_start_row < 3:
            append_start_row = 3
        append_op = (append_start_row, len(output_rows_to_append))
        segments_append.append((append_start_row, output_rows_to_append))

    # ---- Execute inserts in ONE call (#2) ----
    batch_insert_rows_with_format(svc, sheet_id, insert_ops_existing, append_op)

    # ---- Execute ALL value writes in ONE call (#1 + #3) ----
    # FIXED: existing segments are shifted before writing so we don't create blank inserted rows
    batch_write_segments_values(svc, segments_existing, segments_append, insert_ops_existing)

    # ---- Color ambiguous (only new laws in append block) ----
    ambiguous_rows = []
    if append_start_row is not None and ambiguous_positions_new:
        ambiguous_rows = [append_start_row + idx for idx in ambiguous_positions_new]
        color_rows_orange(svc, sheet_id, ambiguous_rows)

    # ---- Black out stale existing rows (shift existing rows by <= rule) ----
    stale_rows = []
    for row_num, url_existing in all_url_rows:
        if url_existing and url_existing not in scraped_urls:
            stale_rows.append(shift_existing_row_after_inserts(row_num, insert_ops_existing))
    color_rows_black(svc, sheet_id, stale_rows)

    total_rows_written = sum(len(rows) for _, rows in segments_existing) + sum(len(rows) for _, rows in segments_append)

    log("=======================================")
    log("DONE")
    log("=======================================")

    return {
        "status": "appended",
        "laws_kept": len(kept_laws),
        "rows_written": total_rows_written,
        "ambiguous_colored": len(ambiguous_rows),
        "stale_blackened": len(stale_rows)
    }


# Alias for compatibility
handler = run_scrape

if __name__ == "__main__":
    print(run_scrape(None))
