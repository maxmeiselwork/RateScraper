#!/usr/bin/env python3
"""
Rate Deck Automation Tool
Fills competitor rates from Expedia/Booking.com into hotel Rate Deck spreadsheets.
"""
# -*- coding: utf-8 -*-

from flask import Flask, render_template, request, send_file, jsonify
import openpyxl
from io import BytesIO
from datetime import datetime, date
import traceback

# Files with many accumulated named styles (Normal, Heading 1, custom, etc.)
# cause openpyxl's apply_stylesheet to spend minutes in NamedStyle._recalculate,
# which indexes every named style's border/font/fill/alignment into the workbook's
# IndexedLists using slow recursive __hash__/__eq__ comparisons.  We never apply
# named styles by name — we only write cell values — so skipping _recalculate is
# safe and eliminates the timeout entirely.  Cell-level xf formatting is loaded
# before this step and is unaffected.
try:
    from openpyxl.styles.named_styles import NamedStyle as _NamedStyle
    _NamedStyle._recalculate = lambda self: None
except Exception:
    pass

app = Flask(__name__)
app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024  # 100MB


# ---------------------------------------------------------------------------
# Competitor mappings
# Each entry: (keyword_in_input_name, keyword_to_find_in_rate_deck_col_A)
# Matching is case-insensitive substring.
# ---------------------------------------------------------------------------

H2O_EXPEDIA_MAP = [
    ('Margaritaville',  'Margaritaville'),
    ('Casa Marina',     'Casa Marina'),
    ('Hyatt Centric',   'Hyatt Centric'),
    ('Ocean Key',       'Ocean Key'),
    ('Pier House',      'Pier House'),
    ('Southernmost',    'Southernmost'),
    ('Reach',           'Reach'),
    ('Courtyard',       'Courtyard'),
]

SMS_EXPEDIA_MAP = [
    ('Margaritaville',  'Margaritaville'),
    ('Casa Marina',     'Casa Marina'),
    ('Hyatt Centric',   'Hyatt Centric'),
    ('Ocean Key',       'Ocean Key'),
    ('Pier House',      'Pier House'),
    ('Southernmost',    'Southernmost'),
    ('Reach',           'Reach'),
    # No Courtyard for SMS
]

# SWM input is always the Lighthouse / Booking.com "Rates" sheet.
SWM_BOOKINGCOM_MAP = [
    ('Southwinds',      'Southwinds'),
    ('Blue Marlin',     'Blue Marlin'),
    ('Best Western',    'Best Western'),
    ('Blue Flamingo',   'Blue Flamingo'),
    ('Courtyard',       'Courtyard'),
    ('Fairfield',       'Fairfield'),
]


# ---------------------------------------------------------------------------
# Value normalisation
# ---------------------------------------------------------------------------

def _to_int_if_whole(v):
    try:
        if v == int(v):
            return int(v)
    except Exception:
        pass
    return v

def normalize_expedia(val):
    """Map Expedia cell to Rate Deck value. Returns None to skip writing."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return _to_int_if_whole(val)
    s = str(val).strip()
    if s in ('S', 'I'):
        return 'SOLD'
    if s == 'M':
        return 'M'
    if s == '-':
        return None   # no data - do not overwrite existing value
    try:
        return _to_int_if_whole(float(s.replace(',', '')))
    except ValueError:
        return s if s else None

def normalize_bookingcom(val):
    """Map Booking.com cell to Rate Deck value. Returns None to skip writing."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return _to_int_if_whole(val)
    s = str(val).strip()
    sl = s.lower()
    if sl == 'sold out':
        return 'SOLD'
    if sl in ('no flex', '--', ''):
        return None   # no data - do not overwrite existing value
    if sl.startswith('los'):
        return s.upper()
    try:
        return _to_int_if_whole(float(s.replace(',', '')))
    except ValueError:
        return s if s else None


# ---------------------------------------------------------------------------
# Sheet / cell lookups
# ---------------------------------------------------------------------------

def _normalise_month_str(s):
    """
    Normalise month strings so strptime can parse them regardless of
    how the spreadsheet spells the month name.
    Handles: full names, 3-letter abbreviations, and 'Sept' (4-letter variant).
    Works for any year.
    """
    s = s.strip()
    # 'Sept YYYY' -> 'Sep YYYY'  (strptime only knows 3-letter 'Sep')
    import re
    s = re.sub(r'\bSept\b', 'Sep', s, flags=re.IGNORECASE)
    return s

def parse_sheet_month_year(sheet_name):
    """
    Return (year, month) from any tab name like:
      'May 2026', 'Jan 2026', 'Sept 2026', 'September 2026', 'Jan 2028', etc.
    Returns None if the name cannot be parsed as a month+year.
    Works for any future year automatically.
    """
    name = _normalise_month_str(sheet_name)
    for fmt in ('%B %Y', '%b %Y'):
        try:
            dt = datetime.strptime(name, fmt)
            return (dt.year, dt.month)
        except ValueError:
            pass
    return None

def find_sheet_for_date(wb, target_date):
    """Return the worksheet whose tab name matches target_date month/year, or None."""
    key = (target_date.year, target_date.month)
    for name in wb.sheetnames:
        if parse_sheet_month_year(name) == key:
            return wb[name]
    return None

def find_col_for_date(ws, target_date, header_row=3, min_col=2):
    """
    Return the column index for target_date in a Rate Deck sheet.
    Strategy 1: direct match on row 3 (works when cells hold actual date values).
    Strategy 2: offset from A4 (works when A4 holds a real datetime anchor).
    Strategy 3: offset from the 1st of the month (fallback when A4 is a formula).
    Pass a data_only-loaded worksheet for best results.
    """
    # Strategy 1: scan row 3 for a real date value
    for col in range(min_col, ws.max_column + 1):
        val = ws.cell(header_row, col).value
        if val is None or isinstance(val, str):
            continue
        cell_date = val.date() if isinstance(val, datetime) else val
        if isinstance(cell_date, date) and cell_date == target_date:
            return col

    # Strategy 2: A4 contains the 1st of the month as a real datetime
    a4 = ws.cell(4, 1).value
    if isinstance(a4, datetime):
        start = a4.date()
        offset = (target_date - start).days
        if 0 <= offset <= 30:
            col = min_col + offset
            if col <= ws.max_column:
                return col

    # Strategy 3: derive start from target_date's own month (works even when A4 is a formula)
    first_of_month = date(target_date.year, target_date.month, 1)
    offset = (target_date - first_of_month).days
    col = min_col + offset
    if col <= ws.max_column:
        return col

    return None

def find_row_for_label(ws, keyword, search_col=1, min_row=20, max_row=50):
    """Return row where search_col contains keyword (case-insensitive partial match)."""
    kw = keyword.lower()
    for row in range(min_row, max_row + 1):
        val = ws.cell(row, search_col).value
        if val and kw in str(val).lower():
            return row
    return None


# ---------------------------------------------------------------------------
# Expedia date->column map
# ---------------------------------------------------------------------------

def build_expedia_date_col_map(ws):
    """
    Returns {date: col_idx} by reading month headers from row 9
    and day numbers from row 11.
    """
    date_map = {}
    current_month = None
    current_year = None

    for col in range(2, ws.max_column + 1):
        month_cell = ws.cell(9, col).value
        if month_cell and isinstance(month_cell, str) and len(month_cell.strip()) > 4:
            normalised = _normalise_month_str(month_cell).title()  # e.g. "September 2026"
            for fmt in ('%B %Y', '%b %Y'):
                try:
                    dt = datetime.strptime(normalised, fmt)
                    current_month = dt.month
                    current_year = dt.year
                    break
                except ValueError:
                    pass

        if current_month is None:
            continue

        day_val = ws.cell(11, col).value
        if day_val is not None:
            try:
                full_date = date(current_year, current_month, int(day_val))
                date_map[full_date] = col
            except (ValueError, TypeError):
                pass

    return date_map


# ---------------------------------------------------------------------------
# Booking.com date->row map
# ---------------------------------------------------------------------------

def build_bookingcom_date_row_map(ws):
    """Returns {date: row_idx} from the Rates sheet (col C = date, starts row 6)."""
    date_map = {}
    for row in range(6, ws.max_row + 1):
        val = ws.cell(row, 3).value
        if val is None:
            continue
        cell_date = val.date() if isinstance(val, datetime) else val
        if isinstance(cell_date, date):
            date_map[cell_date] = row
    return date_map


# ---------------------------------------------------------------------------
# Core processors
# ---------------------------------------------------------------------------

def process_expedia(master_wb, master_wb_ro, input_wb, competitor_map, log):
    ws_expedia = input_wb.active  # "Expedia - Revenue management"

    # Map Expedia row to deck keyword by scanning col A rows 12-30
    expedia_row_for = {}
    for row in range(12, 31):
        name = ws_expedia.cell(row, 1).value
        if not name:
            continue
        name_lc = str(name).lower()
        for (exp_kw, deck_kw) in competitor_map:
            if exp_kw.lower() in name_lc:
                expedia_row_for[deck_kw] = row
                break

    log.append('Expedia competitors matched: ' + str(list(expedia_row_for.keys())))

    expedia_date_col = build_expedia_date_col_map(ws_expedia)
    if expedia_date_col:
        d_min = min(expedia_date_col).isoformat()
        d_max = max(expedia_date_col).isoformat()
        log.append('Expedia date range: ' + d_min + ' to ' + d_max)

    cells_written = 0
    sheets_missed = 0
    cols_missed = 0
    for target_date, exp_col in expedia_date_col.items():
        # Use data_only workbook for lookups so formula cells return computed values
        deck_ws_ro = find_sheet_for_date(master_wb_ro, target_date)
        if deck_ws_ro is None:
            sheets_missed += 1
            continue
        deck_col = find_col_for_date(deck_ws_ro, target_date)
        if deck_col is None:
            cols_missed += 1
            log.append('No col for ' + target_date.isoformat() + ' in ' + deck_ws_ro.title)
            continue

        # Write to the regular (formula-preserving) workbook
        deck_ws = master_wb[deck_ws_ro.title]

        for deck_kw, exp_row in expedia_row_for.items():
            deck_row = find_row_for_label(deck_ws_ro, deck_kw)
            if deck_row is None:
                continue
            val = normalize_expedia(ws_expedia.cell(exp_row, exp_col).value)
            if val is not None:
                deck_ws.cell(deck_row, deck_col).value = val
                cells_written += 1

    if sheets_missed:
        log.append('Dates skipped (no matching sheet): ' + str(sheets_missed))
    if cols_missed:
        log.append('Dates skipped (no matching column): ' + str(cols_missed))
    log.append('Cells written: ' + str(cells_written))


def process_bookingcom(master_wb, master_wb_ro, input_wb, log):
    # SWM input is always a Lighthouse/Booking.com export - use the "Rates" sheet only.
    if 'Rates' not in input_wb.sheetnames:
        raise ValueError(
            'Expected a Lighthouse "Rates" sheet but found: ' + str(input_wb.sheetnames) +
            '. Please upload the correct SWM Lighthouse export file.'
        )
    ws_rates = input_wb['Rates']

    # Map Booking.com column to deck keyword by scanning row 5 headers
    bookingcom_col_for = {}
    for col in range(4, ws_rates.max_column + 1):
        header = ws_rates.cell(5, col).value
        if not header:
            continue
        header_lc = str(header).lower()
        for (bc_kw, deck_kw) in SWM_BOOKINGCOM_MAP:
            if bc_kw.lower() in header_lc:
                bookingcom_col_for[deck_kw] = col
                break

    log.append('Booking.com competitors matched: ' + str(list(bookingcom_col_for.keys())))

    bc_date_row = build_bookingcom_date_row_map(ws_rates)
    if bc_date_row:
        log.append('Booking.com date range: ' + min(bc_date_row).isoformat() + ' to ' + max(bc_date_row).isoformat())

    cells_written = 0
    cols_missed = 0
    for target_date, bc_row in bc_date_row.items():
        deck_ws_ro = find_sheet_for_date(master_wb_ro, target_date)
        if deck_ws_ro is None:
            continue
        deck_col = find_col_for_date(deck_ws_ro, target_date)
        if deck_col is None:
            cols_missed += 1
            log.append('No col for ' + target_date.isoformat() + ' in ' + deck_ws_ro.title)
            continue

        deck_ws = master_wb[deck_ws_ro.title]

        for deck_kw, bc_col in bookingcom_col_for.items():
            deck_row = find_row_for_label(deck_ws_ro, deck_kw)
            if deck_row is None:
                continue
            val = normalize_bookingcom(ws_rates.cell(bc_row, bc_col).value)
            if val is not None:
                deck_ws.cell(deck_row, deck_col).value = val
                cells_written += 1

    if cols_missed:
        log.append('Dates skipped (no matching column): ' + str(cols_missed))
    log.append('Cells written: ' + str(cells_written))


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.route('/')
def index():
    return render_template('index.html')


@app.route('/generate', methods=['POST'])
def generate():
    prop = request.form.get('property', '').lower()
    initials = request.form.get('initials', '').strip()
    input_file = request.files.get('input_file')
    master_file = request.files.get('master_file')

    if not all([prop, initials, input_file, master_file]):
        return jsonify({'error': 'All fields are required.'}), 400
    if prop not in ('h2o', 'sms', 'swm'):
        return jsonify({'error': 'Unknown property type.'}), 400

    log = []
    try:
        log.append('Loading input: ' + input_file.filename)
        input_wb = openpyxl.load_workbook(BytesIO(input_file.read()), data_only=True)

        log.append('Loading master: ' + master_file.filename)
        master_bytes = master_file.read()
        # Load twice: data_only for reading cached formula values, normal for writing.
        # Named-style binding is patched to a no-op above so both loads are fast.
        master_wb_ro = openpyxl.load_workbook(BytesIO(master_bytes), data_only=True)
        master_wb    = openpyxl.load_workbook(BytesIO(master_bytes))

        if prop == 'h2o':
            process_expedia(master_wb, master_wb_ro, input_wb, H2O_EXPEDIA_MAP, log)
        elif prop == 'sms':
            process_expedia(master_wb, master_wb_ro, input_wb, SMS_EXPEDIA_MAP, log)
        elif prop == 'swm':
            process_bookingcom(master_wb, master_wb_ro, input_wb, log)

        master_wb_ro.close()

        today = datetime.now().strftime('%y%m%d')
        prop_label = {'h2o': 'H2O', 'sms': 'SMS', 'swm': 'SWM'}[prop]
        filename = today + '-KO-' + prop_label + '-Rate Deck-' + initials + '.xlsx'

        output = BytesIO()
        master_wb.save(output)
        output.seek(0)

        try:
            print('\n'.join(log))
        except Exception:
            pass  # console encoding issues must not abort a good response

        return send_file(
            output,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            as_attachment=True,
            download_name=filename,
        )

    except Exception as e:
        try:
            traceback.print_exc()
        except Exception:
            pass
        err_msg = str(e).encode('ascii', errors='replace').decode('ascii')
        return jsonify({'error': err_msg, 'log': log}), 500


if __name__ == '__main__':
    print('Rate Deck Automation Tool')
    print('Open http://localhost:5000 in your browser')
    print()
    app.run(debug=False, port=5000)
