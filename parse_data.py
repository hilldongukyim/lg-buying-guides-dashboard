#!/usr/bin/env python3
"""
LG Buying Guides GA Data Parser
Reads monthly xlsx files and master link file, outputs data.json for the dashboard.
"""

import json
import os
import re
import glob
import unicodedata
import openpyxl
from collections import defaultdict

DATA_DIR = os.path.dirname(os.path.abspath(__file__))
MONTH_ORDER = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
MONTH_COL_MAP = {m: i + 8 for i, m in enumerate(MONTH_ORDER)}  # Jan=col8(idx H), Feb=col9(idx I), ...

COUNTRY_MAP = {
    'UK': '영국(UK)', 'FR': '프랑스(FR)', 'PL': '폴란드(PL)', 'ES': '스페인(ES)',
    'BR': '브라질(BR)', 'CA-EN': '캐나다(CA_en)', 'CA-FR': '캐나다(CA_fr)',
    'MX': '멕시코(MX)', 'NL': '네덜란드(NL)', 'IT': '이탈리아(IT)', 'DE': '독일(DE)'
}

# Key metrics and their row definitions (row offsets from year header row=77)
# These are the primary KPIs we extract
KEY_METRICS = {
    # Acquisition
    'page_view_micro': {'section': 'Acquisition', 'label': 'Page View (Micro)', 'row_offset': 3},
    'page_view_lineup': {'section': 'Acquisition', 'label': 'Page View (Lineup)', 'row_offset': 4},
    'session_micro': {'section': 'Acquisition', 'label': 'Session (Micro)', 'row_offset': 6},
    'session_lineup': {'section': 'Acquisition', 'label': 'Session (Lineup)', 'row_offset': 7},
    'session_internal': {'section': 'Acquisition', 'label': 'Internal Sessions', 'row_offset': 8},
    'session_external': {'section': 'Acquisition', 'label': 'External (Entrance)', 'row_offset': 9},
    'session_organic': {'section': 'Acquisition', 'label': 'Organic Sessions', 'row_offset': 10},
    'session_others': {'section': 'Acquisition', 'label': 'Others (Paid/Affiliates/etc)', 'row_offset': 11},
    'engaged_session': {'section': 'Acquisition', 'label': 'Engaged Session', 'row_offset': 12},
    'exit_micro': {'section': 'Acquisition', 'label': 'Exit (Micro)', 'row_offset': 14},
    'exit_lineup': {'section': 'Acquisition', 'label': 'Exit (Lineup)', 'row_offset': 15},
    'pv_per_session_micro': {'section': 'Acquisition', 'label': 'PV/Session (Micro)', 'row_offset': 17, 'is_rate': True},
    'pv_per_session_lineup': {'section': 'Acquisition', 'label': 'PV/Session (Lineup)', 'row_offset': 18, 'is_rate': True},
    'engagement_rate': {'section': 'Acquisition', 'label': 'Engagement Rate', 'row_offset': 19, 'is_rate': True},
    'exit_rate_micro': {'section': 'Acquisition', 'label': 'Exit Rate (Micro)', 'row_offset': 21, 'is_rate': True},
    'exit_rate_lineup': {'section': 'Acquisition', 'label': 'Exit Rate (Lineup)', 'row_offset': 22, 'is_rate': True},

    # Behavior
    'avg_session_duration': {'section': 'Behavior', 'label': 'Avg Session Duration (sec)', 'row_offset': 23},
    'event_click_total': {'section': 'Behavior', 'label': 'Total Event Clicks', 'row_offset': 24},
    'click_gnb_search': {'section': 'Behavior', 'label': 'GNB & Search Clicks', 'row_offset': 25},
    'click_anchor_tab': {'section': 'Behavior', 'label': 'Anchor Tab Clicks', 'row_offset': 26},
    'click_features': {'section': 'Behavior', 'label': 'Features Clicks', 'row_offset': 29},
    'click_per_session_total': {'section': 'Behavior', 'label': 'Total Click/Session', 'row_offset': 99, 'is_rate': True},
    'click_per_session_content': {'section': 'Behavior', 'label': 'Content Click/Session', 'row_offset': 100, 'is_rate': True},

    # Conversion
    'plp_conversion': {'section': 'Conversion', 'label': 'PLP Conversion Rate', 'row_offset': 104, 'is_rate': True},
    'product_conversion': {'section': 'Conversion', 'label': 'Product Conversion Rate', 'row_offset': 105, 'is_rate': True},
    'conv_lineup_sessions': {'section': 'Conversion', 'label': 'Lineup Sessions (Conv)', 'row_offset': 106},
    'conv_plp': {'section': 'Conversion', 'label': 'PLP Visits', 'row_offset': 107},
    'conv_pdp': {'section': 'Conversion', 'label': 'PDP/PBP Visits', 'row_offset': 108},
    'purchase_intent_conversion': {'section': 'Conversion', 'label': 'Purchase Intent Conv Rate', 'row_offset': 109, 'is_rate': True},
    'purchase_conversion': {'section': 'Conversion', 'label': 'Purchase Conv Rate', 'row_offset': 110, 'is_rate': True},
    'conv_add_to_cart': {'section': 'Conversion', 'label': 'Add to Cart / Check-out', 'row_offset': 112},
    'conv_purchase': {'section': 'Conversion', 'label': 'Purchase', 'row_offset': 113},
}


def find_year_row(ws, target_year=2026, max_row=100):
    """Find the row where the year header starts."""
    for row_idx in range(1, max_row + 1):
        cell_val = ws.cell(row=row_idx, column=2).value
        if cell_val == target_year:
            return row_idx
    return None


def extract_metrics_dynamic(ws, year_row):
    """Dynamically extract metrics by scanning the structure from the year row."""
    metrics = {}

    # Build month->column mapping from header row
    month_cols = []
    for col_idx in range(8, 20):  # H through S (Jan-Dec)
        month_name = ws.cell(row=year_row, column=col_idx).value
        if month_name and month_name in MONTH_ORDER:
            month_cols.append((month_name, col_idx))

    # First pass: determine which months have real data by checking key rows
    # (Page View Lineup row, typically 3-4 rows after year header)
    months_with_real_data = set()
    for check_offset in [3, 4, 5, 6, 7]:  # check several rows
        check_row = year_row + check_offset
        for month_name, col_idx in month_cols:
            val = ws.cell(row=check_row, column=col_idx).value
            if val is not None and val != 'N/A' and val != 0 and val != '#DIV/0!':
                try:
                    fval = float(val)
                    if fval > 0:
                        months_with_real_data.add(month_name)
                except (ValueError, TypeError):
                    pass

    # Only use months that have real data
    active_month_cols = [(m, c) for m, c in month_cols if m in months_with_real_data]

    # Scan rows after year header to build metric structure
    current_section = None
    row_idx = year_row + 1
    months_with_data = set()

    while row_idx <= ws.max_row:
        col_b = ws.cell(row=row_idx, column=2).value
        col_c = ws.cell(row=row_idx, column=3).value

        # Check if we hit next year section or empty gap
        if col_b and isinstance(col_b, (int, float)):
            break

        # Section headers: Acquisition, Behavior, Conversion
        if col_b in ('Acquisition', 'Behavior', 'Conversion'):
            current_section = col_b

        # Extract metric name from column C (or B for section-level)
        metric_name = None
        if col_c and isinstance(col_c, str):
            metric_name = col_c
        elif col_b and isinstance(col_b, str) and col_b not in ('Acquisition', 'Behavior', 'Conversion'):
            metric_name = col_b

        if metric_name and current_section:
            raw_name = metric_name
            monthly_data = {}
            for month_name, col_idx in active_month_cols:
                val = ws.cell(row=row_idx, column=col_idx).value
                if val is not None and val != 'N/A':
                    try:
                        monthly_data[month_name] = float(val)
                        months_with_data.add(month_name)
                    except (ValueError, TypeError):
                        pass

            if monthly_data:
                # Create a clean key
                stripped = raw_name.strip()
                clean_key = re.sub(r'[^\w\s]', '', stripped).strip().lower().replace(' ', '_')
                indent_level = len(raw_name) - len(raw_name.lstrip())

                # Handle duplicate keys by appending section
                if clean_key in metrics:
                    clean_key = f"{clean_key}_{current_section.lower()}"

                metrics[clean_key] = {
                    'section': current_section,
                    'label': stripped,
                    'indent': indent_level // 5,
                    'monthly': monthly_data
                }

        row_idx += 1

    # Only return months that actually have data
    valid_months = [m for m, _ in month_cols if m in months_with_data]
    return metrics, valid_months


def parse_master_file():
    """Parse the master link file for page status info."""
    master_files = glob.glob(os.path.join(DATA_DIR, 'LG_Buying_Guides_Master*Link*.xlsx'))
    if not master_files:
        return []

    wb = openpyxl.load_workbook(master_files[0], data_only=True)
    ws = wb[wb.sheetnames[0]]

    pages = []
    current_country = None

    for row in ws.iter_rows(min_row=2, max_row=ws.max_row, values_only=False):
        vals = [cell.value for cell in row]

        # Country in column B (index 1)
        if vals[1]:
            current_country = str(vals[1]).strip()

        category = vals[2]  # 구분 (TV, 모니터, etc.)
        detail = vals[3]    # 상세 (Lineup Guide, Feature Library)
        link = vals[4]      # 링크
        feedback = vals[5]  # 피드백 반영 여부
        ga_tag = vals[6]    # GA 태깅
        live_status = vals[7]  # 라이브 상태
        breadcrumb = vals[8]   # 브레드크럼 노출

        if detail and link:
            pages.append({
                'country': current_country or '',
                'category': str(category).strip() if category else '',
                'type': str(detail).strip(),
                'link': str(link).strip(),
                'feedback': str(feedback).strip() if feedback else '',
                'ga_tagged': str(ga_tag).strip() if ga_tag else '',
                'live_status': str(live_status).strip() if live_status else '',
                'breadcrumb': str(breadcrumb).strip() if breadcrumb else '',
            })

    return pages


CONTENT_TYPES = {
    '라인업가이드': 'lineup',
    '스펙라이브러리': 'feature_library',
}


def find_monthly_files_by_type():
    """Find monthly files grouped by content type and sorted by month."""
    from pathlib import Path
    files = [str(f) for f in Path(DATA_DIR).glob('Monthly*.xlsx')]

    result = {}  # content_type -> list of (filepath, month_idx, month_name)
    for f in files:
        basename = unicodedata.normalize('NFC', os.path.basename(f))
        # Determine content type
        ctype = 'lineup'  # default
        for kr_name, eng_type in CONTENT_TYPES.items():
            if kr_name in basename:
                ctype = eng_type
                break

        for i, month in enumerate(MONTH_ORDER):
            if month in basename:
                result.setdefault(ctype, []).append((f, i, month))
                break

    # Sort each type by month
    for ctype in result:
        result[ctype].sort(key=lambda x: x[1])

    return result


def parse_single_workbook(filepath, content_type):
    """Parse a single workbook (lineup or feature library)."""
    wb = openpyxl.load_workbook(filepath, data_only=True)
    all_data = {}
    all_months = set()

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]

        parts = sheet_name.split('_', 1)
        if len(parts) != 2:
            continue

        category = parts[0]  # TV or Monitor
        country_code = parts[1]  # UK, FR, etc.

        year_row = find_year_row(ws, 2026)
        if not year_row:
            print(f"  Warning: No 2026 data found in {sheet_name}")
            continue

        metrics, months_2026 = extract_metrics_dynamic(ws, year_row)
        all_months.update(months_2026)

        year_row_2025 = find_year_row(ws, 2025)
        metrics_2025 = {}
        if year_row_2025:
            metrics_2025, _ = extract_metrics_dynamic(ws, year_row_2025)

        # Extract Data Insights text
        insights_text = []
        for row_idx in range(18, 30):
            cell_val = ws.cell(row=row_idx, column=2).value
            if cell_val and isinstance(cell_val, str):
                text = cell_val.strip()
                if text.startswith('<Data Insights>'):
                    continue
                col_c = ws.cell(row=row_idx, column=3).value
                if col_c:
                    text = text + ' ' + str(col_c).strip()
                if text:
                    insights_text.append(text)

        # Use content_type prefix to avoid key collisions
        data_key = f"{content_type}_{sheet_name}"
        all_data[data_key] = {
            'category': category,
            'country': country_code,
            'content_type': content_type,
            'metrics_2026': metrics,
            'metrics_2025': metrics_2025,
            'insights': insights_text,
        }

    return all_data, all_months


def parse_monthly_data():
    """Parse all monthly files (lineup + feature library)."""
    files_by_type = find_monthly_files_by_type()
    if not files_by_type:
        return {}, []

    all_data = {}
    all_months = set()

    for ctype, file_list in files_by_type.items():
        latest_file, _, latest_month = file_list[-1]
        print(f"  Reading {ctype}: {os.path.basename(latest_file)} (up to {latest_month})")

        data, months = parse_single_workbook(latest_file, ctype)
        all_data.update(data)
        all_months.update(months)

    sorted_months = sorted(all_months, key=lambda m: MONTH_ORDER.index(m))
    return all_data, sorted_months


def _get_metric(metrics, *keywords):
    """Find metric value helper."""
    for k, v in metrics.items():
        lk = k.lower()
        if all(kw in lk for kw in keywords):
            return v
    return None


def _get_val(metrics, month, *keywords):
    m = _get_metric(metrics, *keywords)
    if m and month in m.get('monthly', {}):
        return m['monthly'][month]
    return None


def _get_session_val(metrics, month):
    """Get session value - works for both lineup and spec library."""
    return (_get_val(metrics, month, 'lineup', 'session')
            or _get_val(metrics, month, 'tv_lineup', 'session')
            or _get_val(metrics, month, 'monitor_lineup', 'session')
            or _get_val(metrics, month, 'spec_library', 'session')
            or _get_val(metrics, month, 'tv_spec', 'session')
            or _get_val(metrics, month, 'monitor_spec', 'session'))


def compute_insights(all_data, months):
    """Generate simple metric-level insights."""
    insights = []
    if len(months) < 2:
        return insights

    latest, prev = months[-1], months[-2]
    for sheet_key, data in all_data.items():
        metrics = data['metrics_2026']
        ctype_label = 'Feature Library' if data.get('content_type') == 'feature_library' else 'Lineup Guide'
        label = f"{data['category']} {data['country']} ({ctype_label})"

        sess_cur = _get_session_val(metrics, latest)
        sess_prev = _get_session_val(metrics, prev)
        if sess_cur and sess_prev and sess_prev > 0:
            ch = ((sess_cur - sess_prev) / sess_prev) * 100
            if abs(ch) > 20:
                insights.append({
                    'type': 'traffic' if ch > 0 else 'warning', 'page': label, 'metric': 'Sessions',
                    'content_type': data.get('content_type', 'lineup'),
                    'message': f"{label}: Sessions {'increased' if ch>0 else 'decreased'} {abs(ch):.0f}% ({prev}: {sess_prev:.0f} → {latest}: {sess_cur:.0f})",
                    'priority': 'high' if abs(ch)>50 else 'medium'
                })

        for ck in metrics:
            if 'plp_conversion' in ck:
                m = metrics[ck]
                cv, pv = m['monthly'].get(latest), m['monthly'].get(prev)
                if cv and pv and pv > 0 and cv <= 1:
                    ch = ((cv - pv) / pv) * 100
                    if abs(ch) > 15:
                        insights.append({
                            'type': 'success' if ch>0 else 'warning', 'page': label, 'metric': 'PLP Conversion',
                            'content_type': data.get('content_type', 'lineup'),
                            'message': f"{label}: PLP Conv {'improved' if ch>0 else 'declined'} {abs(ch):.1f}% ({prev}: {pv:.1%} → {latest}: {cv:.1%})",
                            'priority': 'high' if abs(ch)>30 else 'medium'
                        })
                break
    return insights


def compute_expert_analysis(all_data, months, pages):
    """Generate comprehensive expert-level analysis for both lineup and feature library."""
    if not months:
        return {}

    latest = months[-1]
    prev = months[-2] if len(months) > 1 else None

    # ── Build per-sheet summary ──
    sheet_summaries = {}
    for sheet_key, data in all_data.items():
        m = data['metrics_2026']
        ctype = data.get('content_type', 'lineup')

        s = {
            'content_type': ctype,
            'sessions': _get_session_val(m, latest),
            'sessions_prev': _get_session_val(m, prev) if prev else None,
            'page_views': _get_val(m, latest, 'line_up') or _get_val(m, latest, 'lineup') or _get_val(m, latest, 'spec_library'),
            'engaged': _get_val(m, latest, 'engaged'),
            'event_clicks': _get_val(m, latest, 'event_click') or _get_val(m, latest, 'guide_event_click') or _get_val(m, latest, 'library_event_click'),
            'event_clicks_prev': (_get_val(m, prev, 'event_click') or _get_val(m, prev, 'guide_event_click') or _get_val(m, prev, 'library_event_click')) if prev else None,
            'duration': _get_val(m, latest, 'avg_session_duration') or _get_val(m, latest, 'session_duration'),
            'duration_prev': (_get_val(m, prev, 'avg_session_duration') or _get_val(m, prev, 'session_duration')) if prev else None,
            'plp_conv': _get_val(m, latest, 'plp_conversion'),
            'plp_conv_prev': _get_val(m, prev, 'plp_conversion') if prev else None,
            'product_conv': _get_val(m, latest, 'product_conversion'),
            'purchase_conv': _get_val(m, latest, 'purchase_conversion'),
            'purchase_conv_prev': _get_val(m, prev, 'purchase_conversion') if prev else None,
            'purchase_count': _get_val(m, latest, 'purchase') if _get_val(m, latest, 'purchase') and _get_val(m, latest, 'purchase') < 1000 else None,
            'organic': _get_val(m, latest, 'organic'),
            'external': _get_val(m, latest, 'external'),
            'internal': _get_val(m, latest, 'internal'),
            'engagement_rate': _get_val(m, latest, 'engagem'),
            'exit_rate': _get_val(m, latest, 'exit_rate'),
        }
        if s['page_views'] and s['page_views'] > 5000:
            s['page_views'] = None
        sheet_summaries[sheet_key] = s

    # ── Build reports per content type ──
    content_types_found = sorted(set(d.get('content_type', 'lineup') for d in all_data.values()))

    def _build_reports_for(ct_filter=None):
        """Build country/category/executive for a content type (or all)."""
        filtered_data = {k: v for k, v in all_data.items()
                        if ct_filter is None or v.get('content_type', 'lineup') == ct_filter}
        filtered_sums = {k: v for k, v in sheet_summaries.items() if k in filtered_data}

        # Country reports
        cr = {}
        countries = sorted(set(d['country'] for d in filtered_data.values()))
        for country in countries:
            sheets_c = {k: v for k, v in filtered_data.items() if v['country'] == country}
            sums_c = {k: filtered_sums[k] for k in sheets_c if k in filtered_sums}

            total_sessions = sum(s.get('sessions') or 0 for s in sums_c.values())
            total_sessions_prev = sum(s.get('sessions_prev') or 0 for s in sums_c.values()) if prev else 0
            total_clicks = sum(s.get('event_clicks') or 0 for s in sums_c.values())
            avg_plp = [s['plp_conv'] for s in sums_c.values() if s.get('plp_conv') is not None]
            avg_plp_val = sum(avg_plp)/len(avg_plp) if avg_plp else None
            avg_purch = [s['purchase_conv'] for s in sums_c.values() if s.get('purchase_conv') is not None]
            avg_purch_val = sum(avg_purch)/len(avg_purch) if avg_purch else None
            total_purchases = sum(s.get('purchase_count') or 0 for s in sums_c.values())
            avg_dur = [s['duration'] for s in sums_c.values() if s.get('duration')]
            avg_dur_val = sum(avg_dur)/len(avg_dur) if avg_dur else None

            sess_change = ((total_sessions - total_sessions_prev) / total_sessions_prev * 100) if total_sessions_prev > 0 else None
            status = 'stable'
            if sess_change is not None:
                if sess_change > 10: status = 'growing'
                elif sess_change < -20: status = 'declining'

            country_pages = [p for p in pages if country in p.get('country', '')]
            country_kr = COUNTRY_MAP.get(country, country)
            if not country_pages:
                country_pages = [p for p in pages if country_kr and any(c in p.get('country', '') for c in [country_kr, country])]

            ctype_label = {'lineup': 'Lineup Guide', 'feature_library': 'Spec Library'}.get(ct_filter, '')
            analyst_notes = []
            for k, sd in sheets_c.items():
                if sd.get('insights'):
                    meaningful = [n for n in sd['insights'] if len(str(n)) > 20]
                    if meaningful:
                        label = f"{sd['category']} {ctype_label}" if ctype_label else f"{sd['category']}"
                        analyst_notes.append({'page': label, 'notes': meaningful})

            per_product = {}
            for k, sd in sheets_c.items():
                s = sums_c.get(k, {})
                per_product[sd['category']] = {
                    'sessions': s.get('sessions'),
                    'page_views': s.get('page_views'),
                    'plp_conv': s.get('plp_conv'),
                    'purchase_conv': s.get('purchase_conv'),
                    'duration': s.get('duration'),
                    'clicks': s.get('event_clicks'),
                    'engagement_rate': s.get('engagement_rate'),
                }

            cr[country] = {
                'total_sessions': total_sessions,
                'session_change_pct': round(sess_change, 1) if sess_change else None,
                'total_clicks': total_clicks,
                'avg_plp_conv': round(avg_plp_val, 4) if avg_plp_val else None,
                'avg_purchase_conv': round(avg_purch_val, 4) if avg_purch_val else None,
                'total_purchases': total_purchases,
                'avg_duration': round(avg_dur_val, 1) if avg_dur_val else None,
                'status': status,
                'links': country_pages,
                'analyst_notes': analyst_notes,
                'per_product': per_product,
            }

        # Category reports
        cat_reps = {}
        categories = sorted(set(d['category'] for d in filtered_data.values()))
        for cat in categories:
            cat_sheets = {k: v for k, v in filtered_data.items() if v['category'] == cat}
            cat_sums = {k: filtered_sums[k] for k in cat_sheets if k in filtered_sums}

            ranked_sessions = sorted(
                [(filtered_data[k]['country'], s.get('sessions') or 0) for k, s in cat_sums.items()],
                key=lambda x: x[1], reverse=True)
            ranked_plp = sorted(
                [(filtered_data[k]['country'], s.get('plp_conv') or 0) for k, s in cat_sums.items() if s.get('plp_conv') is not None],
                key=lambda x: x[1], reverse=True)
            ranked_duration = sorted(
                [(filtered_data[k]['country'], s.get('duration') or 0) for k, s in cat_sums.items() if s.get('duration')],
                key=lambda x: x[1], reverse=True)

            total_sess = sum(s.get('sessions') or 0 for s in cat_sums.values())
            avg_plp_list = [s['plp_conv'] for s in cat_sums.values() if s.get('plp_conv')]
            avg_plp_cat = sum(avg_plp_list)/len(avg_plp_list) if avg_plp_list else None

            cat_reps[cat] = {
                'total_sessions': total_sess,
                'avg_plp_conv': round(avg_plp_cat, 4) if avg_plp_cat else None,
                'countries_count': len(cat_sheets),
                'ranked_sessions': ranked_sessions[:5],
                'ranked_plp': ranked_plp[:5],
                'ranked_duration': ranked_duration[:5],
            }

        # Executive
        total_sess_all = sum(s.get('sessions') or 0 for s in filtered_sums.values())
        total_sess_prev_all = sum(s.get('sessions_prev') or 0 for s in filtered_sums.values()) if prev else 0
        total_clicks_all = sum(s.get('event_clicks') or 0 for s in filtered_sums.values())
        all_plp = [s['plp_conv'] for s in filtered_sums.values() if s.get('plp_conv') is not None]
        all_purch = [s['purchase_conv'] for s in filtered_sums.values() if s.get('purchase_conv') is not None]
        g_change = ((total_sess_all - total_sess_prev_all) / total_sess_prev_all * 100) if total_sess_prev_all > 0 else None

        top_plp = sorted([(k, s.get('plp_conv', 0)) for k, s in filtered_sums.items() if s.get('plp_conv')], key=lambda x: x[1], reverse=True)
        low_plp = sorted([(k, s.get('plp_conv', 0)) for k, s in filtered_sums.items() if s.get('plp_conv')], key=lambda x: x[1])

        def _clean_key(k):
            # Remove content type prefix for display
            for prefix in ['lineup_', 'feature_library_']:
                if k.startswith(prefix):
                    k = k[len(prefix):]
            return k.replace('_', ' ')

        executive = {
            'latest_month': latest,
            'prev_month': prev,
            'total_sessions': total_sess_all,
            'session_change_pct': round(g_change, 1) if g_change else None,
            'total_clicks': total_clicks_all,
            'avg_plp_conv': round(sum(all_plp)/len(all_plp), 4) if all_plp else None,
            'avg_purchase_conv': round(sum(all_purch)/len(all_purch), 4) if all_purch else None,
            'total_pages_tracked': len(filtered_sums),
            'top_plp_pages': [(_clean_key(k), round(v, 4)) for k, v in top_plp[:3]],
            'low_plp_pages': [(_clean_key(k), round(v, 4)) for k, v in low_plp[:3]],
        }

        return {'executive': executive, 'country_reports': cr, 'category_reports': cat_reps}

    # Build overall + per content type
    overall = _build_reports_for(None)
    per_content_type = {}
    for ct in content_types_found:
        per_content_type[ct] = _build_reports_for(ct)

    return {
        'executive': overall['executive'],
        'country_reports': overall['country_reports'],
        'category_reports': overall['category_reports'],
        'content_types': content_types_found,
        'per_content_type': per_content_type,
        'sheet_summaries': sheet_summaries,
    }


def main():
    print("=" * 60)
    print("LG Buying Guides Dashboard - Data Parser")
    print("=" * 60)

    # Parse master file
    print("\n[1/3] Parsing master link file...")
    pages = parse_master_file()
    print(f"  Found {len(pages)} page entries")

    # Parse monthly data
    print("\n[2/3] Parsing monthly GA data...")
    monthly_data, months = parse_monthly_data()
    print(f"  Parsed {len(monthly_data)} sheets, months: {months}")

    # Compute insights
    print("\n[3/4] Computing insights...")
    insights = compute_insights(monthly_data, months)
    print(f"  Generated {len(insights)} insights")

    # Expert analysis
    print("\n[4/4] Generating expert analysis...")
    expert = compute_expert_analysis(monthly_data, months, pages)
    print(f"  Country reports: {len(expert.get('country_reports', {}))}")
    print(f"  Category reports: {len(expert.get('category_reports', {}))}")

    # Build output
    output = {
        'meta': {
            'months_available': months,
            'sheets_parsed': list(monthly_data.keys()),
            'last_updated': None,
            'countries': sorted(set(d['country'] for d in monthly_data.values())),
            'categories': sorted(set(d['category'] for d in monthly_data.values())),
            'content_types': sorted(set(d.get('content_type', 'lineup') for d in monthly_data.values())),
        },
        'pages': pages,
        'monthly_data': {},
        'insights': insights,
        'expert': expert,
    }

    # Serialize monthly data (convert for JSON)
    for key, data in monthly_data.items():
        output['monthly_data'][key] = {
            'category': data['category'],
            'country': data['country'],
            'content_type': data.get('content_type', 'lineup'),
            'metrics_2026': data['metrics_2026'],
            'metrics_2025': data['metrics_2025'],
            'insights': data['insights'],
        }

    # Write JSON
    output_path = os.path.join(DATA_DIR, 'data.json')
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=2, default=str)

    print(f"\nOutput written to: {output_path}")
    print(f"File size: {os.path.getsize(output_path) / 1024:.1f} KB")
    return output_path


if __name__ == '__main__':
    main()
