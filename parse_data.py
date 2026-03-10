#!/usr/bin/env python3
"""
LG Buying Guides GA Data Parser
Reads monthly xlsx files and master link file, outputs data.json for the dashboard.
"""

import json
import os
import re
import glob
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


def find_latest_monthly_file():
    """Find all monthly files and return them sorted by month."""
    # Use pathlib for better Unicode support
    from pathlib import Path
    files = [str(f) for f in Path(DATA_DIR).glob('Monthly*.xlsx')]

    file_month_pairs = []
    for f in files:
        basename = os.path.basename(f)
        for i, month in enumerate(MONTH_ORDER):
            if month in basename:
                file_month_pairs.append((f, i, month))
                break

    file_month_pairs.sort(key=lambda x: x[1])
    return file_month_pairs


def parse_monthly_data():
    """Parse the latest monthly file (which is cumulative)."""
    monthly_files = find_latest_monthly_file()
    if not monthly_files:
        return {}, []

    # Use the latest file (most cumulative data)
    latest_file, _, latest_month = monthly_files[-1]
    print(f"Reading latest monthly file: {os.path.basename(latest_file)} (up to {latest_month})")

    wb = openpyxl.load_workbook(latest_file, data_only=True)

    all_data = {}
    all_months = set()

    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]

        # Parse sheet name: "TV_UK", "Monitor_FR", etc.
        parts = sheet_name.split('_', 1)
        if len(parts) != 2:
            continue

        category = parts[0]  # TV or Monitor
        country_code = parts[1]  # UK, FR, etc.

        # Find year 2026 row
        year_row = find_year_row(ws, 2026)
        if not year_row:
            print(f"  Warning: No 2026 data found in {sheet_name}")
            continue

        metrics, months_2026 = extract_metrics_dynamic(ws, year_row)
        all_months.update(months_2026)

        # Also try to get 2025 data for YoY comparison
        year_row_2025 = find_year_row(ws, 2025)
        metrics_2025 = {}
        if year_row_2025:
            metrics_2025, _ = extract_metrics_dynamic(ws, year_row_2025)

        # Also extract the Data Insights text if available
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

        all_data[sheet_name] = {
            'category': category,
            'country': country_code,
            'metrics_2026': metrics,
            'metrics_2025': metrics_2025,
            'insights': insights_text,
        }

    sorted_months = sorted(all_months, key=lambda m: MONTH_ORDER.index(m))
    return all_data, sorted_months


def compute_insights(all_data, months):
    """Generate actionable insights from the data."""
    insights = []

    if len(months) < 2:
        return insights

    latest = months[-1]
    prev = months[-2]

    for sheet_key, data in all_data.items():
        metrics = data['metrics_2026']
        cat = data['category']
        country = data['country']
        label = f"{cat} {country}"

        # Check session trends
        session_key = None
        for k in metrics:
            if 'lineup' in k and 'session' in k.lower() and metrics[k].get('indent', 0) <= 1:
                session_key = k
                break

        if session_key and latest in metrics[session_key]['monthly'] and prev in metrics[session_key]['monthly']:
            curr_val = metrics[session_key]['monthly'][latest]
            prev_val = metrics[session_key]['monthly'][prev]
            if prev_val > 0:
                change_pct = ((curr_val - prev_val) / prev_val) * 100
                if abs(change_pct) > 20:
                    direction = 'increased' if change_pct > 0 else 'decreased'
                    insights.append({
                        'type': 'traffic' if change_pct > 0 else 'warning',
                        'page': label,
                        'metric': 'Lineup Sessions',
                        'message': f"{label}: Lineup sessions {direction} by {abs(change_pct):.0f}% ({prev}: {prev_val:.0f} → {latest}: {curr_val:.0f})",
                        'priority': 'high' if abs(change_pct) > 50 else 'medium'
                    })

        # Check conversion trends
        for conv_key in metrics:
            if 'conversion' in conv_key and metrics[conv_key].get('section') == 'Conversion':
                m = metrics[conv_key]
                if latest in m['monthly'] and prev in m['monthly']:
                    curr_val = m['monthly'][latest]
                    prev_val = m['monthly'][prev]
                    if prev_val > 0 and curr_val <= 1:  # rate metrics
                        change_pct = ((curr_val - prev_val) / prev_val) * 100
                        if abs(change_pct) > 15:
                            direction = 'improved' if change_pct > 0 else 'declined'
                            insights.append({
                                'type': 'success' if change_pct > 0 else 'warning',
                                'page': label,
                                'metric': m['label'],
                                'message': f"{label}: {m['label']} {direction} by {abs(change_pct):.1f}% ({prev}: {prev_val:.2%} → {latest}: {curr_val:.2%})",
                                'priority': 'high' if abs(change_pct) > 30 else 'medium'
                            })
                break  # only first conversion metric per page

        # Check engagement
        for eng_key in metrics:
            if 'engagement_rate' in eng_key or 'engagemet_rate' in eng_key:
                m = metrics[eng_key]
                if latest in m['monthly']:
                    val = m['monthly'][latest]
                    if val < 0.5:
                        insights.append({
                            'type': 'warning',
                            'page': label,
                            'metric': 'Engagement Rate',
                            'message': f"{label}: Low engagement rate ({val:.1%}) - consider improving content or UX",
                            'priority': 'medium'
                        })
                break

    return insights


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
    print("\n[3/3] Computing insights...")
    insights = compute_insights(monthly_data, months)
    print(f"  Generated {len(insights)} insights")

    # Build output
    output = {
        'meta': {
            'months_available': months,
            'sheets_parsed': list(monthly_data.keys()),
            'last_updated': None,  # will be set by server
            'countries': sorted(set(d['country'] for d in monthly_data.values())),
            'categories': sorted(set(d['category'] for d in monthly_data.values())),
        },
        'pages': pages,
        'monthly_data': {},
        'insights': insights,
    }

    # Serialize monthly data (convert for JSON)
    for key, data in monthly_data.items():
        output['monthly_data'][key] = {
            'category': data['category'],
            'country': data['country'],
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
