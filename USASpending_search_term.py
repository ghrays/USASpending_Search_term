import streamlit as st
import requests
import pandas as pd
import numpy as np
import logging
import zipfile
import time
from io import BytesIO
from datetime import datetime
from dateutil import parser as date_parser
from pathlib import Path
#import pygsheets
import re
import json

# --- Configuration Constants ---
DOWNLOAD_URL = "https://api.usaspending.gov/api/v2/download/awards/"
STATUS_URL_BASE = "https://api.usaspending.gov/api/v2/download/status/"
HEADERS = {
    "Content-Type": "application/json",
    "User-Agent": "Mozilla/5.0"
}
CONFIG = {
    "google_credentials": str(Path(__file__).parent / 'creds' / 'google_sheets.json'),
    "source_sheet_id": "1j_EiY0mQKmqhHy9lC0wvd_Vs_p94yootNv180R6vZ7Y"
}

# Award type code groups
type_filters = {
    'contract':     ["A","B","C","D"],
    'contract_idv': ["IDV_A","IDV_B","IDV_B_A","IDV_B_B","IDV_B_C","IDV_C","IDV_D","IDV_E"],
    'grant':        ["02","03","04","05"]
}

# Static filters
static_filters = {
    "keywords":     ["wall street journal", "WSJ"],
    "time_period": [
        {"start_date": "2007-10-01", "end_date": "2025-09-30"}
    ]
}

# Download payload template (per your PR format)
download_payload_template = {
    "filters": {},
    "page": 1,
    "limit": 100,
    "sort": "Award Amount",
    "order": "desc",
    "auditTrail": "Results Table - Spending by award search",
    "fields": [
        "Award ID","Recipient Name","Award Amount","Total Outlays","Description",
        "Contract Award Type","Recipient UEI","Recipient Location",
        "Primary Place of Performance","def_codes","COVID-19 Obligations",
        "COVID-19 Outlays","Infrastructure Obligations","Infrastructure Outlays",
        "Awarding Agency","Awarding Sub Agency","Start Date","End Date",
        "NAICS","PSC","recipient_id","prime_award_recipient_id"
    ],
    "subawards": False
}

# Columns we ultimately care about downstream
desired_columns = [
    "award_type","award_id_fain","award_id_piid","award_or_idv_flag","parent_award_id_piid",
    "assistance_type_description","recipient_uei","recipient_name","bucket_name","recipient_parent_uei","recipient_parent_name",
    "total_obligated_amount","total_outlayed_amount","total_funding_amount","potential_total_value_of_award",
    "period_of_performance_start_date","period_of_performance_current_end_date","period_of_performance_potential_end_date",
    "ordering_period_end_date","awarding_agency_name","awarding_agency_code","awarding_office_name",
    "awarding_office_code","awarding_sub_agency_name","awarding_sub_agency_code","funding_agency_name","funding_agency_code",
    "funding_sub_agency_name","funding_sub_agency_code","funding_office_name","funding_office_code",
    "treasure_accounts_fundins_this_award","federal_accounts_fundings_this_award","usaspending_permalink",
    "prime_award_base_transaction_description","run_datetime"
]

# ------------------------------------------------------------------------------
#  Logging setup
# ------------------------------------------------------------------------------
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(__name__)

logger = setup_logging()
today = datetime.today()

# ------------------------------------------------------------------------------
#  Download & status‑polling logic
# ------------------------------------------------------------------------------
def download_awards(session, award_codes):
    # build payload from our template
    payload = download_payload_template.copy()
    payload["filters"] = {
        "keywords":       static_filters["keywords"],
        "time_period":    static_filters["time_period"],
        "award_type_codes": award_codes
    }

    # —– SHOW THE REQUEST ON SCREEN —–
    st.sidebar.markdown("### API Request")
    st.sidebar.write("**POST** " + DOWNLOAD_URL)
    st.sidebar.write("**Headers:**")
    st.sidebar.json(dict(session.headers))
    st.sidebar.write("**Payload:**")
    st.sidebar.json(payload)

    msg = f"Submitting download job for award_type_codes={award_codes}"
    logger.info(msg)
    st.sidebar.info(msg)

    resp = session.post(DOWNLOAD_URL, json=payload)
    resp.raise_for_status()

    job_id = resp.json().get('file_name')
    if not job_id:
        raise RuntimeError("Download API did not return a job ID")

    st.sidebar.write(f"Download job ID: {job_id}")
    logger.info(f"Download job ID: {job_id}")

    status_url = f"{STATUS_URL_BASE}?file_name={job_id}&type=awards"
    delay = 1
    status_placeholder = st.sidebar.empty()

    while True:
        status_resp = session.get(status_url)
        status_resp.raise_for_status()
        status = status_resp.json().get('status')
        status_msg = f"Job {job_id} status: {status}"
        logger.info(status_msg)
        status_placeholder.text(status_msg)

        if status == 'finished':
            download_url = status_resp.json().get('url') or status_resp.json().get('file_url')
            if download_url:
                status_placeholder.write("Download URL available, fetching data...")
                logger.info("Download URL available, fetching data...")
                break
        elif status == 'failed':
            error_msg = f"Download job {job_id} failed"
            logger.error(error_msg)
            status_placeholder.error(error_msg)
            raise RuntimeError(error_msg)

        time.sleep(delay)
        status_placeholder.write(f"Waiting {delay}s before retry...")
        delay = min(delay * 2, 30)

    zip_resp = session.get(download_url)
    zip_resp.raise_for_status()
    with zipfile.ZipFile(BytesIO(zip_resp.content)) as zf:
        for name in zf.namelist():
            if name.lower().endswith('.csv'):
                with zf.open(name) as csvfile:
                    return pd.read_csv(csvfile, low_memory=False)
    return pd.DataFrame()

# ------------------------------------------------------------------------------
#  Cleaning & filtering (including client‑side keyword & date logic)
# ------------------------------------------------------------------------------
def clean_and_filter(df, keywords):
    df = df.copy()

    # classify award_type
    conds = [
        df['award_or_idv_flag'].str.upper().eq('IDV')  & df['award_id_piid'].notna(),
        df['award_or_idv_flag'].str.upper().eq('AWARD')& df['award_id_piid'].notna()
    ]
    df['award_type'] = np.select(conds, ['contract_idv','contract'], default='grant')

    # parse dates safely
    def parse_or_none(val):
        if pd.isna(val) or str(val).strip()=='':
            return None
        try:
            return date_parser.parse(str(val))
        except Exception:
            return None

    for col in [
        'period_of_performance_potential_end_date',
        'period_of_performance_current_end_date',
        'ordering_period_end_date'
    ]:
        df[col] = df[col].apply(parse_or_none)

    # keep only future‑relevant records
    df = df.loc[
        ((df['award_type']=='contract')     & (df['period_of_performance_potential_end_date'] > today)) |
        ((df['award_type']=='grant')        & (df['period_of_performance_current_end_date']   > today)) |
        ((df['award_type']=='contract_idv') & (df['ordering_period_end_date']                  > today))
    ].copy()

    # apply keyword filter again client‑side
    if keywords:
        pattern = "|".join(re.escape(k) for k in keywords)
        df = df.loc[
            df['prime_award_base_transaction_description']
              .str.contains(pattern, case=False, na=False)
        ]

    # merge PIID/FAIN for downstream
    df['piid_or_fain'] = (
        df['award_id_fain'].fillna('').astype(str) +
        df['award_id_piid'].fillna('').astype(str)
    )
    df.drop(columns=['award_id_fain','award_id_piid'], inplace=True, errors='ignore')
    return df

# ------------------------------------------------------------------------------
#  Push to Google Sheets
# ------------------------------------------------------------------------------
"""
def update_google_sheets(df):
    df_to_write = df.copy()
    for col in df_to_write.select_dtypes(include=['category']):
        df_to_write[col] = df_to_write[col].astype(str)
    df_to_write.replace('nan', '', inplace=True)

    gc = pygsheets.authorize(service_file=CONFIG["google_credentials"])
    ss = gc.open_by_key(CONFIG["source_sheet_id"])

    try:
        summary_ws = ss.worksheet_by_title("summary")
    except pygsheets.WorksheetNotFound:
        summary_ws = ss.add_worksheet("summary", index=0)
    summary_ws.update_value('A1', 'Last Updated')
    summary_ws.update_value('B1', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    try:
        results_ws = ss.worksheet_by_title("results")
        results_ws.clear()
    except pygsheets.WorksheetNotFound:
        results_ws = ss.add_worksheet("results", index=1)

    results_ws.set_dataframe(df_to_write, 'A1', nan='')
    msg = f"Google sheet updated with {len(df_to_write)} rows"
    logger.info(msg)
    st.sidebar.success(msg)
"""
# ------------------------------------------------------------------------------
#  Streamlit UI
# ------------------------------------------------------------------------------
st.set_page_config(page_title="USAspending Awards Explorer", layout="wide")
st.title("USAspending Awards Explorer")

st.sidebar.header("Search Configuration")
keywords_input = st.sidebar.text_input(
    "Keywords (comma-separated)",
    ", ".join(static_filters["keywords"])
)
keywords = [k.strip() for k in keywords_input.split(',') if k.strip()]

if st.sidebar.button("Fetch Awards"):
    session = requests.Session()
    session.headers.update(HEADERS)

    data_frames = []
    total_types = len(type_filters)
    status_main  = st.sidebar.empty()
    progress_bar = st.sidebar.progress(0)

    for idx, (atype, codes) in enumerate(type_filters.items(), start=1):
        status_main.text(f"Fetching {atype} awards ({idx}/{total_types})...")
        df = download_awards(session, codes)
        if df.empty:
            st.sidebar.warning(f"No data for '{atype}' awards")
        else:
            status_main.success(f"Fetched {len(df)} records for {atype}")
            df['award_type'] = atype
            data_frames.append(df)
        progress_bar.progress(int(idx / total_types * 100))

    if not data_frames:
        status_main.error("No data downloaded for any award type.")
        st.error("No data downloaded for any award type.")
    else:
        status_main.text("Combining and filtering data...")
        combined = pd.concat(data_frames, ignore_index=True)
        combined = combined[[c for c in desired_columns if c in combined.columns]]
        filtered_df = clean_and_filter(combined, keywords)
        status_main.success(f"Found {len(filtered_df)} records matching your criteria.")
        st.dataframe(filtered_df)

        if st.sidebar.button("Update Google Sheet"):
            status_main.text("Updating Google Sheet...")
            update_google_sheets(filtered_df)
