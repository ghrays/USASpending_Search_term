def download_awards(session, award_codes):
    # build payload from our template
    payload = download_payload_template.copy()
    payload["filters"] = {
        "keywords":       static_filters["keywords"],
        "time_period":    static_filters["time_period"],
        "award_type_codes": award_codes
    }

    msg = f"Submitting download job for award_type_codes={award_codes}"
    logger.info(msg)
    st.sidebar.info(msg)

    # --- POST with error inspection ---
    try:
        resp = session.post(DOWNLOAD_URL, json=payload)
        resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        # Log full response
        code = e.response.status_code
        text = e.response.text
        logger.error(f"POST {DOWNLOAD_URL} failed {code}: {text}")
        st.sidebar.error(f"Error {code} fetching data: {text}")
        return pd.DataFrame()

    job_id = resp.json().get('file_name')
    if not job_id:
        err = resp.json()
        logger.error(f"No job_id in response: {err}")
        st.sidebar.error(f"Unexpected API response: {json.dumps(err)}")
        return pd.DataFrame()

    logger.info(f"Download job ID: {job_id}")
    status_url = f"{STATUS_URL_BASE}?file_name={job_id}&type=awards"
    delay = 1
    status_placeholder = st.sidebar.empty()

    # --- Polling loop with error inspection ---
    while True:
        try:
            status_resp = session.get(status_url)
            status_resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            code = e.response.status_code
            text = e.response.text
            logger.error(f"GET {status_url} failed {code}: {text}")
            status_placeholder.error(f"Error checking job status {code}: {text}")
            return pd.DataFrame()

        status = status_resp.json().get('status')
        status_msg = f"Job {job_id} status: {status}"
        logger.info(status_msg)
        status_placeholder.text(status_msg)

        if status == 'finished':
            download_url = status_resp.json().get('url') or status_resp.json().get('file_url')
            if download_url:
                status_placeholder.write("Download URL available, fetching data…")
                logger.info("Download URL available, fetching data…")
                break
        elif status == 'failed':
            error_msg = f"Download job {job_id} failed"
            logger.error(error_msg)
            status_placeholder.error(error_msg)
            return pd.DataFrame()

        time.sleep(delay)
        status_placeholder.write(f"Waiting {delay}s before retry…")
        delay = min(delay * 2, 30)

    # --- Finally fetch the ZIP ---
    try:
        zip_resp = session.get(download_url)
        zip_resp.raise_for_status()
    except requests.exceptions.HTTPError as e:
        code = e.response.status_code
        text = e.response.text
        logger.error(f"Error fetching ZIP {download_url} {code}: {text}")
        st.sidebar.error(f"Error downloading file: {text}")
        return pd.DataFrame()

    with zipfile.ZipFile(BytesIO(zip_resp.content)) as zf:
        for name in zf.namelist():
            if name.lower().endswith('.csv'):
                with zf.open(name) as csvfile:
                    return pd.read_csv(csvfile, low_memory=False)

    return pd.DataFrame()
