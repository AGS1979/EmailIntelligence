import os
import sqlite3
import json
import tempfile
import re
from datetime import datetime, timedelta
import uuid
import io
import base64

import streamlit as st
import pandas as pd
from extract_msg import Message
from openai import OpenAI
import requests
import msal
from thefuzz import fuzz, process
from sqlalchemy import create_engine, text
from azure.storage.blob import BlobServiceClient, generate_blob_sas, BlobSasPermissions
import docx
from bs4 import BeautifulSoup
from htmldocx import HtmlToDocx

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Email Intelligence Extractor",
    page_icon="📧",
    layout="wide"
)

# --- STYLING ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="st-"] {
        font-family: 'Inter', sans-serif;
        color: #262626; /* Darker text for better contrast */
    }
    
    .st-emotion-cache-1y4p8pa { /* Main content area */
        max-width: 100%;
        padding: 2.5rem 4rem; /* Increased padding */
    }

    /* --- Header Container (Removed logo specific styles) --- */
    .header-container {
        display: flex;
        justify-content: space-between;
        align-items: center;
        padding-bottom: 1.5rem; /* More padding below title */
        border-bottom: 1px solid #e0e0e0; /* Lighter border */
        margin-bottom: 2.5rem; /* More space below header */
    }
    
    .app-title {
        font-size: 2.5em; /* Larger, bolder title */
        font-weight: 700;
        color: #1a1a1a;
        letter-spacing: -0.02em; /* Slightly tighter letter spacing */
    }
    
    /* --- Buttons --- */
    .stButton>button {
        border-radius: 8px;
        border: 1px solid #d1d5db;
        background-color: #ffffff;
        color: #374151;
        font-weight: 500; /* Slightly less bold */
        transition: all 0.2s ease;
        padding: 0.6rem 1.2rem; /* Increased padding */
        box-shadow: 0 1px 2px rgba(0,0,0,0.05); /* Subtle shadow */
    }
    
    .stButton>button:hover {
        background-color: #f0f2f6; /* Lighter hover background */
        border-color: #a0a4ac;
        color: #1a1a1a;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1); /* Slightly more pronounced shadow on hover */
    }
    
    /* --- Buttons (Revised for higher specificity) --- */
    button[data-testid="stButton-primary"] {
    background-color: #1e70bf !important; /* Professional blue */
    color: white !important; /* White text */
    border: none !important;
    border-radius: 8px !important;
    padding: 0.6rem 1.2rem !important;
    font-weight: 500 !important;
    box-shadow: 0 2px 4px rgba(0,0,0,0.1) !important;
    transition: all 0.2s ease !important;
    }

    button[data-testid="stButton-primary"]:hover {
    background-color: #155a9b !important; /* Darker blue on hover */
    color: white !important;
    box-shadow: 0 4px 8px rgba(0,0,0,0.15) !important;
    }

    /* --- Containers and Cards --- */
    .st-emotion-cache-16txtl3 { /* This targets st.container(border=True) */
        padding: 2rem; /* Increased padding */
        background-color: #ffffff; /* White background for containers */
        border-radius: 12px;
        border: 1px solid #e0e0e0; /* Light grey border */
        box-shadow: 0 4px 12px rgba(0,0,0,0.05); /* Soft shadow for depth */
        margin-bottom: 1.5rem;
    }

    /* --- Headings --- */
    h1, h2, h3, h4, h5, h6 {
        font-weight: 600; /* Consistent font-weight for headings */
        color: #1a1a1a;
        margin-top: 1.5rem;
        margin-bottom: 0.8rem;
    }
    h2 { font-size: 1.8em; }
    h3 { font-size: 1.3em; margin-top: 1.2rem; }

    /* --- Input Fields --- */
    .stTextInput>div>div>input {
        border-radius: 8px;
        border: 1px solid #d1d5db;
        padding: 0.7rem 1rem;
        font-size: 1rem;
        box-shadow: inset 0 1px 2px rgba(0,0,0,0.03);
    }
    .stTextInput>div>div>input:focus {
        border-color: #1e70bf; /* Highlight focus with primary blue */
        box-shadow: 0 0 0 2px rgba(30, 112, 191, 0.2);
        outline: none;
    }

    /* --- Tabs --- */
    .stTabs [data-baseweb="tab-list"] button {
        background-color: #f8f8f8; /* Lighter background for inactive tabs */
        color: #666666; /* Greyer text for inactive tabs */
        border-radius: 8px 8px 0 0;
        padding: 0.7rem 1.2rem;
        font-weight: 500;
        border: none;
        margin-right: 5px; /* Space between tabs */
        transition: all 0.2s ease;
    }
    .stTabs [data-baseweb="tab-list"] button:hover {
        background-color: #e8e8e8;
        color: #333333;
    }
    .stTabs [data-baseweb="tab-list"] button[aria-selected="true"] {
        background-color: #ffffff; /* White background for active tab */
        color: #1e70bf; /* Primary blue for active tab text */
        border-bottom: 3px solid #1e70bf; /* Blue underline for active tab */
        font-weight: 600;
        box-shadow: none; /* Remove shadow from active tab */
    }
    .stTabs [data-baseweb="tab-panel"] {
        padding-top: 1.5rem;
        border-top: 1px solid #e0e0e0;
        margin-top: -1px; /* Overlap border with active tab */
    }

    /* --- Sidebar Navigation --- */
    .st-emotion-cache-vk3305 { /* Targets the radio button container */
        border-radius: 12px;
        padding: 1rem;
        background-color: #f8f9fa; /* Light background for sidebar nav */
        border: 1px solid #e0e0e0;
    }
    .st-emotion-cache-vk3305 .st-emotion-cache-j7qwjs { /* Individual radio button labels */
        font-weight: 500;
        color: #333333;
        padding: 0.5rem 0.75rem;
        margin: 0.2rem 0;
        border-radius: 6px;
        transition: background-color 0.2s ease;
    }
    .st-emotion-cache-vk3305 .st-emotion-cache-j7qwjs:hover {
        background-color: #eef2f6; /* Hover effect for nav items */
    }
    .st-emotion-cache-vk3305 .st-emotion-cache-j7qwjs.st-emotion-cache-j7qwjs-selected { /* Selected radio button */
        background-color: #e0f2fe; /* Light blue for selected */
        color: #1e70bf; /* Primary blue text for selected */
        font-weight: 600;
    }

    /* --- Streamlit Info/Success/Warning/Error messages --- */
    .st-emotion-cache-1f87rhc.e1dfwjs21 { /* Targets st.info/success etc. */
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 1rem;
    }
    /* Specific styles for info, success, warning for better visual consistency */
    .st-emotion-cache-1f87rhc.e1dfwjs21:has(.stAlert.info) { background-color: #e0f2fe; border-color: #90cdf4; color: #ffffff; }
    .st-emotion-cache-1f87rhc.e1dfwjs21:has(.stAlert.success) { background-color: #d1fae5; border-color: #6ee7b7; color: #065f46; }
    .st-emotion-cache-1f87rhc.e1dfwjs21:has(.stAlert.warning) { background-color: #fef3c7; border-color: #fbbf24; color: #9a3412; }
    .st-emotion-cache-1f87rhc.e1dfwjs21:has(.stAlert.error) { background-color: #fee2e2; border-color: #ef4444; color: #991b1b; }
</style>
""", unsafe_allow_html=True)


# --- CONFIGURATION & CLIENTS ---
try:
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
    AZURE_CLIENT_ID = st.secrets["AZURE_CLIENT_ID"]
    AZURE_TENANT_ID = st.secrets["AZURE_TENANT_ID"]
    AZURE_CLIENT_SECRET = st.secrets["AZURE_CLIENT_SECRET"]
    DB_CONNECTION_STRING = st.secrets["DB_CONNECTION_STRING"]
    
    connect_str = st.secrets["AZURE_STORAGE_CONNECTION_STRING"]
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)
    AZURE_CONTAINER_NAME = st.secrets["AZURE_CONTAINER_NAME"]

except KeyError as e:
    st.error(f"Configuration key not found in Streamlit Secrets: {e}")
    st.stop()

# --- CONSTANTS ---
MASTER_DB_NAME = "Master_Company_List.db"
AUTHORITY = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]

# --- DATABASE SETUP ---
engine = create_engine(DB_CONNECTION_STRING)

def query_db(query, params=None):
    with engine.connect() as conn:
        return pd.read_sql_query(sql=text(query), con=conn, params=params)

def query_local_db(db_name, query, params=None):
    with sqlite3.connect(db_name) as conn:
        return pd.read_sql_query(query, conn, params=params if params else ())

@st.cache_data(ttl=3600)
def get_master_company_data():
    if not os.path.exists(MASTER_DB_NAME):
        return pd.DataFrame()
    return query_local_db(MASTER_DB_NAME, "SELECT * FROM master_companies")

@st.cache_data(ttl=3600)
def get_master_broker_names():
    if not os.path.exists(MASTER_DB_NAME): 
        return []
    df = query_local_db(MASTER_DB_NAME, "SELECT Name FROM master_brokers")
    return df['Name'].tolist() if not df.empty else []

def insert_into_db(data):
    data_lowercase_keys = {key.lower(): value for key, value in data.items()}
    df = pd.DataFrame([data_lowercase_keys])
    with engine.connect() as conn:
        df.to_sql('email_data', con=conn, if_exists='append', index=False)
        conn.commit()

# --- MATCHING LOGIC ---
def normalize_company_name(name):
    if not isinstance(name, str):
        return ""
    name = name.lower()
    suffixes = [
        r'\bholding\b', r'\bholdings\b', r'\bhold\b', r'\bltd\b', r'\binc\b',
        r'\bcorp\b', r'\bcorporation\b', r'\bgroup\b', r'\bplc\b', r'\bco\b'
    ]
    for suffix in suffixes:
        name = re.sub(suffix, '', name, flags=re.IGNORECASE)
    name = re.sub(r'[^\w\s]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    return name

def find_company_in_master(extracted_report, master_df):
    company_name = extracted_report.get("Company")
    ticker = extracted_report.get("Ticker")
    name_columns = ['short_name', 'company_short_name', 'full_company_name', 'acronym']
    
    for col in name_columns:
        if col not in master_df.columns:
            master_df[col] = None

    if ticker and isinstance(ticker, str):
        mask = master_df['ticker'].str.lower() == ticker.lower()
        match = master_df[mask.fillna(False)]
        if not match.empty:
            return match.iloc[0], "Ticker Match"

    if not company_name or not isinstance(company_name, str):
        return None, "No Match (Invalid/Missing Company Name)"

    for col in name_columns:
        if pd.api.types.is_string_dtype(master_df[col]):
            match = master_df[master_df[col].str.lower() == company_name.lower()]
            if not match.empty:
                return match.iloc[0], f"Exact Match ({col})"

    normalized_input = normalize_company_name(company_name)
    if not normalized_input:
        return None, "No Match (Empty Normalized Name)"

    normalized_cols = []
    for col in name_columns:
        normalized_col_name = f'normalized_{col}'
        if normalized_col_name not in master_df.columns:
            master_df[normalized_col_name] = master_df[col].fillna('').apply(normalize_company_name)
        normalized_cols.append(normalized_col_name)

    for norm_col in normalized_cols:
        match = master_df[master_df[norm_col] == normalized_input]
        if not match.empty:
            original_col = norm_col.replace('normalized_', '')
            return match.iloc[0], f"Normalized Match ({original_col})"

    all_substring_matches = pd.DataFrame()
    for norm_col in normalized_cols:
        valid_rows = master_df[master_df[norm_col] != '']
        substring_matches = valid_rows[valid_rows[norm_col].str.contains(normalized_input, na=False)]
        if not substring_matches.empty:
            all_substring_matches = pd.concat([all_substring_matches, substring_matches])
    
    all_substring_matches = all_substring_matches.drop_duplicates()
    if len(all_substring_matches) == 1:
        return all_substring_matches.iloc[0], "Substring Match"

    def get_max_fuzzy_score(row):
        scores = [fuzz.token_set_ratio(company_name, str(row[col])) for col in name_columns if pd.notna(row[col])]
        return max(scores) if scores else 0

    master_df['fuzzy_score'] = master_df.apply(get_max_fuzzy_score, axis=1)
    
    if not master_df.empty:
        best_match = master_df.loc[master_df['fuzzy_score'].idxmax()]
        if best_match['fuzzy_score'] >= 95:
            return best_match, f"Fuzzy Match ({best_match['fuzzy_score']}%)"

    return None, "No Match"

def find_broker_in_master(extracted_broker_name, master_broker_list):
    if not extracted_broker_name or not master_broker_list:
        return "Unknown", 0
    best_match, score = process.extractOne(extracted_broker_name, master_broker_list)
    return (best_match, score) if score >= 85 else (extracted_broker_name, score)

# --- OUTLOOK & PARSING LOGIC ---
@st.cache_resource(ttl=3500)
def get_graph_api_token():
    app = msal.ConfidentialClientApplication(client_id=AZURE_CLIENT_ID, authority=AUTHORITY, client_credential=AZURE_CLIENT_SECRET)
    result = app.acquire_token_silent(scopes=SCOPE, account=None) or app.acquire_token_for_client(scopes=SCOPE)
    if "access_token" in result: return result['access_token']
    st.error("Failed to acquire access token."); st.json(result.get("error_description"))
    return None

def scan_outlook_emails(user_id, token, sender_domain=None):
    headers = {'Authorization': f'Bearer {token}'}
    query_filter = "isRead eq false"
    if sender_domain: query_filter += f" and contains(from/emailAddress/address, '{sender_domain}')"
    endpoint = f"https://graph.microsoft.com/v1.0/users/{user_id}/mailFolders/inbox/messages"
    params = {'$filter': query_filter, '$select': 'subject,body,from', '$top': '25'}
    try:
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()
        return response.json().get('value', [])
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching emails: {e}"); st.json(e.response.json())
        return None

# NEW/MODIFIED: This function cleans the HTML before Word conversion
def parse_and_clean_html_for_docx(html_string, temp_dir, msg_file_path=None):
    if not html_string:
        return ""
        
    soup = BeautifulSoup(html_string, 'html.parser')

    # --- 1. Remove known junk/boilerplate elements ---
    # Find and remove the "CAUTION: This email..." warning, which is often in a table or div
    caution_elements = soup.find_all(text=re.compile(r'CAUTION: This email has been sent from outside'))
    for element in caution_elements:
        # Find the parent table or div and remove it
        parent_container = element.find_parent('table') or element.find_parent('div')
        if parent_container:
            parent_container.decompose()

    # --- 2. Extract the main content section ---
    # Many Outlook emails wrap the main content in a div with this class
    main_content = soup.find('div', class_='WordSection1')
    if not main_content:
        main_content = soup.body if soup.body else soup
    if not main_content:
        return "" # Return empty if no content can be found

    # --- 3. Process all image tags within the main content ---
    for img_tag in main_content.find_all('img'):
        src = img_tag.get('src')
        if not src:
            img_tag.decompose() # Remove image tags with no source
            continue
        
        # --- Handle Base64 encoded images ---
        if src.startswith('data:image'):
            try:
                header, encoded = src.split(',', 1)
                img_type = header.split(';')[0].split('/')[1]
                img_data = base64.b64decode(encoded)
                img_filename = f"{uuid.uuid4()}.{img_type}"
                temp_img_path = os.path.join(temp_dir, img_filename)
                
                with open(temp_img_path, "wb") as f:
                    f.write(img_data)
                
                img_tag['src'] = temp_img_path
            except Exception as e:
                st.warning(f"Could not process a Base64 image: {e}")
                img_tag.decompose() # Remove broken tag

        # --- Handle CID embedded images (requires the .msg file) ---
        elif src.startswith('cid:') and msg_file_path:
            try:
                # Use a context manager to ensure the file is handled properly
                with Message(msg_file_path) as msg:
                    # Create a dictionary for quick lookups
                    cid_attachments = {att.cid: att for att in msg.attachments if getattr(att, 'cid', None)}
                    cid = src[4:]
                    
                    if cid in cid_attachments:
                        attachment = cid_attachments[cid]
                        # Use a safe filename
                        safe_img_filename = re.sub(r'[\\/*?:"<>|]', "", attachment.longFilename or f"{cid}.tmp")
                        temp_img_path = os.path.join(temp_dir, safe_img_filename)
                        
                        with open(temp_img_path, "wb") as f:
                            f.write(attachment.data)
                        img_tag['src'] = temp_img_path
                    else:
                        # If CID not found, remove the broken image link
                        img_tag.decompose()

            except Exception as e:
                st.warning(f"Could not process CID image '{src}': {e}")
                img_tag.decompose()

    # --- 4. Return the cleaned HTML as a string ---
    return str(main_content)


def extract_info_with_chatgpt(subject, body, master_brokers):
    broker_list_str = ", ".join(master_brokers)
    
    prompt = f"""You are an expert financial analyst. From the email below, extract key details for each financial report mentioned.

    **Instructions:**
    1.  **Company:** Extract the name of the company the financial report is about.
    2.  **Ticker:** Extract the stock Ticker if mentioned.
    3.  **BrokerName:** You MUST choose the most appropriate name from this list of known brokers: {broker_list_str}. If no suitable broker is mentioned or found, classify it as 'Unknown'.
    4.  **Category:** High-level classification like 'Equity Research'.
    5.  **ContentType:** Must be from this specific list: 'Earnings Commentary', 'Earnings Call Commentary', 'Market Update', 'Stock Initiation', 'Other'.
    6.  **FiscalYear:** The four-digit year the report is about (e.g., 2024, 2025). Look for terms like '4Q24', 'FY25', or '2025 Results'. If not explicitly stated, infer from the email's content. If it cannot be determined, return null.
    7.  **FiscalQuarter:** The quarter the report is about as a single number (1, 2, 3, or 4). Look for terms like 'Q3', '3Q', 'Third Quarter'. If not explicitly stated, infer from the content. If it cannot be determined, return null.

    **Email Details:**
    - EMAIL SUBJECT: {subject}
    - EMAIL BODY (first 8000 characters):
    ---
    {body[:8000]}
    ---
    
    Provide the output in a JSON object with a single key "reports", which is a list.
    Example: {{"reports": [{{"Country": "USA", "Sector": "Technology", "Company": "Example Corp", "Ticker": "EXMPL", "Category": "Equity Research", "ContentType": "Earnings Commentary", "BrokerName": "Global Brokerage", "FiscalYear": 2025, "FiscalQuarter": 3}}]}}"""
    try:
        response = client.chat.completions.create(model="gpt-4o", messages=[{"role": "system", "content": "You are a helpful assistant designed to output JSON."}, {"role": "user", "content": prompt}], response_format={"type": "json_object"})
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        st.error(f"Error with OpenAI API call: {e}"); return None

# --- CORE PROCESSING FUNCTION ---
def process_emails(email_source, source_type):
    master_companies_df = get_master_company_data()
    master_brokers = get_master_broker_names()

    if master_companies_df.empty:
        st.error(f"Master company database '{MASTER_DB_NAME}' is empty or not found.")
        return

    status_container = st.container() 
    progress_bar = st.progress(0, text="Initializing...")
    total_emails = len(email_source)
    
    for i, item in enumerate(email_source):
        subject, plain_body, raw_html_body = (None, None, None)
        blob_name = None

        try:
            if source_type == 'upload':
                file_bytes = item.getvalue()
                blob_name = f"emails/{uuid.uuid4()}-{item.name}"
                blob_client = blob_service_client.get_blob_client(container=AZURE_CONTAINER_NAME, blob=blob_name)
                blob_client.upload_blob(file_bytes)
                
                with tempfile.NamedTemporaryFile(delete=False, suffix=".msg") as tmp:
                    tmp.write(file_bytes)
                
                with Message(tmp.name) as msg:
                    subject = msg.subject
                    plain_body = msg.body
                    raw_html_body = msg.htmlBody or f"<pre>{plain_body}</pre>"

                os.unlink(tmp.name)
            
            elif source_type == 'outlook':
                subject = item.get('subject', 'No Subject')
                plain_body = item.get('body', {}).get('content', '')
                raw_html_body = plain_body
                
            if blob_name:
                status_container.info(f"📤 Saved original email to Azure with name: {blob_name}")

        except Exception as e:
            st.error(f"Failed during file handling or Azure upload: {e}")
            continue

        progress_bar.progress((i + 1) / total_emails, text=f"Processing: {subject}")
        if not (subject and plain_body and raw_html_body):
            status_container.warning(f"Skipping an email due to parsing error.")
            continue
        
        extracted = extract_info_with_chatgpt(subject, plain_body, master_brokers)
        if not (extracted and "reports" in extracted): continue

        for report in extracted["reports"]:
            report.setdefault('Company', 'N/A')
            report.setdefault('Ticker', None)
            report.setdefault('BrokerName', 'Unknown')
            report.setdefault('FiscalYear', None)
            report.setdefault('FiscalQuarter', None)
            report.setdefault('Category', 'N/A')
            report.setdefault('ContentType', 'Other')
            report.setdefault('Country', 'N/A')
            report.setdefault('Sector', 'N/A')
            
            report['EmailSubject'] = subject
            report['EmailContent'] = raw_html_body
            report['blob_name'] = blob_name

            company_to_find = report.get("Company", "N/A")
            matched_row, match_status = find_company_in_master(report, master_companies_df.copy())

            if matched_row is not None:
                report["Company"] = matched_row['short_name']
                report["Ticker"] = matched_row['ticker']
                report["Country"] = matched_row['country']
                report["Sector"] = matched_row['sector']
            else:
                status_container.warning(f"❌ Could not find a match for '{company_to_find}'")

            report["MatchStatus"] = match_status
            insert_into_db(report)

    progress_bar.progress(1.0, text="Processing complete!")
    st.success("✅ Processing complete! The database has been updated.")


# --- AZURE SAS URL HELPER ---
def generate_sas_url(container_name, blob_name):
    if not blob_name or pd.isna(blob_name):
        return None
    try:
        account_name = blob_service_client.account_name
        account_key = blob_service_client.credential.account_key
        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=container_name,
            blob_name=blob_name,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.utcnow() + timedelta(hours=1)
        )
        return f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}?{sas_token}"
    except Exception:
        return None

# --- MAIN UI ---
def main():
    st.markdown(f'<div class="header-container"><div class="app-title">Email Intelligence Extractor</div></div>', unsafe_allow_html=True)
    
    nav_tab1, nav_tab2, nav_tab3 = st.tabs([
        "📥 Scan & Process Emails", 
        "🔍 Query Database", 
        "📚 Manage Master Lists"
    ])

    with nav_tab1:
        st.header("Scan & Process Emails")
        scan_tab1, scan_tab2 = st.tabs(["Scan Outlook Inbox", "Upload .msg Files"])
        with scan_tab1:
            with st.container(border=True):
                st.subheader("Outlook Email Scan")
                target_email = st.text_input("Enter Mailbox Email Address:", placeholder="e.g., finance.reports@yourcompany.com", key="outlook_email_input")
                target_domain = st.text_input("Filter by Sender Domain (optional):", placeholder="e.g., jpmorgan.com", key="outlook_domain_input")
                if st.button("Scan for New Emails", type="primary", use_container_width=True, key="scan_outlook_button"):
                    if not target_email: st.warning("Please enter a mailbox email address.")
                    else:
                        with st.spinner("Authenticating and fetching emails..."):
                            token = get_graph_api_token()
                            if token:
                                emails = scan_outlook_emails(target_email, token, target_domain)
                                if emails: process_emails(emails, 'outlook')
                                elif emails is not None: st.success("✅ No new unread emails found.")
        with scan_tab2:
            with st.container(border=True):
                st.subheader("Upload .msg Files")
                uploaded_files = st.file_uploader("Select .msg files to process", type=["msg"], accept_multiple_files=True, key="msg_uploader")
                if st.button("Process Uploaded Emails", type="primary", use_container_width=True, key="process_upload_button"):
                    if uploaded_files: process_emails(uploaded_files, 'upload')
                    else: st.warning("Please upload at least one .msg file.")

    with nav_tab2:
        st.header("Query Extracted Data")

        try:
            all_data_df = query_db("SELECT * FROM email_data ORDER BY \"processedat\" DESC")
            all_data_df.columns = [x.lower() for x in all_data_df.columns]
            
            if 'processedat' in all_data_df.columns:
                all_data_df['processedat'] = pd.to_datetime(all_data_df['processedat'], errors='coerce').dt.tz_localize(None)

        except Exception as e:
            st.error(f"Could not connect to the database: {e}")
            all_data_df = pd.DataFrame()

        if all_data_df.empty:
            st.warning("The extracted data table is empty. Please process some emails first.")
        else:
            filtered_df = all_data_df.copy()

            # --- FILTER UI ---
            with st.container(border=True):
                st.subheader("📊 Filter Your Data")
                col1, col2, col3 = st.columns(3)
                with col1:
                    def get_options(column_name):
                        if column_name in all_data_df.columns:
                            return sorted([x for x in all_data_df[column_name].unique() if pd.notna(x)])
                        return []
                    selected_countries = st.multiselect("Country:", get_options('country'))
                    selected_brokers = st.multiselect("Broker Name:", get_options('brokername'))
                    selected_sectors = st.multiselect("Sector:", get_options('sector'))
                with col2:
                    selected_companies = st.multiselect("Company Name:", get_options('company'))
                    selected_content_types = st.multiselect("Email Content Type:", get_options('contenttype'))
                with col3:
                    if 'fiscalyear' in all_data_df.columns:
                        fiscal_years = sorted([int(x) for x in all_data_df['fiscalyear'].dropna().unique()], reverse=True)
                        selected_fiscal_years = st.multiselect("Fiscal Year:", options=fiscal_years)
                    if 'fiscalquarter' in all_data_df.columns:
                        fiscal_quarters = sorted([int(x) for x in all_data_df['fiscalquarter'].dropna().unique()])
                        selected_fiscal_quarters = st.multiselect("Fiscal Quarter:", options=fiscal_quarters)
                search_query = st.text_input("Open Search (Subject or Content):", placeholder="e.g., 'acquisition' or 'earnings call'")

            # --- APPLY FILTERS ---
            if selected_countries:
                filtered_df = filtered_df[filtered_df['country'].isin(selected_countries)]
            if selected_brokers:
                filtered_df = filtered_df[filtered_df['brokername'].isin(selected_brokers)]
            if selected_sectors:
                filtered_df = filtered_df[filtered_df['sector'].isin(selected_sectors)]
            if selected_companies:
                filtered_df = filtered_df[filtered_df['company'].isin(selected_companies)]
            if selected_content_types:
                filtered_df = filtered_df[filtered_df['contenttype'].isin(selected_content_types)]
            if selected_fiscal_years:
                filtered_df = filtered_df[filtered_df['fiscalyear'].isin(selected_fiscal_years)]
            if selected_fiscal_quarters:
                filtered_df = filtered_df[filtered_df['fiscalquarter'].isin(selected_fiscal_quarters)]
            if search_query:
                filtered_df = filtered_df[
                    filtered_df['emailsubject'].str.contains(search_query, case=False, na=False) |
                    filtered_df['emailcontent'].str.contains(search_query, case=False, na=False)
                ]
            
            st.info(f"Displaying **{len(filtered_df)}** of **{len(all_data_df)}** total entries.")

            if not filtered_df.empty:
                temp_db_name = "financial_emails_export_filtered.db"
                if os.path.exists(temp_db_name):
                    os.remove(temp_db_name)
                conn = sqlite3.connect(temp_db_name)
                df_to_export = filtered_df.copy()
                if 'processedat' in df_to_export.columns and pd.api.types.is_datetime64_any_dtype(df_to_export['processedat']):
                    if getattr(df_to_export['processedat'].dt, 'tz', None) is not None:
                         df_to_export['processedat'] = df_to_export['processedat'].dt.tz_localize(None)
                df_to_export.to_sql('email_data', conn, if_exists='fail', index=False)
                conn.close()
                with open(temp_db_name, "rb") as fp:
                    db_bytes = fp.read()
                os.remove(temp_db_name)

                st.download_button(
                    label="Download Filtered Results (SQLite DB)",
                    data=db_bytes,
                    file_name="financial_emails_filtered.db",
                    mime="application/octet-stream"
                )
            
            # --- MODIFIED: BULK EMAIL CONTENT DOWNLOAD (WORD DOC) ---
            if not filtered_df.empty:
                st.write("---") 
                st.subheader(f"Download Filtered Email Content ({len(filtered_df)} emails)")
                if st.button(f"Generate Word Document", key="generate_word_btn"):
                    
                    with tempfile.TemporaryDirectory() as temp_dir:
                        doc = docx.Document()
                        doc.add_heading('Filtered Email Intelligence Report', 0)
                        doc.add_paragraph(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                        
                        progress_text = "Generating Word document... Please wait."
                        word_progress = st.progress(0, text=progress_text)
                        
                        df_for_export = filtered_df.copy()
                        if 'processedat' in df_for_export.columns:
                            df_for_export.sort_values(by='processedat', ascending=False, inplace=True)
                        
                        # The parser is now instantiated INSIDE the loop
                        for i, (index, row) in enumerate(df_for_export.iterrows()):
                            parser = HtmlToDocx() # <<< FIX: Create a new parser for each email

                            doc.add_page_break()
                            doc.add_heading(f"Company: {row.get('company', 'N/A')} ({row.get('ticker', 'N/A')})", level=1)
                            doc.add_heading(f"Subject: {row.get('emailsubject', 'No Subject')}", level=2)
                            
                            p = doc.add_paragraph()
                            p.add_run('Date: ').bold = True
                            p.add_run(f"{row.get('processedat').strftime('%Y-%m-%d %H:%M') if pd.notna(row.get('processedat')) else 'N/A'} | ")
                            p.add_run('Broker: ').bold = True
                            p.add_run(f"{row.get('brokername', 'N/A')}")
                            
                            original_html = row.get('emailcontent', '<p>No content available.</p>')
                            blob_name = row.get('blob_name')
                            tmp_msg_path = None

                            # Re-process from .msg file if available to get images
                            if blob_name and pd.notna(blob_name):
                                try:
                                    blob_client = blob_service_client.get_blob_client(AZURE_CONTAINER_NAME, blob_name)
                                    msg_bytes = blob_client.download_blob().readall()
                                    
                                    fd, tmp_msg_path = tempfile.mkstemp(suffix=".msg", dir=temp_dir)
                                    with os.fdopen(fd, 'wb') as tmp_file:
                                        tmp_file.write(msg_bytes)
                                    
                                    # Use the original HTML from the database row
                                    cleaned_html = parse_and_clean_html_for_docx(original_html, temp_dir, tmp_msg_path)
                                
                                except Exception as e:
                                    st.warning(f"Could not re-process email to extract images: {blob_name}. Error: {e}")
                                    # Fallback to cleaning the stored HTML without the .msg context
                                    cleaned_html = parse_and_clean_html_for_docx(original_html, temp_dir)

                            else:
                                # Process HTML for emails that weren't uploaded as .msg
                                cleaned_html = parse_and_clean_html_for_docx(original_html, temp_dir)

                            try:
                                parser.add_html_to_document(cleaned_html, doc)
                            except (IndexError, KeyError) as e:
                                doc.add_paragraph(
                                    f"[Content could not be rendered due to a complex or malformed table in the original email. Error: {e}]"
                                )
                            except Exception as e:
                                doc.add_paragraph(
                                    f"[An unexpected error occurred while rendering email content: {e}]"
                                )

                            word_progress.progress((i + 1) / len(df_for_export), text=f"Processing {i+1}/{len(df_for_export)}: {row.get('company', 'N/A')}")
                        
                        word_progress.empty()
                        doc_buffer = io.BytesIO()
                        doc.save(doc_buffer)
                        st.session_state['word_data'] = doc_buffer.getvalue()
                        st.session_state['word_filename'] = f"email_content_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.docx"
                        st.rerun()

                if st.session_state.get('word_data'):
                    st.download_button(
                        label=f"✅ Click to Download: {st.session_state['word_filename']}",
                        data=st.session_state['word_data'],
                        file_name=st.session_state['word_filename'],
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        type="primary",
                        use_container_width=True,
                        on_click=lambda: st.session_state.pop('word_data', None)
                    )

            # --- DISPLAY DATAFRAME with Download Link ---
            st.write("---")
            if 'blob_name' in filtered_df.columns:
                display_df = filtered_df.copy()
                display_df['download_link'] = display_df['blob_name'].apply(
                    lambda name: generate_sas_url(AZURE_CONTAINER_NAME, name)
                )
                st.dataframe(
                    display_df,
                    column_config={
                        "download_link": st.column_config.LinkColumn(
                            "Original Email",
                            help="Click to download the original .msg file (link expires in 1 hour)",
                            display_text="⬇️ Download"
                        ),
                        "emailcontent": None,
                    },
                    use_container_width=True
                )
            else:
                st.dataframe(filtered_df, use_container_width=True)

    with nav_tab3:
        st.header("Manage Master Lists")
        st.info(f"This data is read from '{MASTER_DB_NAME}'. To update it, please use your external import script.")
        # Displaying master company data
        st.subheader("Master Company List")
        st.dataframe(get_master_company_data(), use_container_width=True)
        # Displaying master broker data
        st.subheader("Master Broker List")
        broker_names = get_master_broker_names()
        if broker_names:
            st.dataframe(pd.DataFrame(broker_names, columns=["Broker Name"]), use_container_width=True)
        else:
            st.warning(f"The 'master_brokers' table was not found in '{MASTER_DB_NAME}'.")

if __name__ == "__main__":
    main()