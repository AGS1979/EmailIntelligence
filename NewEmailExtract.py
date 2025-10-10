import os
import sqlite3
import json
import tempfile
import re
from datetime import datetime

import streamlit as st
import pandas as pd
from extract_msg import Message
from openai import OpenAI
import requests
import msal
from thefuzz import fuzz, process
from sqlalchemy import create_engine, text

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
except KeyError as e:
    st.error(f"Configuration key not found in Streamlit Secrets: {e}")
    st.stop()

# --- CONSTANTS ---
MASTER_DB_NAME = "Master_Company_List.db"
AUTHORITY = f"https://login.microsoftonline.com/{AZURE_TENANT_ID}"
SCOPE = ["https://graph.microsoft.com/.default"]

# --- NEW DATABASE SETUP ---
# Create a reusable database engine from the connection string in Streamlit Secrets
engine = create_engine(DB_CONNECTION_STRING)

# --- DATABASE FUNCTIONS ---
def setup_output_database():
    # This function is no longer needed.
    # The table should be created one time in the Supabase SQL Editor using the following command:
    # CREATE TABLE IF NOT EXISTS email_data (
    #     id SERIAL PRIMARY KEY, Country TEXT, Sector TEXT,
    #     Company TEXT, Ticker TEXT, Category TEXT, ContentType TEXT, BrokerName TEXT,
    #     EmailSubject TEXT, EmailContent TEXT, MatchStatus TEXT,
    #     ProcessedAt TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    # );
    pass

def query_db(query, params=None):
    # This function now queries the permanent cloud database
    with engine.connect() as conn:
        return pd.read_sql_query(sql=text(query), con=conn, params=params)

# This function reads the local master DB file from the repository
def query_local_db(db_name, query, params=None):
    with sqlite3.connect(db_name) as conn:
        return pd.read_sql_query(query, conn, params=params if params else ())

@st.cache_data(ttl=3600)
def get_master_company_data():
    if not os.path.exists(MASTER_DB_NAME):
        return pd.DataFrame()
    # Note: Using query_local_db to read from the SQLite file in the repo
    return query_local_db(MASTER_DB_NAME, "SELECT * FROM master_companies")

@st.cache_data(ttl=3600)
def get_master_broker_names():
    if not os.path.exists(MASTER_DB_NAME): 
        return []
    # Note: Using query_local_db to read from the SQLite file in the repo
    df = query_local_db(MASTER_DB_NAME, "SELECT Name FROM master_brokers")
    return df['Name'].tolist() if not df.empty else []

def insert_into_db(data):
    # This function now inserts data into the permanent cloud database
    df = pd.DataFrame([data])
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
    
    if 'normalized_name' not in master_df.columns:
        master_df['normalized_name'] = master_df['short_name'].apply(normalize_company_name)

    if ticker:
        match = master_df[master_df['ticker'].str.lower() == ticker.lower()]
        if not match.empty:
            return match.iloc[0], "Ticker Match"

    if company_name:
        match = master_df[master_df['short_name'].str.lower() == company_name.lower()]
        if not match.empty:
            return match.iloc[0], "Exact Name Match"

        normalized_input = normalize_company_name(company_name)
        if normalized_input:
            exact_normalized_match = master_df[master_df['normalized_name'] == normalized_input]
            if not exact_normalized_match.empty:
                return exact_normalized_match.iloc[0], "Normalized Match"
            
            substring_matches = master_df[master_df['normalized_name'].str.contains(normalized_input, na=False)]
            if len(substring_matches) == 1:
                return substring_matches.iloc[0], "Substring Match"

        master_df['fuzzy_score'] = master_df['short_name'].apply(
            lambda x: fuzz.token_set_ratio(company_name, x)
        )
        best_match = master_df.loc[master_df['fuzzy_score'].idxmax()]
        
        if best_match['fuzzy_score'] >= 95:
            return best_match, f"Fuzzy Match ({best_match['fuzzy_score']}%)"

    return None, "No Match"

def find_broker_in_master(extracted_broker_name, master_broker_list):
    if not extracted_broker_name or not master_broker_list:
        return "Unknown", 0
    best_match, score = process.extractOne(extracted_broker_name, master_broker_list)
    if score >= 85:
        return best_match, score
    else:
        return extracted_broker_name, score

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

def parse_email(file_path):
    try:
        with Message(file_path) as msg: return msg.subject, msg.body
    except Exception as e:
        st.error(f"Error parsing file: {e}"); return None, None

def extract_info_with_chatgpt(subject, body, master_brokers):
    broker_list_str = ", ".join(master_brokers)
    
    prompt = f"""You are an expert financial analyst. From the email below, extract key details for each financial report mentioned.

    **Instructions:**
    1.  **Company:** Extract the name of the company the financial report is about.
    2.  **Ticker:** Extract the stock Ticker if mentioned.
    3.  **BrokerName:** You MUST choose the most appropriate name from this list of known brokers: {broker_list_str}. If no suitable broker is mentioned or found, classify it as 'Unknown'.
    4.  **Category:** High-level classification like 'Equity Research'.
    5.  **ContentType:** Must be from this specific list: 'Earnings Commentary', 'Earnings Call Commentary', 'Market Update', 'Stock Initiation', 'Other'.

    **Email Details:**
    - EMAIL SUBJECT: {subject}
    - EMAIL BODY (first 8000 characters):
    ---
    {body[:8000]}
    ---
    
    Provide the output in a JSON object with a single key "reports", which is a list.
    Example: {{"reports": [{{"Country": "USA", "Sector": "Technology", "Company": "Example Corp", "Ticker": "EXMPL", "Category": "Equity Research", "ContentType": "Earnings Commentary", "BrokerName": "Global Brokerage"}}]}}"""
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
        if not os.path.exists(MASTER_DB_NAME):
            st.error(f"Database file '{MASTER_DB_NAME}' not found. Please ensure it's in the correct directory.")
        else:
            st.error(f"Database '{MASTER_DB_NAME}' found, but the 'master_companies' table is empty or missing. Please run the import script.")
        return

    if not master_brokers:
        st.warning(f"Warning: The 'master_brokers' table in '{MASTER_DB_NAME}' is empty. Broker name extraction may be less accurate.")

    status_container = st.container() 
    progress_bar = st.progress(0, text="Initializing...")
    total_emails = len(email_source)
    
    for i, item in enumerate(email_source):
        subject, body = (None, None)
        if source_type == 'outlook':
            subject, body = item.get('subject', 'No Subject'), item.get('body', {}).get('content', '')
        elif source_type == 'upload':
            with tempfile.NamedTemporaryFile(delete=False, suffix=".msg") as tmp: tmp.write(item.getvalue())
            subject, body = parse_email(tmp.name); os.unlink(tmp.name)
        
        progress_bar.progress((i + 1) / total_emails, text=f"Processing: {subject}")
        if not (subject and body): continue
        
        extracted = extract_info_with_chatgpt(subject, body, master_brokers)
        if not (extracted and "reports" in extracted): continue

        for report in extracted["reports"]:
            report.setdefault('Country', 'N/A')
            report.setdefault('Sector', 'N/A')
            report.setdefault('Company', 'N/A')
            report.setdefault('Ticker', None)
            report.setdefault('Category', 'N/A')
            report.setdefault('ContentType', 'Other')
            report.setdefault('BrokerName', 'Unknown')
            report['EmailSubject'] = subject
            report['EmailContent'] = body

            company_to_find = report.get("Company", "N/A")
            matched_row, match_status = find_company_in_master(report, master_companies_df)

            if matched_row is not None:
                report["Company"] = matched_row['short_name']
                report["Ticker"] = matched_row['ticker']
                report["Country"] = matched_row['country']
                report["Sector"] = matched_row['sector']
                status_container.info(f"🔎 Found '{company_to_find}' -> Matched to '{report['Company']}' via '{match_status}'")
            else:
                status_container.warning(f"❌ Could not find a match for '{company_to_find}'")

            report["MatchStatus"] = match_status

            extracted_broker = report.get("BrokerName", "N/A")
            if master_brokers:
                matched_broker, score = find_broker_in_master(extracted_broker, master_brokers)
                report["BrokerName"] = matched_broker
                status_container.info(f" broker '{extracted_broker}' -> Matched to '{matched_broker}' (Score: {score}%)")

            insert_into_db(report)

    progress_bar.progress(1.0, text="Processing complete!")
    st.success("✅ Processing complete! The database has been updated.")


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
            # Query ALL data from your permanent cloud database
            all_data_df = query_db("SELECT * FROM email_data ORDER BY ProcessedAt DESC")
        except Exception as e:
            st.error(f"Could not connect to the database: {e}")
            all_data_df = pd.DataFrame() # Create an empty df on error

        if all_data_df.empty:
            st.warning("The extracted data table is empty. Please process some emails first.")
        else:
            # --- START: PREPARE AND SHOW DOWNLOAD BUTTON ---
            # 1. Create a temporary SQLite file on the server to hold the data
            temp_db_name = "financial_emails_export.db"
            conn = sqlite3.connect(temp_db_name)
            # 2. Write the DataFrame from the cloud into this temporary file
            all_data_df.to_sql('email_data', conn, if_exists='replace', index=False)
            conn.close()

            # 3. Read the bytes from the newly created file
            with open(temp_db_name, "rb") as fp:
                db_bytes = fp.read()
            
            # 4. Clean up by deleting the temporary file from the server
            os.remove(temp_db_name)

            # 5. Display the download button with the prepared data
            st.download_button(
                label="Download Full Database (SQLite DB)",
                data=db_bytes,
                file_name="financial_emails.db", # The name the user will see
                mime="application/octet-stream"
            )
            # --- END: DOWNLOAD BUTTON LOGIC ---

            # Display the full dataframe from the cloud database
            st.dataframe(all_data_df, use_container_width=True)

    with nav_tab3:
        st.header("Manage Master Lists")
        st.info(f"This data is read from '{MASTER_DB_NAME}'. To update it, please use your external import script.")
        master_tab1, master_tab2 = st.tabs(["Master Companies", "Master Brokers"])
        with master_tab1:
            st.subheader("Master Company List")
            st.dataframe(get_master_company_data(), use_container_width=True)
        with master_tab2:
            st.subheader("Master Broker List")
            broker_names = get_master_broker_names()
            if broker_names:
                st.dataframe(pd.DataFrame(broker_names, columns=["Broker Name"]), use_container_width=True)
            else:
                st.warning(f"The 'master_brokers' table was not found in '{MASTER_DB_NAME}'. Please ensure your import script has run correctly.")

if __name__ == "__main__":
    main()