# email_ui.py

import os
import sqlite3
import json
import tempfile
import base64
import streamlit as st
import pandas as pd
from extract_msg import Message
from openai import OpenAI

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Email Intelligence Extractor",
    page_icon="📧",
    layout="wide"
)

# --- STYLING ---
st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700&display=swap');
/* ... (other styles remain the same) ... */

div[data-baseweb="radio"] > div:not([data-checked="true"]) > label:hover {
    background-color: #f0f2f6;
}

/* --- MODIFIED/NEW CSS RULES START HERE --- */

/* New container to hold both title and logo */
.header-container {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding-bottom: 1rem;
    border-bottom: 2px solid #f0f2f6;
    margin-bottom: 2rem;
}

/* Updated title style (no border needed anymore) */
.aranca-title {
    font-size: 2.5rem !important;
    font-weight: 700;
    color: #1e1e1e;
}

/* Updated logo style (no longer needs absolute positioning) */
.aranca-logo img {
    height: 38px;
    object-fit: contain;
}
/* --- MODIFIED/NEW CSS RULES END HERE --- */

[data-testid="stSidebarCollapseButton"] {
    display: none;
}
</style>
""", unsafe_allow_html=True)


# --- CONFIGURATION & CLIENTS ---
try:
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
except KeyError:
    st.error("OpenAI API key not found. Please add it to your secrets.toml file.")
    st.stop()

DATABASE_NAME = "financial_emails.db"
MAPPING_FILE = "company_mapping.json"

# --- HELPER FUNCTIONS ---
def get_base_64_logo_image(path="logo.png"):
    if os.path.exists(path):
        with open(path, "rb") as f:
            return base64.b64encode(f.read()).decode()
    return ""

# --- NEW: Functions for managing company name mappings ---
def load_mappings():
    if os.path.exists(MAPPING_FILE):
        with open(MAPPING_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_mappings(mappings):
    with open(MAPPING_FILE, 'w') as f:
        json.dump(mappings, f, indent=4)

def get_canonical_name(name, mappings):
    return mappings.get(name, name)

# --- BACKEND PROCESSING LOGIC ---
def parse_email(file_path):
    try:
        with Message(file_path) as msg:
            return msg.subject, msg.body
    except Exception as e:
        st.error(f"Error parsing file: {e}")
        return None, None

def extract_info_with_chatgpt(subject, body):
    prompt = f"""
    You are an expert financial analyst assistant. Your primary task is to analyze the following email and extract its contents into a structured JSON format.

    CRITICAL INSTRUCTIONS:
    1.  First, determine the email's structure. Is it a "digest" covering multiple distinct companies/topics, or is it a focused report on a single company/topic?
    2.  If the email is a focused report on ONE company or ONE macro topic, you MUST generate only ONE JSON object for it.
    3.  If the email is a "digest" covering MULTIPLE distinct companies or topics, then generate a separate JSON object for each one.
    4.  **Category Standardization**: For the "Category" field, you must use standardized terms. If the report is about company results (e.g., 'Earnings Call Summary', 'Earnings Preview', 'First Take on Results', 'Quarterly Results'), classify it simply as 'Earnings'. Other common categories are 'Weekly Digest', 'Initiation of Coverage', 'Rating Update', or 'Macro Outlook'.
    5.  **Strict Company Name Standardization**: You MUST return the most complete and official legal name for the company. For example, variations like 'Salik Co.', 'Salik', or 'Salik Dubai' must all be standardized to 'Salik Company PJSC'. Similarly, 'eXtra' should become 'United Electronics Company'.
    6.  **Content Extraction**: For the "EmailContent" field, extract the specific and relevant paragraph(s) for that report.
    7.  For general/macro reports, set "Company" to "N/A".

    FINAL JSON STRUCTURE:
    The final output must be a single JSON object with one key, "reports", which contains a list of the generated JSON objects.

    REQUIRED FIELDS FOR EACH OBJECT:
    - "Country": The company's primary country or the report's region.
    - "Sector": The industry sector (e.g., "Retail", "Banking", "Macro").
    - "Company": The standardized, official, and complete legal name of the company. Set to "N/A" for non-company-specific reports.
    - "Category": The standardized type of report. **Crucially, if it is earnings-related, this must be 'Earnings'.**
    - "BrokerName": The name of the investment bank that authored the report.
    - "EmailSubject": The complete, original subject of the email.
    - "EmailContent": The specific text snippet from the email body relevant ONLY to this report.

    EMAIL SUBJECT: {subject}
    EMAIL BODY:
    ---
    {body}
    ---

    Provide the output in the specified JSON format only.
    """
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a helpful assistant designed to output JSON."},
                {"role": "user", "content": prompt}
            ],
            response_format={"type": "json_object"}
        )
        extracted_json = response.choices[0].message.content
        return json.loads(extracted_json) if extracted_json else None
    except Exception as e:
        st.error(f"Error with OpenAI API call: {e}")
        return None

def setup_database():
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS email_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            Country TEXT,
            Sector TEXT,
            Company TEXT,
            Category TEXT,
            BrokerName TEXT,
            EmailSubject TEXT,
            EmailContent TEXT,
            ProcessedAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        conn.commit()

def insert_into_db(data):
    with sqlite3.connect(DATABASE_NAME) as conn:
        cursor = conn.cursor()
        cursor.execute("""
        INSERT INTO email_data (Country, Sector, Company, Category, BrokerName, EmailSubject, EmailContent)
        VALUES (:Country, :Sector, :Company, :Category, :BrokerName, :EmailSubject, :EmailContent)
        """, data)
        conn.commit()

@st.cache_data(ttl=60)
def query_db(query, params=None):
    with sqlite3.connect(DATABASE_NAME) as conn:
        return pd.read_sql_query(query, conn, params=params)

def get_unique_values(column):
    df = query_db(f"SELECT DISTINCT {column} FROM email_data WHERE {column} IS NOT NULL AND {column} != 'N/A' ORDER BY {column}")
    return df[column].tolist()

# --- UI RENDERING ---
def main():
    logo_base_64 = get_base_64_logo_image()
    
    # --- MODIFIED HTML STRUCTURE ---
    st.markdown(
        f"""
        <div class="header-container">
            <div class="aranca-title">Email Intelligence Extractor</div>
            <div class="aranca-logo">
                <img src="data:image/png;base64,{logo_base_64}" alt="Aranca Logo">
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

    with st.sidebar:
        st.title("Navigation")
        app_mode = st.radio(
            "Choose a section:",
            ["Upload & Process Emails", "Query Database", "Manage Company Names"], # --- NEW: Added new page
            key="nav_radio"
        )
        
        st.divider()
        if st.button("⚠️ Reset Database"):
            if os.path.exists(DATABASE_NAME):
                os.remove(DATABASE_NAME)
            if os.path.exists(MAPPING_FILE): # Also remove mapping file
                os.remove(MAPPING_FILE)
            st.cache_data.clear()
            st.success("Database and mappings have been reset.")
            st.rerun()
    
    setup_database()

    if app_mode == "Upload & Process Emails":
        st.subheader("📤 Upload & Process Emails")
        st.info("Do not click links or open attachments unless you recognize the sender and know the content is safe.")
        
        uploaded_files = st.file_uploader(
            "Drag and drop your .msg files here",
            type=["msg"],
            accept_multiple_files=True
        )
        if st.button("Process Uploaded Emails", type="primary", use_container_width=True):
            if uploaded_files:
                # --- NEW: Load mappings before processing ---
                mappings = load_mappings()
                progress_bar = st.progress(0, text="Initializing...")
                status_text = st.empty()

                for i, uploaded_file in enumerate(uploaded_files):
                    status_text.text(f"Processing file {i+1}/{len(uploaded_files)}: {uploaded_file.name}")
                    progress_bar.progress((i + 1) / len(uploaded_files))

                    with tempfile.NamedTemporaryFile(delete=False, suffix=".msg") as tmp:
                        tmp.write(uploaded_file.getvalue())
                        tmp_path = tmp.name

                    subject, body = parse_email(tmp_path)
                    os.remove(tmp_path)

                    if subject and body:
                        extracted_data_wrapper = extract_info_with_chatgpt(subject, body)
                        if extracted_data_wrapper and "reports" in extracted_data_wrapper:
                            reports = extracted_data_wrapper["reports"]
                            st.info(f"🔎 Found {len(reports)} report(s) in `{uploaded_file.name}`.")
                            for report_data in reports:
                                # --- NEW: Apply canonical name mapping ---
                                original_name = report_data.get("Company", "N/A")
                                if original_name != "N/A":
                                    canonical_name = get_canonical_name(original_name, mappings)
                                    report_data["Company"] = canonical_name
                                
                                insert_into_db(report_data)
                                st.success(f"✅ Stored report for: **{report_data['Company']}**")
                        else:
                            st.warning(f"⚠️ Could not extract structured data from: {uploaded_file.name}")
                status_text.text("Processing complete!")
            else:
                st.warning("Please upload at least one file.")

    elif app_mode == "Query Database":
        st.subheader("🔍 Query Database")
        st.info("Filter and search the extracted email data by company, broker, or report category.")

        companies = get_unique_values("Company")
        brokers = get_unique_values("BrokerName")
        categories = get_unique_values("Category")

        col1, col2, col3 = st.columns(3)
        with col1:
            selected_companies = st.multiselect("Filter by Company", options=companies)
        with col2:
            selected_brokers = st.multiselect("Filter by Broker", options=brokers)
        with col3:
            selected_categories = st.multiselect("Filter by Category", options=categories, help="Filter by the type of report, e.g., Earnings.")

        search_term = st.text_input("Search in Email Content", help="Enter a keyword to search for within the email body.")

        query = "SELECT id, Company, Category, BrokerName, Country, Sector, EmailSubject, ProcessedAt FROM email_data"
        conditions = []
        params = {}

        if selected_companies:
            conditions.append(f"Company IN ({','.join('?' for _ in selected_companies)})")
            params['companies'] = selected_companies
        if selected_brokers:
            conditions.append(f"BrokerName IN ({','.join('?' for _ in selected_brokers)})")
            params['brokers'] = selected_brokers
        if selected_categories:
            conditions.append(f"Category IN ({','.join('?' for _ in selected_categories)})")
            params['categories'] = selected_categories
        if search_term:
            conditions.append("EmailContent LIKE ?")
            params['search'] = f"%{search_term}%"

        if conditions:
            query += " WHERE " + " AND ".join(conditions)

        flat_params = []
        if 'companies' in params: flat_params.extend(params['companies'])
        if 'brokers' in params: flat_params.extend(params['brokers'])
        if 'categories' in params: flat_params.extend(params['categories'])
        if 'search' in params: flat_params.append(params['search'])

        st.markdown("---")
        df = query_db(query, flat_params)
        st.dataframe(df, use_container_width=True)
        st.caption(f"Displaying {len(df)} rows.")

    # --- NEW: UI Page for Managing Company Name Mappings ---
    elif app_mode == "Manage Company Names":
        st.subheader("⚙️ Manage Company Name Mappings")
        st.info("Use this page to map variations of company names (e.g., 'Salik Co.') to a single, official name (e.g., 'Salik Company PJSC'). This will clean up the 'Filter by Company' dropdown.")

        mappings = load_mappings()
        all_db_names = get_unique_values("Company")
        
        # Identify names that are already canonical targets
        canonical_names = sorted(list(set(mappings.values())))
        # Identify names that are variations (keys in the map)
        mapped_variations = list(mappings.keys())
        
        # Names that need mapping are those not yet a target or a mapped variation
        names_to_map = sorted([name for name in all_db_names if name not in canonical_names and name not in mapped_variations])

        st.markdown("---")
        st.header("Create a New Mapping")

        col1, col2 = st.columns(2)
        with col1:
            selected_variation = st.selectbox("Select a company name variation to map:", options=names_to_map, index=None, placeholder="Choose a name...")
        
        with col2:
            # Options for canonical name include existing ones plus an option for a new one
            all_canonical_options = ["<Type a new canonical name>"] + canonical_names
            selected_canonical = st.selectbox("Map it to this official (canonical) name:", options=all_canonical_options, index=None, placeholder="Choose or type a new name...")

        new_canonical_name = ""
        if selected_canonical == "<Type a new canonical name>":
            new_canonical_name = st.text_input("Enter the new official company name:")

        if st.button("Save New Mapping", type="primary"):
            target_canonical = new_canonical_name if new_canonical_name else selected_canonical
            if selected_variation and target_canonical:
                mappings[selected_variation] = target_canonical
                save_mappings(mappings)
                st.success(f"Mapped '{selected_variation}' -> '{target_canonical}'. The change will apply to new emails.")
                st.rerun()
            else:
                st.warning("Please select both a variation and a canonical name.")

        st.markdown("---")
        st.header("Existing Mappings")
        if mappings:
            st.json(mappings, expanded=True)
        else:
            st.write("No mappings created yet.")


if __name__ == "__main__":
    main()