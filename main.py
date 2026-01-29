import os
import re
import json
import pdfplumber
from flask import Flask, request, jsonify
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.discovery import build

app = Flask(__name__)

# --- CONFIGURATION ---
# Ensure these match your actual IDs and environment variables
MASTER_SHEET_ID = "1cY6O1tDlkCRaTE9eYlqZ-culLKR2Y6Q2uHzmcuJ5Gg4"
# Assuming 'INPUT' tab has a specific ID or name you want to target.
# If copying the whole file, we just need to know which tab to write to.
TARGET_TAB_NAME = "INPUT" 

# --- AUTHENTICATION ---
# We use the environment variable GOOGLE_CREDENTIALS for the service account json
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

def get_creds():
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not set")
    creds_dict = json.loads(creds_json)
    return ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)

# --- MAPPING LOGIC ---
def get_product_code(text):
    t = text.lower()
    if "card" in t: return 8
    if "auto" in t or "car" in t or "lease" in t or "ijara" in t: return 2
    if "personal" in t or "finance" in t or "micro" in t: return 6
    if "running" in t or "od" in t or "cash line" in t: return 34
    return text

def get_term_code(text):
    t = text.lower()
    if "card" in t or "running" in t or "cash line" in t: return "E"
    return "T"

# --- PDF PARSER ---
def parse_tasdeeq_pdf(file_stream):
    data = {
        "Name": "[[ NOT PROVIDED ]]",
        "CNIC": "[[ NOT PROVIDED ]]",
        "DOB": "[[ NOT PROVIDED ]]",
        "Loans": []
    }

    try:
        with pdfplumber.open(file_stream) as pdf:
            full_text = ""
            for page in pdf.pages:
                full_text += page.extract_text() + "\n"

            # --- PERSONAL INFO ---
            name_match = re.search(r"Name:\s*(.*?)(?=\s+(Father|Gender|CNIC)|$)", full_text, re.IGNORECASE)
            if name_match:
                clean = name_match.group(1).strip()
                if clean: data["Name"] = clean

            cnic_match = re.search(r"\d{5}-\d{7}-\d", full_text)
            if cnic_match: data["CNIC"] = cnic_match.group(0)

            dob_match = re.search(r"Date of Birth:\s*(\d{2}/\d{2}/\d{4})", full_text)
            if dob_match: data["DOB"] = dob_match.group(1)

            # --- FULL LOAN HISTORY ---
            lines = full_text.split('\n')
            current_loan = {}
            garbage_pattern = re.compile(r"^\d+\s?-\s?\d+$")
            capture_overdues = False

            for line in lines:
                line = line.strip()

                # 1. Detect New Loan ("1- BANK NAME")
                if re.match(r"^\d+\s?-\s?[A-Za-z]", line) and not garbage_pattern.match(line):
                    if current_loan: data["Loans"].append(current_loan)

                    bank_part = line.split("-", 1)[1].strip() if "-" in line else line
                    current_loan = {
                        "Bank": bank_part, "Limit": 0, "Outstanding": 0, "MinDue": 0,
                        "30": 0, "60": 0, "90": 0,
                        "Start": "", "End": ""
                    }
                    capture_overdues = False 

                if current_loan:
                    # Amounts
                    if "Loan Limit:" in line:
                        val = re.search(r"[\d,]+", line.split("Limit:")[-1])
                        if val: current_loan["Limit"] = int(val.group(0).replace(",", ""))
                    if "Outstanding Balance:" in line:
                        val = re.search(r"[\d,]+", line.split("Balance:")[-1])
                        if val: current_loan["Outstanding"] = int(val.group(0).replace(",", ""))
                    if "Min Amount Due:" in line:
                        val = re.search(r"[\d,]+", line.split("Due:")[-1])
                        if val: current_loan["MinDue"] = int(val.group(0).replace(",", ""))

                    # Dates
                    if "Facility Date:" in line:
                        date_match = re.search(r"(\d{2}/\d{2}/\d{4})", line.split("Facility Date:")[-1])
                        if date_match: current_loan["Start"] = date_match.group(1)
                    if "Maturity Date:" in line:
                        date_match = re.search(r"(\d{2}/\d{2}/\d{4})", line.split("Maturity Date:")[-1])
                        if date_match: current_loan["End"] = date_match.group(1)

                    # Overdues Logic
                    if "SUMMARY OF OVERDUES" in line:
                        capture_overdues = True

                    if capture_overdues and line.startswith("Times"):
                        nums = re.findall(r"\d+", line)
                        if len(nums) >= 3:
                            current_loan["30"] = int(nums[0])
                            current_loan["60"] = int(nums[1])
                            current_loan["90"] = int(nums[2])
                        capture_overdues = False 

            if current_loan: data["Loans"].append(current_loan)

    except Exception as e:
        print(f"   ⚠️ Parsing Issue: {e}")
        # We return partial data if something failed, or empty structure
        
    return data

@app.route('/', methods=['GET'])
def index():
    return "DBR Processor is running."

@app.route('/process', methods=['POST'])
def process_pdf():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    try:
        # 1. Parse PDF
        customer_data = parse_tasdeeq_pdf(file)
        
        # 2. Setup Google Drive/Sheets connection
        creds = get_creds()
        gc = gspread.authorize(creds)
        drive_service = build('drive', 'v3', credentials=creds)

        # 3. Create New Spreadsheet
        # Sanitize filename
        safe_name = customer_data['Name'].replace("/", "_")
        if "NOT PROVIDED" in safe_name: safe_name = "Unknown_Customer"
        new_file_name = f"DBR - {safe_name}"

        # Copy the Master Template
        copied_file = drive_service.files().copy(
            fileId=MASTER_SHEET_ID,
            body={"name": new_file_name}
        ).execute()
        new_sheet_id = copied_file.get('id')

        # 4. Write Data to the New Sheet
        sh = gc.open_by_key(new_sheet_id)
        ws = sh.worksheet(TARGET_TAB_NAME)

        updates = []

        # Identity & Defaults
        updates.append({"range": "C6", "values": [[customer_data["Name"]]]})
        updates.append({"range": "C7", "values": [[customer_data["CNIC"]]]})
        updates.append({"range": "C8", "values": [[customer_data["DOB"]]]})
        updates.append({"range": "C9", "values": [['=IF(C8<>"",DATEDIF(C8,TODAY(),"Y"),"")']]}) # Age

        updates.append({"range": "H7", "values": [[0]]})
        updates.append({"range": "H8", "values": [[0]]})
        updates.append({"range": "C12", "values": [["[SELECT]"]]})
        updates.append({"range": "C13", "values": [["Salaried"]]})

        # Loan Grid
        start_row = 19
        loan_rows = []

        for loan in customer_data["Loans"]:
            p_code = get_product_code(loan["Bank"])
            term_code = get_term_code(loan["Bank"])

            # Map to Sheet Columns (B through M)
            row_data = [
                "N", p_code, term_code, loan["Limit"], "", loan["Outstanding"],
                loan["MinDue"], loan["30"], loan["60"], loan["90"],
                loan["Start"], loan["End"]
            ]
            loan_rows.append(row_data)

        if loan_rows:
            end_row = start_row + len(loan_rows) - 1
            updates.append({"range": f"B{start_row}:M{end_row}", "values": loan_rows})

        ws.batch_update(updates, value_input_option="USER_ENTERED")

        # Return the link
        sheet_url = f"https://docs.google.com/spreadsheets/d/{new_sheet_id}/edit"
        return jsonify({'status': 'success', 'sheet_url': sheet_url})

    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 8080))
    app.run(host='0.0.0.0', port=port)
