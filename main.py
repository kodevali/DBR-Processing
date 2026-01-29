import os
import time
import threading
import json
import io
import re
import gspread
import pdfplumber
from flask import Flask
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials

# ==========================================
# 1. CONFIGURATION (YOU MUST FILL THESE)
# ==========================================
# Open your Google Drive Folder -> Look at the URL -> Copy the ID after /folders/
INPUT_FOLDER_ID = "1wU3Kb_k4TwulUg-Q6r0NGy9iOe89Z0Gv"      # e.g., "1bP_xyz..."
PROCESSED_FOLDER_ID = "10owRvXdMHTaenqsmN0PbU1rTOsqfVkxq"  # e.g., "1cQ_abc..."
MASTER_SHEET_ID = "1cY6O1tDlkCRaTE9eYlqZ-culLKR2Y6Q2uHzmcuJ5Gg4" 
TEMPLATE_TAB_ID = 291296153 # The ID of the "INPUT" tab you want to clone

# ==========================================
# 2. AUTHENTICATION (SECURE FOR RENDER)
# ==========================================
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]

# Load credentials from Render Environment Variable
try:
    creds_json = os.environ.get("GOOGLE_CREDENTIALS")
    if not creds_json:
        raise ValueError("GOOGLE_CREDENTIALS environment variable not found!")
    
    key_dict = json.loads(creds_json)
    creds = ServiceAccountCredentials.from_json_keyfile_dict(key_dict, scope)
    
    # Initialize Clients
    gc = gspread.authorize(creds)
    drive_service = build('drive', 'v3', credentials=creds)
    print("✅ Authenticated with Google Successfully")

except Exception as e:
    print(f"❌ Authentication Failed: {e}")
    # We continue so Flask can start and show the error log if needed

# ==========================================
# 3. FLASK SERVER (KEEPS RENDER ALIVE)
# ==========================================
app = Flask(__name__)

@app.route('/')
def home():
    return "DBR Automation Bot is Running 24/7. Check Logs for Activity."

# ==========================================
# 4. MAPPING LOGIC
# ==========================================
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

# ==========================================
# 5. PDF PARSER (MEMORY BASED)
# ==========================================
def parse_pdf_stream(file_stream):
    data = {"Name": "[[ NOT PROVIDED ]]", "CNIC": "", "DOB": "", "Loans": []}
    
    try:
        with pdfplumber.open(file_stream) as pdf:
            full_text = ""
            for page in pdf.pages: 
                full_text += page.extract_text() + "\n"

            # --- Personal Info ---
            name_match = re.search(r"Name:\s*(.*?)(?=\s+(Father|Gender|CNIC)|$)", full_text, re.IGNORECASE)
            if name_match: data["Name"] = name_match.group(1).strip()
            
            cnic_match = re.search(r"\d{5}-\d{7}-\d", full_text)
            if cnic_match: data["CNIC"] = cnic_match.group(0)
            
            dob_match = re.search(r"Date of Birth:\s*(\d{2}/\d{2}/\d{4})", full_text)
            if dob_match: data["DOB"] = dob_match.group(1)

            # --- Loan Extraction ---
            lines = full_text.split('\n')
            current_loan = {}
            garbage = re.compile(r"^\d+\s?-\s?\d+$")
            capture_overdues = False

            for line in lines:
                line = line.strip()
                
                # New Loan Detection
                if re.match(r"^\d+\s?-\s?[A-Za-z]", line) and not garbage.match(line):
                    if current_loan: data["Loans"].append(current_loan)
                    
                    bank_part = line.split("-", 1)[1].strip() if "-" in line else line
                    current_loan = {
                        "Bank": bank_part, "Limit": 0, "Outstanding": 0, 
                        "MinDue": 0, "Start": "", "End": "",
                        "30": 0, "60": 0, "90": 0
                    }
                    capture_overdues = False
                
                if current_loan:
                    # Financials
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
                        d = re.search(r"(\d{2}/\d{2}/\d{4})", line.split("Facility Date:")[-1])
                        if d: current_loan["Start"] = d.group(1)
                    if "Maturity Date:" in line:
                        d = re.search(r"(\d{2}/\d{2}/\d{4})", line.split("Maturity Date:")[-1])
                        if d: current_loan["End"] = d.group(1)

                    # Overdues (30+, 60+, 90+)
                    if "SUMMARY OF OVERDUES" in line: capture_overdues = True
                    if capture_overdues and line.startswith("Times"):
                        nums = re.findall(r"\d+", line)
                        if len(nums) >= 3:
                            current_loan["30"] = int(nums[0])
                            current_loan["60"] = int(nums[1])
                            current_loan["90"] = int(nums[2])
                        capture_overdues = False

            if current_loan: data["Loans"].append(current_loan)
            
        return data

    except Exception as e:
        print(f"   ⚠️ Parsing Error: {e}")
        return None

# ==========================================
# 6. WORKER THREAD (DRIVE POLLER)
# ==========================================
def worker_loop():
    print("🚀 Background Worker Started. Watching Drive Folder...")
    
    while True:
        try:
            # 1. Check for PDF Files in Input Folder
            results = drive_service.files().list(
                q=f"'{INPUT_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
                fields="files(id, name)"
            ).execute()
            
            files = results.get('files', [])

            if not files:
                # No files? Sleep 15s to save resources
                time.sleep(15)
                continue

            for file in files:
                print(f"📄 Found: {file['name']} - Downloading...")

                # 2. Download File to RAM (BytesIO)
                request = drive_service.files().get_media(fileId=file['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                fh.seek(0) # Reset cursor to start of file

                # 3. Parse PDF
                print("   Parsing...")
                data = parse_pdf_stream(fh)

                if data:
                    print(f"   Writing to Sheet for {data['Name']}...")
                    
                    # 4. Sheet Logic
                    master_sh = gc.open_by_key(MASTER_SHEET_ID)
                    safe_name = data['Name'].replace("/", "_")[:25] # Limit len
                    if "NOT PROVIDED" in safe_name: safe_name = f"Unknown_{file['name'][:10]}"

                    # Create Tab
                    try:
                        # Try duplicating template
                        ws = master_sh.duplicate_sheet(source_sheet_id=TEMPLATE_TAB_ID, new_sheet_name=safe_name)
                    except:
                        # If name exists, try appending random number or open existing
                        try:
                            ws = master_sh.worksheet(safe_name)
                        except:
                            safe_name = f"{safe_name}_{int(time.time())}"
                            ws = master_sh.duplicate_sheet(source_sheet_id=TEMPLATE_TAB_ID, new_sheet_name=safe_name)

                    # Prepare Updates
                    updates = []
                    # Identity
                    updates.append({"range": "C6", "values": [[data["Name"]]]})
                    updates.append({"range": "C7", "values": [[data["CNIC"]]]})
                    updates.append({"range": "C8", "values": [[data["DOB"]]]})
                    # Age Formula
                    updates.append({"range": "C9", "values": [['=IF(C8<>"",DATEDIF(C8,TODAY(),"Y"),"")']]})
                    # Defaults
                    updates.append({"range": "H7", "values": [[0]]})
                    updates.append({"range": "H8", "values": [[0]]})
                    updates.append({"range": "C12", "values": [["[SELECT]"]]})
                    updates.append({"range": "C13", "values": [["Salaried"]]})

                    # Loans
                    start_row = 19
                    loan_rows = []
                    for loan in data["Loans"]:
                        row = [
                            "N", get_product_code(loan["Bank"]), get_term_code(loan["Bank"]),
                            loan["Limit"], "", loan["Outstanding"], loan["MinDue"],
                            loan["30"], loan["60"], loan["90"], loan["Start"], loan["End"]
                        ]
                        loan_rows.append(row)
                    
                    if loan_rows:
                        end_row = start_row + len(loan_rows) - 1
                        updates.append({"range": f"B{start_row}:M{end_row}", "values": loan_rows})

                    ws.batch_update(updates, value_input_option="USER_ENTERED")
                    print("   ✅ Sheet Updated!")

                    # 5. Move File to Processed Folder
                    drive_service.files().update(
                        fileId=file['id'],
                        addParents=PROCESSED_FOLDER_ID,
                        removeParents=INPUT_FOLDER_ID,
                        fields='id, parents'
                    ).execute()
                    print("   📂 Moved to Processed folder.")

                else:
                    print("   ⚠️ Failed to extract data. Renaming file.")
                    # Rename on Drive to indicate error? Or just leave it.
                    # Ideally move to an "Error" folder, but for now we skip move.
                
                # Sleep briefly between files
                time.sleep(2)

        except Exception as e:
            print(f"❌ Worker Error: {e}")
            time.sleep(30) # Wait longer on error

# Start the worker thread in background
threading.Thread(target=worker_loop, daemon=True).start()

# ==========================================
# 7. MAIN ENTRY
# ==========================================
if __name__ == '__main__':
    # Render assigns a port automatically
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
