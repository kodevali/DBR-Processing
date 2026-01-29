import os
import time
import threading
import pdfplumber
import re
import gspread
import io
from flask import Flask
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from oauth2client.service_account import ServiceAccountCredentials

# --- CONFIGURATION (GET THESE FROM DRIVE URL) ---
# Example: drive.google.com/drive/folders/1A2B3C... -> ID is "1A2B3C..."
INPUT_FOLDER_ID = "1wU3Kb_k4TwulUg-Q6r0NGy9iOe89Z0Gv"
PROCESSED_FOLDER_ID = "10owRvXdMHTaenqsmN0PbU1rTOsqfVkxq"
MASTER_SHEET_ID = "1cY6O1tDlkCRaTE9eYlqZ-culLKR2Y6Q2uHzmcuJ5Gg4"
CREDENTIALS_FILE = "credentials.json"

# --- AUTHENTICATION ---
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
gc = gspread.authorize(creds)
drive_service = build('drive', 'v3', credentials=creds)

app = Flask(__name__)

# --- PARSING LOGIC ---
def get_product_code(text):
    t = text.lower()
    if "card" in t: return 8
    if "auto" in t or "car" in t or "lease" in t or "ijara" in t: return 2
    if "personal" in t or "finance" in t: return 6
    if "running" in t or "od" in t: return 34
    return text 

def get_term_code(text):
    t = text.lower()
    if "card" in t or "running" in t: return "E"
    return "T"

def parse_pdf_bytes(file_stream):
    data = {"Name": "[[ NOT PROVIDED ]]", "CNIC": "", "DOB": "", "Loans": []}
    try:
        with pdfplumber.open(file_stream) as pdf:
            full_text = ""
            for page in pdf.pages: full_text += page.extract_text() + "\n"

            # Identity
            name_match = re.search(r"Name:\s*(.*?)(?=\s+(Father|Gender|CNIC)|$)", full_text, re.IGNORECASE)
            if name_match: data["Name"] = name_match.group(1).strip()
            
            cnic_match = re.search(r"\d{5}-\d{7}-\d", full_text)
            if cnic_match: data["CNIC"] = cnic_match.group(0)
            
            dob_match = re.search(r"Date of Birth:\s*(\d{2}/\d{2}/\d{4})", full_text)
            if dob_match: data["DOB"] = dob_match.group(1)

            # Loans
            lines = full_text.split('\n')
            current_loan = {}
            garbage = re.compile(r"^\d+\s?-\s?\d+$")

            for line in lines:
                line = line.strip()
                if re.match(r"^\d+\s?-\s?[A-Za-z]", line) and not garbage.match(line):
                    if current_loan: data["Loans"].append(current_loan)
                    bank_part = line.split("-", 1)[1].strip() if "-" in line else line
                    current_loan = {"Bank": bank_part, "Limit": 0, "Outstanding": 0, "Start": "", "End": "", "MinDue": 0, "30": 0, "60": 0, "90": 0}
                
                if current_loan:
                    if "Loan Limit:" in line:
                        val = re.search(r"[\d,]+", line.split("Limit:")[-1])
                        if val: current_loan["Limit"] = int(val.group(0).replace(",", ""))
                    if "Outstanding Balance:" in line:
                        val = re.search(r"[\d,]+", line.split("Balance:")[-1])
                        if val: current_loan["Outstanding"] = int(val.group(0).replace(",", ""))
                    if "Min Amount Due:" in line:
                        val = re.search(r"[\d,]+", line.split("Due:")[-1])
                        if val: current_loan["MinDue"] = int(val.group(0).replace(",", ""))
                    if "Facility Date:" in line:
                        d = re.search(r"(\d{2}/\d{2}/\d{4})", line.split("Facility Date:")[-1])
                        if d: current_loan["Start"] = d.group(1)
                    if "Maturity Date:" in line:
                        d = re.search(r"(\d{2}/\d{2}/\d{4})", line.split("Maturity Date:")[-1])
                        if d: current_loan["End"] = d.group(1)
                    if "Times" in line and "30+" in full_text: # Simple check for overdues
                        nums = re.findall(r"\d+", line)
                        if len(nums) >= 3:
                            current_loan["30"], current_loan["60"], current_loan["90"] = int(nums[0]), int(nums[1]), int(nums[2])

            if current_loan: data["Loans"].append(current_loan)
        return data
    except Exception as e:
        print(f"Error Parsing: {e}")
        return None

def process_drive_files():
    print("🤖 Background Worker Started...")
    while True:
        try:
            # List files in INPUT folder
            results = drive_service.files().list(
                q=f"'{INPUT_FOLDER_ID}' in parents and mimeType='application/pdf' and trashed=false",
                fields="files(id, name)").execute()
            items = results.get('files', [])

            if not items:
                print("💤 No files found. Sleeping...")
            
            for file in items:
                print(f"📄 Found file: {file['name']} ({file['id']})")
                
                # Download File to Memory
                request = drive_service.files().get_media(fileId=file['id'])
                fh = io.BytesIO()
                downloader = MediaIoBaseDownload(fh, request)
                done = False
                while done is False:
                    status, done = downloader.next_chunk()
                
                fh.seek(0) # Reset pointer
                
                # Parse
                data = parse_pdf_bytes(fh)
                
                if data:
                    # Update Sheet
                    safe_name = data['Name'].replace("/", "_")
                    if "NOT PROVIDED" in safe_name: safe_name = f"Unknown_{file['name']}"
                    
                    master_sh = gc.open_by_key(MASTER_SHEET_ID)
                    
                    # Duplicate Template Logic
                    try:
                        template_id = 291296153
                        ws = master_sh.duplicate_sheet(source_sheet_id=template_id, new_sheet_name=safe_name[:25])
                    except:
                        try:
                            ws = master_sh.worksheet(safe_name[:25])
                        except:
                            print("Skipping... Sheet name issue")
                            continue

                    # Fill Data
                    updates = []
                    updates.append({"range": "C6", "values": [[data["Name"]]]})
                    updates.append({"range": "C7", "values": [[data["CNIC"]]]})
                    updates.append({"range": "C8", "values": [[data["DOB"]]]})
                    updates.append({"range": "C9", "values": [['=IF(C8<>"",DATEDIF(C8,TODAY(),"Y"),"")']]})
                    updates.append({"range": "H7", "values": [[0]]})
                    updates.append({"range": "H8", "values": [[0]]})
                    updates.append({"range": "C12", "values": [["[SELECT]"]]})
                    updates.append({"range": "C13", "values": [["Salaried"]]})

                    start_row = 19
                    loan_rows = []
                    for loan in data["Loans"]:
                        row = ["N", get_product_code(loan["Bank"]), get_term_code(loan["Bank"]), 
                               loan["Limit"], "", loan["Outstanding"], loan["MinDue"], 
                               loan["30"], loan["60"], loan["90"], loan["Start"], loan["End"]]
                        loan_rows.append(row)
                    
                    if loan_rows:
                        updates.append({"range": f"B{start_row}:M{start_row+len(loan_rows)-1}", "values": loan_rows})
                    
                    ws.batch_update(updates, value_input_option="USER_ENTERED")
                    
                    # MOVE FILE TO PROCESSED
                    # Add new parent, remove old parent
                    drive_service.files().update(
                        fileId=file['id'],
                        addParents=PROCESSED_FOLDER_ID,
                        removeParents=INPUT_FOLDER_ID,
                        fields='id, parents').execute()
                    
                    print(f"✅ Processed & Moved: {file['name']}")

        except Exception as e:
            print(f"❌ Error in loop: {e}")
        
        time.sleep(15) # Check every 15 seconds

# Start Background Thread
threading.Thread(target=process_drive_files, daemon=True).start()

@app.route('/')
def home():
    return "DBR Bot is Running 24/7"

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 10000))
    app.run(host='0.0.0.0', port=port)
