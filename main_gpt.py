import json
import pandas as pd
import os
import openai
import glob
import tiktoken
import time
import zipfile
import requests
import xml.etree.ElementTree as ET
import re
from langchain.text_splitter import RecursiveCharacterTextSplitter

# ▶️ DART API 키 설정
DART_API_KEY = "DART API KEY"
CORP_CODE_URL = f"https://opendart.fss.or.kr/api/corpCode.xml?crtfc_key={DART_API_KEY}"
ZIP_FILE = "corp_code.zip"
XML_FILE = "CORPCODE.xml"

# ▶️ OpenAI API 키 설정
openai.api_key = "OPENAI API KEY"
openai.organization = "ORGANIZATION"

# ▶️ JSON 및 결과 폴더 경로
json_folder = r"json 폴더 경로 입력"
output_folder = r"결과 폴더 경로 입력"
os.makedirs(output_folder, exist_ok=True)

# ▶️ GPT 토큰 제한
TOKEN_LIMIT = 6000
tokenizer = tiktoken.get_encoding("cl100k_base")

# ▶️ DART 종목코드 → 기업명 매핑
def download_and_extract_corpcode():
    if not os.path.exists(XML_FILE):
        print("\ud83d\udce6 DART \uae30\uc5b5 \ucf54\ub4dc zip \ub2e4\uc6b4\ub85c\ub4dc \uc911...")
        response = requests.get(CORP_CODE_URL)
        with open(ZIP_FILE, "wb") as f:
            f.write(response.content)
        with zipfile.ZipFile(ZIP_FILE, "r") as zip_ref:
            zip_ref.extractall()
        print("\u2705 CORPCODE.xml \ucd94\ucd9c \uc644\ub8cc")

def load_stock_code_to_company_name():
    stock_to_name = {}
    download_and_extract_corpcode()
    tree = ET.parse(XML_FILE)
    root = tree.getroot()

    for corp in root.findall("list"):
        stock_code = corp.find("stock_code").text
        corp_name = corp.find("corp_name").text
        if stock_code:
            stock_to_name[stock_code] = corp_name

    return stock_to_name

# ▶️ 텍스트 전처리
def preprocess_text(raw_text, max_tokens=TOKEN_LIMIT):
    token_count = len(tokenizer.encode(raw_text))
    if token_count <= max_tokens:
        return raw_text

    sentences = raw_text.split("\n")
    trimmed_text = []
    total_length = 0

    for sentence in sentences:
        sentence_length = len(tokenizer.encode(sentence))
        if total_length + sentence_length > max_tokens:
            break
        trimmed_text.append(sentence)
        total_length += sentence_length

    return "\n".join(trimmed_text)

# ▶️ GPT 응답 마크다운 제거 및 JSON 블록만 추출
def clean_response(response_text):
    text = re.sub(r"```json|```", "", response_text).strip()
    match = re.search(r"\{.*\}", text, re.DOTALL)
    if match:
        return match.group(0).strip()
    else:
        return text

# ▶️ GPT 분석 함수 (자동 재시도 포함)
def analyze_text_with_gpt(company_name, raw_text, max_retry=3):
    trimmed_text = preprocess_text(raw_text)

    prompt = f"""
You are an AI assistant specializing in analyzing financial and business reports.
The text provided below is a **pre-downloaded offline business report**.
You must ONLY use the given text to extract information.
Do NOT try to browse the internet or assume you lack access to the data.
This is NOT real-time fetching — all necessary information is already included.

Your task is to extract **clear and specific company names** related to the target company’s supply chain and industry classification.
Accuracy is critical for investment and value chain analysis.

Extract in the exact JSON format:
{{
  "industry": "Main industry sector of the target company",
  "suppliers": [{{"category": "Raw material or service category", "company": "Exact name of supplier company"}}],
  "buyers": [{{"category": "Product or service type", "company": "Exact name of buyer company"}}]
}}

Rules:
- Do NOT include vague or group-like terms such as "various companies", "multiple clients", "domestic manufacturers", "불명", "협력업체", or anything similar.
- Only include **real, specific, and identifiable company names** like "Samsung Electronics", "SK hynix", etc.
- If the company name is not explicitly mentioned or is unclear, **exclude that entry entirely** from the output.
- Maintain the **original language** of the source content (Korean if written in Korean, English if written in English).
- Ensure that each entry for 'suppliers' and 'buyers' includes only **one company per category per line** to maintain clarity and precision in data extraction.
- **If multiple company names are separated by commas or "등", extract each of them as a separate entry**, even if they appear in the same sentence or cell.

Example Output:
{{
  "industry": "철강",
  "suppliers": [
    {{"category": "원재료", "company": "POSCO"}}
  ],
  "buyers": [
    {{"category": "자동차 부품", "company": "현대모비스"}}
  ]
}}

{{
  "industry": "반도체", 
  "suppliers": [ 
    {{"category": "웨이퍼", "company": "SK실트론"}}, 
    {{"category": "포토레지스트", "company": "동진쎄미켐"}} 
  ],
  "buyers": [ 
    {{"category": "반도체", "company": "삼성전자"}}, 
    {{"category": "반도체", "company": "TSMC"}} 
  ]
}}

Now analyze the following offline business report for: {company_name}

{trimmed_text}
"""

    attempt = 0
    while attempt < max_retry:
        try:
            response = openai.ChatCompletion.create(
                model="gpt-4",
                messages=[
                    {"role": "system", "content": "You extract business relations and industry classification from financial reports."},
                    {"role": "user", "content": prompt}
                ]
            )
            raw_content = response.choices[0].message.content
            print("📤 GPT 응답:")
            print(raw_content.encode("utf-8", errors="replace").decode("utf-8"))

            if any(kw in raw_content.lower() for kw in ["don't have access", "cannot access", "cannot browse"]):
                print("\u26a0\ufe0f \uc774\uc0c1 \uc751\ub2f5 \uac10\uc9c0, \uc7ac\uc2dc\ub3c4...")
                attempt += 1
                time.sleep(3)
                continue

            cleaned_content = clean_response(raw_content)
            parsed_json = json.loads(cleaned_content)
            return parsed_json, raw_content[:100]

        except json.JSONDecodeError:
            print("\u274c JSON \ud30c\uc2f1 \uc2e4\ud328. \uc7ac\uc2dc\ub3c4...")
            attempt += 1
            time.sleep(3)

        except openai.error.RateLimitError as e:
            wait_time = 30
            print(f"\u26a0\ufe0f Rate Limit \ubc1c\uc0dd, {wait_time}\ucd08 \ub300\uae30 \ud6c4 \uc7ac\uc2dc\ub3c4...")
            time.sleep(wait_time)

        except Exception as e:
            print(f"\u26a0\ufe0f GPT \uc694\uccad \uc911 \uc608\uc678 \ubc1c\uc0dd: {e}")
            attempt += 1
            time.sleep(3)

    print("\ud83d\udeab \ucd5c\ub300 \uc7ac\uc2dc\ub3c4 \ucd08\uacfc. \ubd84\uc11d \uc2e4\ud328.")
    return None, ""

# ▶️ 전체 JSON 파일 처리
if __name__ == "__main__":
    stock_to_name = load_stock_code_to_company_name()
    json_files = glob.glob(os.path.join(json_folder, "*.json"))
    total_files = len(json_files)

    fail_list = []

    for idx, json_file in enumerate(json_files, start=1):
        try:
            with open(json_file, "r", encoding="utf-8") as file:
                data = json.load(file)

            ticker_code = os.path.basename(json_file).replace(".json", "")
            company_name = stock_to_name.get(ticker_code, ticker_code)

            print(f"▶ [{idx}/{total_files}] Processing: {ticker_code} ({company_name})...")

            full_text = "\n".join([sec["text"] for sec in data.get("sections", [])])
            extracted_data, response_summary = analyze_text_with_gpt(company_name, full_text)

            if extracted_data is None:
                fail_list.append([ticker_code, company_name, "Parsing Failed or Empty Response", response_summary])
                continue

            industry_category = extracted_data.get("industry", "기타")
            processed_data = []

            for supplier in extracted_data.get("suppliers", []):
                processed_data.append([company_name, industry_category, "공급처", supplier["category"], supplier["company"]])

            for buyer in extracted_data.get("buyers", []):
                processed_data.append([company_name, industry_category, "판매처", buyer["category"], buyer["company"]])

            df = pd.DataFrame(processed_data, columns=["종목명", "대분류", "중분류", "소분류", "연관기업"])
            excel_path = os.path.join(output_folder, f"{ticker_code}.xlsx")
            df.to_excel(excel_path, index=False)

            print(f"✅ [{idx}/{total_files}] {ticker_code} ({company_name}): 엑셀 저장 완료 → {excel_path}")

        except Exception as e:
            print(f"⚠️ [{idx}/{total_files}] {json_file} 처리 중 오류 발생: {str(e)}")
            ticker_code = os.path.basename(json_file).replace(".json", "")
            company_name = stock_to_name.get(ticker_code, ticker_code)
            fail_list.append([ticker_code, company_name, f"Exception: {str(e)}", ""])

    if fail_list:
        fail_df = pd.DataFrame(fail_list, columns=["종목코드", "기업명", "실패 이유", "GPT 응답 요약"])
        fail_path = os.path.join(output_folder, "fail_list.xlsx")
        fail_df.to_excel(fail_path, index=False)
        print(f"❗ 실패한 종목 {len(fail_list)}개 저장 완료 → {fail_path}")
    else:
        print("✅ 실패한 종목 없이 전부 성공했습니다!")
