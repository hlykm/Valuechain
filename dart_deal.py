print("✅ 코드 시작됨")  # 맨 위에 추가

import os
import warnings
import pandas as pd
import OpenDartReader
from tqdm import tqdm
from datetime import datetime as dt, timedelta
import requests
from bs4 import BeautifulSoup
import re
import time

warnings.filterwarnings('ignore')

# 📌 경로 설정
base_dir = os.path.abspath("기본 경로 입력")
new_project_dir = os.path.join(base_dir, "폴더 입력")

# ✅ 저장할 폴더 경로
new_data_dir = os.path.join(new_project_dir, "결과물 폴더명 입력")
os.makedirs(new_data_dir, exist_ok=True)  # ✅ 폴더 없으면 자동 생성

print("✅ Script started...")

# 📌 OpenDart API 설정
api_key = 'DART API KEY'  # 🔴 실제 API 키 입력 필요
dart = OpenDartReader(api_key)

# 📌 KOSPI 기업 리스트 로드
list_df = pd.read_excel(os.path.join(base_dir, 'data', '02.mktcap_3000.xlsx'), sheet_name='list', index_col=0)

def get_dcm_no(rcp_no):
    """📌 DART에서 dcmNo 찾기"""
    main_url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcp_no}"
    headers = {"User-Agent": "Mozilla/5.0"}

    response = requests.get(main_url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    script_tag = soup.find("script", string=re.compile("viewDoc"))
    if script_tag:
        match = re.search(r'viewDoc\(".*?",\s*"(\d+)"', script_tag.text)
        if match:
            return match.group(1)
    return None

def get_dart_document_url(rcp_no, dcm_no):
    """📌 DART 본문 HTML URL 가져오기"""
    return f"https://dart.fss.or.kr/report/viewer.do?rcpNo={rcp_no}&dcmNo={dcm_no}&dtd=HTML"

def extract_contract_info(rcp_no):
    """📌 DART에서 계약 정보 추출"""
    dcm_no = get_dcm_no(rcp_no)
    if not dcm_no:
        print(f"❌ dcmNo not found for rcp_no: {rcp_no}")
        return {"contract_type": "조회 실패", "contract_name": "조회 실패", "contract_party": "조회 실패"}

    doc_url = get_dart_document_url(rcp_no, dcm_no)

    print(f"🔍 Fetching contract details from: {doc_url}")
    time.sleep(3)

    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(doc_url, headers=headers)

    # 🔹 인코딩 확인 및 설정
    encoding = response.apparent_encoding
    response.encoding = encoding
    print(f"✅ Detected encoding: {encoding}")

    soup = BeautifulSoup(response.text, 'html.parser')

    contract_info = {
        "contract_type": "",
        "contract_name": "",
        "contract_party": ""
    }

    # 🔍 공시 본문에서 계약 정보 추출 (iframe 없이 직접 찾기)
    for row in soup.find_all("tr"):
        cells = row.find_all(["th", "td"])
        if len(cells) > 1:
            header = cells[0].get_text(strip=True)
            value = cells[1].get_text(strip=True)

            if "판매ㆍ공급계약 구분" in header:
                contract_info["contract_type"] = value
            elif "체결계약명" in header or "세부내용" in header:  # 🔹 체결계약명 또는 세부내용
                contract_info["contract_name"] = value
            elif "계약상대" in header:
                contract_info["contract_party"] = value

    if not any(contract_info.values()):  # 데이터가 없을 경우
        print("❌ 계약 정보를 찾을 수 없음. HTML 구조 변경 확인 필요.")

    return contract_info

def process_disclosures(Dart_df):
    """📌 공시 데이터를 처리하여 개별 엑셀 파일로 저장"""
    for i in tqdm(Dart_df.index):
        try:
            code = i[1:]

            # ✅ 다트 API에서 기업명 가져오기
            company_info = dart.company(code)
            stock_name = company_info['corp_name'] if company_info is not None else "조회 실패"

            enddate = dt.today().strftime('%Y-%m-%d')
            startdate = (dt.today() - timedelta(days=3*365)).strftime('%Y-%m-%d')

            result_list = dart.list(code, start=startdate, end=enddate, kind='I', final=False)
            disclosures = result_list[result_list.report_nm.str.contains("공급계약체결")]

            if disclosures.empty:
                continue  # 🔹 공시가 없으면 다음 기업으로

            extracted_reports = []
            for _, result in disclosures.iterrows():
                rcp_no = result['rcept_no']
                contract_info = extract_contract_info(rcp_no)

                # ✅ 엑셀 양식에 맞게 데이터 추가
                extracted_reports.append({
                    "종목명": stock_name,
                    "대분류": contract_info["contract_type"],  # ✅ contract_type → 대분류
                    "중분류": "판매처",  # ✅ "판매처"로 고정
                    "소분류": contract_info["contract_name"],  # ✅ contract_name → 소분류
                    "연관기업": contract_info["contract_party"],  # ✅ contract_party → 연관기업
                })

            # ✅ DataFrame 변환 및 중복 제거
            df_result = pd.DataFrame(extracted_reports)

            # ✅ 🔥 중복 제거 (대분류, 소분류, 연관기업이 동일한 경우)
            df_result.drop_duplicates(subset=["대분류", "소분류", "연관기업"], keep="first", inplace=True)

            # ✅ 공시 데이터가 있는 경우만 파일 저장
            if not df_result.empty:
                excel_output_path = os.path.join(new_data_dir, f"{code}.xlsx")  # ✅ 기업별 개별 파일 저장
                df_result.to_excel(excel_output_path, index=False, engine="openpyxl")
                print(f"✅ 저장 완료: {excel_output_path}")

        except Exception as e:
            print(f"❌ Error processing disclosures for {code}: {e}")
            continue

# ✅ 실행
process_disclosures(list_df)
print("✅ Data extraction complete.")
