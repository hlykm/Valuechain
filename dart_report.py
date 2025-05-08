import os
import warnings
import pandas as pd
import OpenDartReader
from tqdm import tqdm
from dateutil.relativedelta import relativedelta
from datetime import datetime as dt
from bs4 import BeautifulSoup
import urllib.request as urlreq
import json

warnings.filterwarnings('ignore')

# 📌 새로운 폴더 경로 설정
base_dir = os.path.abspath("폴더 경로 설정")
new_project_dir = os.path.join(base_dir, "코드")
new_data_dir = os.path.join(new_project_dir, "mktcap_3000_사업보고서_json")

# 📌 새로운 폴더 생성 (없으면 생성)
os.makedirs(new_data_dir, exist_ok=True)

def text_output(url_link):
    """URL에서 HTML 내용을 가져와 텍스트로 변환"""
    url_open = urlreq.urlopen(url_link).read()
    soup = BeautifulSoup(url_open, 'html5lib')
    wording = soup.select('body')[0].get_text().replace("\n", "").replace("\xa0", "")
    return wording

def proc_xml(xml_doc):
    """📌 사업 보고서 XML에서 '3. 원재료 및 생산설비'와 '4. 매출 및 수주상황' 부분을 추출 (텍스트 + 표)"""
    xml_file = BeautifulSoup(xml_doc, features="html.parser")
    section_list = xml_file.select('section-2')

    extracted_sections = []  # ✅ 모든 섹션을 저장할 리스트

    for section in section_list:
        section_text = section.get_text()
        tables = []  # ✅ 표 데이터를 저장할 리스트

        # ✅ 포함될 수 있는 모든 제목 케이스 정의
        valid_titles = [
            "3. 원재료 및 생산설비",
            "3. (제조서비스업)원재료 및 생산설비",
            "3. (금융업)원재료 및 생산설비",
            "4. 매출 및 수주상황",
            "4. (제조서비스업)매출 및 수주상황",
            "4. (금융업)매출 및 수주상황"
        ]

        # ✅ 섹션 텍스트 내에 포함된 정확한 제목 추출
        section_title = None
        for title in valid_titles:
            if title in section_text:
                section_title = title
                break

        if section_title:
            # ✅ 표 데이터 추출 및 JSON-friendly 변환
            for table in section.find_all("table"):
                try:
                    df = pd.read_html(str(table))[0]  # 표를 DataFrame으로 변환
                    tables_json = [{str(k): v for k, v in row.items()} for row in df.to_dict(orient="records")]
                    tables.append(tables_json)
                except:
                    continue  # 표가 읽히지 않으면 건너뜀

            # ✅ 섹션별 데이터 저장
            extracted_sections.append({
                "title": section_title,
                "text": section_text.strip(),
                "tables": tables
            })

    return extracted_sections  # ✅ JSON-friendly 구조로 반환

def get_buss_detail(Dart_df):
    """📌 DART에서 기업별 사업 보고서를 가져와서 JSON 저장 (기업별 개별 JSON 파일 생성)"""
    enddate = dt.today().strftime('%Y-%m-%d')

    for i in tqdm(Dart_df.index):
        try:
            code = i[1:]  # ✅ 기업 코드 추출
            startdate = dt.strftime(dt.strptime(enddate, '%Y-%m-%d') - relativedelta(months=12), '%Y-%m-%d')
            result_list = dart.list(code, start=startdate, kind='A', final=False)

            try:
                result_list['Rpt_Date'] = result_list.report_nm.apply(lambda x: str.split(x, "(")[1].replace(')', ''))
                result_list = result_list.sort_values(by='Rpt_Date', ascending=False)
            except:
                pass

            if len(result_list) == 0:
                raise ValueError('조회 데이터 없음')

            for _, result in result_list.iterrows():
                if '정정' not in result.report_nm:
                    result_temp = {
                        "report_nm": result["report_nm"],
                        "rcept_no": result["rcept_no"]
                    }
                    break

            # ✅ 텍스트 + 표 데이터 추출
            sections = proc_xml(dart.document(result_temp["rcept_no"]))

            if not sections:
                raise ValueError('Scraped wrong report.')
        except:
            try:
                code = i[1:]
                result_list = dart.list(code, end=enddate, kind='A', final=False)

                for _, result in result_list.iterrows():
                    if '정정' not in result.report_nm:
                        result_temp = {
                            "report_nm": result["report_nm"],
                            "rcept_no": result["rcept_no"]
                        }

                # ✅ 사업의 내용 섹션 URL 직접 가져오기
                listofsubdocs = dart.sub_docs(result_temp["rcept_no"], match='사업의 내용')
                dcmno = listofsubdocs.iloc[0]['url'][:-4]

                sections = [{
                    "title": "사업의 내용",
                    "text": text_output(dcmno),
                    "tables": []
                }]
            except:
                result_temp = {
                    "report_nm": "조회 데이터 없음",
                    "rcept_no": "조회 데이터 없음",
                    "sections": []
                }

        # ✅ JSON-friendly 데이터 변환
        result_temp["sections"] = sections

        # ✅ 모든 키를 문자열로 변환하여 JSON 오류 방지
        cleaned_result_temp = {str(k): v for k, v in result_temp.items()}

        # ✅ 기업별 JSON 파일 생성
        json_file_path = os.path.join(new_data_dir, f"{code}.json")
        with open(json_file_path, "w", encoding="utf-8") as f:
            json.dump(cleaned_result_temp, f, ensure_ascii=False, indent=4)

    return True


# 📌 리스트 불러오기
list_df = pd.read_excel(os.path.join(base_dir, 'data', '02.mktcap_3000.xlsx'), sheet_name='list', index_col=0)

# 📌 OpenDart API 사용 설정
api_key = 'DART API KEY'  # 🔴 실제 OpenDart API 키 입력 필요
dart = OpenDartReader(api_key)

# 📌 데이터 추출 실행
buss_df = get_buss_detail(list_df)

print('finished')

