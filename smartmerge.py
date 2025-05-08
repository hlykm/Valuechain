import pandas as pd
import os
import glob
from difflib import SequenceMatcher

# 📁 폴더 경로 설정
folder_a = r"합칠 파일이 있는 경로 입력"
folder_b = r"합칠 파일이 있는 또 다른 폴더 경로 입력"
output_folder = r"결과물 경로 입력"
os.makedirs(output_folder, exist_ok=True)

# 📐 유사도 판단 함수
def is_similar(a, b, threshold=0.85):
    return SequenceMatcher(None, str(a), str(b)).ratio() >= threshold

# 📄 모든 파일 수집
files_a = {os.path.basename(f): f for f in glob.glob(os.path.join(folder_a, "*.xlsx"))}
files_b = {os.path.basename(f): f for f in glob.glob(os.path.join(folder_b, "*.xlsx"))}
all_keys = set(files_a.keys()).union(set(files_b.keys()))

print(f"📊 총 파일 수: A폴더({len(files_a)}개), B폴더({len(files_b)}개), 병합 대상({len(all_keys)}개)\n")

# 🔁 병합 처리
for filename in sorted(all_keys):
    path_a = files_a.get(filename)
    path_b = files_b.get(filename)

    df_all = []

    # A 폴더에서 로드
    if path_a:
        try:
            df_a = pd.read_excel(path_a)
            if "종목명" in df_a.columns and not df_a.empty:
                df_all.append(df_a)
            else:
                print(f"⚠️ {filename} - A 폴더 파일 비어있거나 '종목명' 없음")
        except Exception as e:
            print(f"❌ {filename} - A 폴더 파일 읽기 실패: {e}")

    # B 폴더에서 로드
    if path_b:
        try:
            df_b = pd.read_excel(path_b)
            if "종목명" in df_b.columns and not df_b.empty:
                df_all.append(df_b)
            else:
                print(f"⚠️ {filename} - B 폴더 파일 비어있거나 '종목명' 없음")
        except Exception as e:
            print(f"❌ {filename} - B 폴더 파일 읽기 실패: {e}")

    # 둘 다 비었으면 패스
    if not df_all:
        print(f"⏭️ {filename} - 병합할 데이터 없음 (둘 다 비었거나 읽기 실패)")
        continue

    merged_df = pd.concat(df_all, ignore_index=True)

    # ✅ 필터링: 정확하지 않은 연관기업 + 종목명 없는 행 제거
    merged_df = merged_df[
        ~merged_df["연관기업"].fillna("").str.strip().str.contains("불명|미상|협력업체|기타", case=False) &
        merged_df["종목명"].notna() &
        (merged_df["종목명"].astype(str).str.strip() != "")
    ]

    # ✅ 중복 제거: 중분류 + 유사한 연관기업 기준
    seen = []
    cleaned_rows = []

    for _, row in merged_df.iterrows():
        mid = row.get("중분류")
        company = row.get("연관기업")

        if pd.isna(mid) or pd.isna(company):
            continue

        duplicate = False
        for existing_mid, existing_company in seen:
            if mid == existing_mid and is_similar(company, existing_company):
                duplicate = True
                break

        if not duplicate:
            seen.append((mid, company))
            cleaned_rows.append(row)

    result_df = pd.DataFrame(cleaned_rows)

    # 저장
    output_path = os.path.join(output_folder, filename)
    result_df.to_excel(output_path, index=False)
    print(f"✅ 병합 완료: {filename} → {output_path}")

print("\n🎉 병합 전체 완료! 결과 경로:", output_folder)
