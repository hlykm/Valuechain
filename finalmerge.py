import os
import pandas as pd
import glob

# 폴더 경로
folder_path = r"C:\Users\김현지M\스마일게이트자산운용\글로벌운용본부 - 문서\Collaboration\밸류체인\코드\mktcap_3000_최종_오류추가"
all_files = glob.glob(os.path.join(folder_path, "*.xlsx"))

# 결과 저장용 리스트
df_list = []

for file in all_files:
    stock_code = os.path.basename(file).split('.')[0]  # 파일명에서 종목코드 추출
    df = pd.read_excel(file)

    # 파일마다 종목코드 열 추가
    df["종목코드"] = stock_code

    df_list.append(df)

# 모든 파일 합치기
final_df = pd.concat(df_list, ignore_index=True)

# 열 순서 정리 (종목코드를 F열로)
cols = ["종목명", "대분류", "중분류", "소분류", "연관기업", "종목코드"]
final_df = final_df[cols]

# 엑셀로 저장
save_path = os.path.join(folder_path, "merged_result.xlsx")
final_df.to_excel(save_path, index=False)
print(f"병합 완료: {save_path}")
