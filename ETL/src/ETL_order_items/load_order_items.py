import pandas as pd
import os

def load_data(df, target_file_path):

    # Tạo thư mục nếu chưa tồn tại
    directory = os.path.dirname(target_file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)

    print(f"[Load] Đang lưu dữ liệu vào {target_file_path}")
    
    df.to_csv(target_file_path, index=False, encoding='utf-8')
    
    print("[Load] Xuất file thành công!")