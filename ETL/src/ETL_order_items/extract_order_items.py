import pandas as pd
import os

def extract_data(file_path):
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")
    
    print(f"[Extract] Đang đọc dữ liệu từ {file_path}!")
    return pd.read_csv(file_path)
