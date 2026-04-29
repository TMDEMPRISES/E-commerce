import pandas as pd

def load_raw_data(file_path):
    """Đọc dữ liệu từ file CSV gốc"""
    try:
        return pd.read_csv(file_path)
    except FileNotFoundError:
        print(f"Lỗi: Không tìm thấy file tại {file_path}")
        return None