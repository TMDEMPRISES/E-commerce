import pandas as pd

def save_processed_data(df, output_path): # Kiểm tra kỹ tên hàm ở đây
    """Lưu DataFrame đã xử lý ra file CSV sạch"""
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"Đã lưu dữ liệu sạch vào: {output_path}")