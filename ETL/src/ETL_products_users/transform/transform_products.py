import pandas as pd

def transform_products_data(df):
    """Thực hiện làm sạch và tính toán lợi nhuận cho bảng Products"""
    # 1. Chọn và đổi tên cột
    cols = {
        'product_id': 'id',
        'product_category_name': 'category',
        'product_name': 'name',
        'product_brand': 'brand',
        'cost': 'cost',
        'price': 'price'
    }
    df_clean = df[cols.keys()].rename(columns=cols).copy()

    # 2. Ép kiểu dữ liệu số (xử lý lỗi nếu có ký tự lạ)
    df_clean['cost'] = pd.to_numeric(df_clean['cost'], errors='coerce')
    df_clean['price'] = pd.to_numeric(df_clean['price'], errors='coerce')

    # 3. Xử lý dữ liệu thiếu và trùng lặp
    df_clean = df_clean.fillna({'category': 'Unknown', 'brand': 'Unknown'})
    df_clean = df_clean.drop_duplicates(subset=['id'])

    # 4. Tính toán lợi nhuận và làm tròn
    df_clean['profit'] = (df_clean['price'] - df_clean['cost']).round(2)

    return df_clean