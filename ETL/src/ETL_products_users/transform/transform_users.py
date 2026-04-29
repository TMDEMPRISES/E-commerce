import pandas as pd

def transform_users_data(df):
    """Thực hiện làm sạch và biến đổi dữ liệu khách hàng"""
    # 1. Chọn và đổi tên cột
    users_dim = df[['customer_unique_id', 'customer_age', 'customer_gender', 
                    'customer_city', 'customer_state']].copy()
    users_dim.columns = ['id', 'age', 'gender', 'city', 'state']

    # 2. Thêm cột quốc gia
    users_dim['country'] = 'USA'

    # 3. Xử lý trùng lặp
    users_dim = users_dim.drop_duplicates(subset=['id'], keep='first')

    # 4. Chuẩn hóa chuỗi
    users_dim['city'] = users_dim['city'].astype(str).str.strip().str.title()
    users_dim['state'] = users_dim['state'].astype(str).str.strip().str.upper()

    return users_dim