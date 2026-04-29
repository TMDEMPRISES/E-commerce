import pandas as pd

def transform_fact_order_items(df_items, df_orders, df_customers):
    """
    Biến đổi, làm sạch, in ra dữ liệu lỗi và kết hợp dữ liệu để tạo ra bảng Fact order_items.
    """
    print("[Transform] Đang xử lý và kết nối dữ liệu...")

    # 1. Join các bảng lại với nhau
    df_merged = pd.merge(df_items, df_orders, on='order_id', how='inner')
    df_merged = pd.merge(df_merged, df_customers, on='customer_id', how='inner')

    # 2. Tạo ID duy nhất cho Fact table
    df_merged['id'] = df_merged['order_id'].astype(str) + "_" + df_merged['order_item_id'].astype(str)

    # 3. Đổi tên cột cho đúng chuẩn
    df_merged = df_merged.rename(columns={
        'customer_unique_id': 'user_id',
        'order_status': 'status',
        'order_purchase_timestamp': 'created_at',
        'price': 'sale_price'
    })

    # 4. Giữ lại các cột cần thiết
    columns_to_keep = ['id', 'order_id', 'user_id', 'product_id', 'status', 'created_at', 'sale_price']
    fact_df = df_merged[columns_to_keep].copy()

    print("\n[Transform] Xử lý dữ liệu lỗi!")
    
    # Tạo một list để chứa các DataFrame lỗi
    error_dfs = []

    # 1. Bắt lỗi missing values (Thiếu dữ liệu quan trọng)
    missing_mask = fact_df[['sale_price', 'user_id', 'created_at', 'order_id', 'product_id']].isna().any(axis=1)
    error_missing = fact_df[missing_mask].copy()
    if not error_missing.empty:
        error_missing['error_reason'] = 'Missing Values'
        error_dfs.append(error_missing)
        print(f"  -> Phát hiện {len(error_missing)} dòng bị thiếu dữ liệu. Mẫu 3 dòng đầu:")
        print(error_missing.head(3).to_string())

    # Loại bỏ các dòng missing khỏi luồng chính
    fact_df = fact_df[~missing_mask]

    # 2. Bắt lỗi giá bán (Giá <= 0)
    fact_df['sale_price'] = pd.to_numeric(fact_df['sale_price'], errors='coerce')
    error_price = fact_df[fact_df['sale_price'] <= 0].copy()
    if not error_price.empty:
        error_price['error_reason'] = 'Invalid Price (<= 0)'
        error_dfs.append(error_price)
        
    # Lọc giữ lại giá hợp lệ trong luồng chính
    fact_df = fact_df[fact_df['sale_price'] > 0]

    # 3. format lại ngày tháng cho chuẩn
    fact_df['created_at'] = pd.to_datetime(fact_df['created_at'], errors='coerce')

    # ==========================================
    # TỔNG HỢP VÀ XUẤT FILE LỖI (NẾU CÓ)
    # ==========================================
    if error_dfs:
        df_all_errors = pd.concat(error_dfs, ignore_index=True)
        error_file_path = 'ETL/data/error/error_records_log.csv'
        df_all_errors.to_csv(error_file_path, index=False, encoding='utf-8')
        print(f"\nĐã xuất tổng cộng {len(df_all_errors)} dòng dữ liệu lỗi ra file '{error_file_path}'!")
    else:
        print("\n Không phát hiện dòng dữ liệu lỗi nào!")

    print(f"\n[Transform] Success\nTạo bảng Fact để Load có kích thước: {fact_df.shape}")
    
    return fact_df