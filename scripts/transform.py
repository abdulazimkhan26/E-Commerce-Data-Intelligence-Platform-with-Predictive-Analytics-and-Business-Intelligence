import pandas as pd

categoryTranslation_df = pd.read_csv(r"C:\Users\abdul\OneDrive\Documents\E-Commerce Data Intelligence Platform with Predictive Analytics and Business Intelligence\raw_data\product_category_name_translation.csv")

def transform_dim_customer(customer_df):
    
    df = customer_df.copy()

    customer_df.rename(columns={
    'customer_zip_code_prefix': 'zip_code',
    'customer_city': 'city',
    'customer_state': 'state'
    }, inplace=True)

    df = df.drop_duplicates(subset=["customer_id"])

    df.insert(0, "customer_key", range(1, len(df)+1))

    print("Customer transformation completed", df.shape)

    return df

def transform_dim_product(product_df):
    df = product_df.copy()

    df = df.iloc[:, :2]

    df.rename(columns={
    'product_category_name': 'product_category'}, inplace=True)

    df['product_category'] = df['product_category'].str.replace('_', ', ')
    unique_category = df['product_category'].unique()
    print(unique_category)

    df['product_category'] = df['product_category'].fillna('UNDEFINED CATEGORY') 
    df['product_category'].isna().sum()

    df = df.drop_duplicates(subset=["product_id"])

    df.insert(0, "product_key", range(1, len(df)+1))

    print("Product transformation completed", df.shape)

    return df

def transform_dim_time(orders_df):
    df = orders_df.copy()

    df["order_date"] = pd.to_datetime(df["order_purchase_timestamp"]).dt.date

    df = df[["order_date"]].drop_duplicates().sort_values("order_date").reset_index(drop=True)

    df["year"] = pd.to_datetime(df["order_date"]).dt.year
    df["month"] = pd.to_datetime(df["order_date"]).dt.month
    df["day"] = pd.to_datetime(df["order_date"]).dt.day
    df["quarter"] = pd.to_datetime(df["order_date"]).dt.quarter

    df.insert(0, "time_key", range(1, len(df) + 1))

    print("dim_time created:", df.shape)
    return df

def transform_fact_sales(order_items_df, orders_df,
                         dim_customer_df, dim_product_df, dim_time_df):

    df = order_items_df.merge(orders_df[["order_id", "customer_id","order_purchase_timestamp"]], on="order_id", how="left")

    df = df.merge(dim_customer_df[["customer_id", "customer_key"]],
                  on="customer_id", how="left")

    df = df.merge(dim_product_df[["product_id", "product_key"]],
                  on="product_id", how="left")

    df["order_date"] = pd.to_datetime(df["order_purchase_timestamp"]).dt.date

    df = df.merge(dim_time_df[["order_date", "time_key"]],
                  on="order_date", how="left")

    fact = df[[
        "order_id",
        "customer_key",
        "product_key",
        "time_key",
        "order_item_id",
        "price",
        "freight_value"
    ]]

    fact.insert(0, "sales_key", range(1, len(fact) + 1))

    print(fact.isnull().sum())
    print("fact_sales created:", fact.shape)

    print(fact.head())

    return fact