import pandas as pd
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_PATH = os.path.join(BASE_DIR, "raw_data")

def load_csv(file_name):
    file_path = os.path.join(DATA_PATH,file_name)
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File {file_name} not found in {DATA_PATH}")
    
    df = pd.read_csv(file_path)
    print(f"Loaded {file_name} | Rows: {df.shape[0]}")
    return df

def extract_all():

    datasets = {
        "customer": load_csv("olist_customers_dataset.csv"),
        "order": load_csv("olist_orders_dataset.csv"),
        "order_items": load_csv("olist_order_items_dataset.csv"),
        "product": load_csv("olist_products_dataset.csv"),
        "payment": load_csv("olist_order_payments_dataset.csv"),
        "review": load_csv("olist_order_reviews_dataset.csv"),
        "seller": load_csv("olist_sellers_dataset.csv")       
    }

    print("All datasets extracted successfully ✅")

    return datasets