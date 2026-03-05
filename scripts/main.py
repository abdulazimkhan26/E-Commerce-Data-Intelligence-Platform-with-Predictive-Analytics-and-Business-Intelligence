from extract import extract_all
from transform import *
from load import load_to_db

def main():
    print("ETL Started")

    data = extract_all()
    

    dim_customer = transform_dim_customer(data["customer"])
    dim_product = transform_dim_product(data["product"])
    dim_time = transform_dim_time(data["order"])
    fact_sales = transform_fact_sales(data["order_items"], data["order"], dim_customer, dim_product, dim_time)

    load_to_db(dim_customer, dim_product, dim_time, fact_sales)    

    print("Transform Phase Completed")

if __name__ == "__main__":
    main()