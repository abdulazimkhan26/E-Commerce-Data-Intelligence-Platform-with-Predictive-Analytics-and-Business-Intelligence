from sqlalchemy import create_engine, text

engine = create_engine("postgresql+psycopg2://postgres:260849@localhost:5433/Olist")

def load_to_db(dim_customer, dim_product, dim_time, fact_sales):
    # Drop fact tables first to avoid foreign key issues
    with engine.begin() as conn:
        conn.execute(text("DROP TABLE IF EXISTS warehouse.fact_sales CASCADE"))
        conn.execute(text("DROP TABLE IF EXISTS warehouse.fact_reviews CASCADE"))

    # Load dimension tables
    dim_customer.to_sql("dim_customer", engine, schema="warehouse", if_exists="replace", index=False)
    dim_product.to_sql("dim_product", engine, schema="warehouse", if_exists="replace", index=False)
    dim_time.to_sql("dim_time", engine, schema="warehouse", if_exists="replace", index=False)

    # Load fact tables
    fact_sales.to_sql("fact_sales", engine, schema="warehouse", if_exists="replace", index=False)
    # fact_reviews.to_sql("fact_reviews", engine, schema="warehouse", if_exists="replace", index=False)

    print("Load completed successfully")