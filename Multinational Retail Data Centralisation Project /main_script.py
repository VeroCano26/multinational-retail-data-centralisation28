# main_script.py

from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

def main():
    # Initialize instances
    db_connector = DatabaseConnector()
    data_extractor = DataExtractor()
    data_cleaner = DataCleaning()

    # Step 5: Read data from RDS database
    user_table_name = "sales_data"  # Replace with the actual table name
    user_data = data_extractor.read_rds_table(db_connector, user_table_name)

    # Step 6: Clean user data
    cleaned_user_data = data_cleaner.clean_user_data(user_data)

    # Step 8: Upload cleaned user data to the database (dim_users)
    cleaned_user_table_name = "dim_users"
    db_connector.upload_to_db(cleaned_user_data, cleaned_user_table_name)

    # Step 9: Extract and clean store details using the API
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    api_headers = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
    num_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, api_headers)

    # Assuming you have the endpoint for retrieving store details
    store_details_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/"
    store_data = data_extractor.extract_store_details(store_details_endpoint, num_stores, api_headers)

    # Clean store data
    cleaned_store_data = data_cleaner.clean_store_data(store_data)

    # Step 10: Upload cleaned store data to the database (dim_stores)
    cleaned_store_table_name = "dim_stores"
    db_connector.upload_to_db(cleaned_store_data, cleaned_store_table_name)

    # Print the first few rows of the uploaded store data
    if cleaned_store_data is not None:
        print(cleaned_store_data.head())

    # Step 11: Retrieve details for each store from the API
    store_numbers = [1, 2, 3, ...]  # Replace with your actual list of store numbers

    # Loop through each store number and retrieve details
    for store_number in store_numbers:
        store_details = data_extractor.retrieve_store_details(store_number, api_headers)
        # Process/store the retrieved details as needed

     # Step 12: Extract and clean product details using the S3 bucket
    products_s3_address = "s3://data-handling-public/products.csv"
    product_data = data_extractor.extract_from_s3(products_s3_address)

    # Clean product weights
    cleaned_product_data = data_cleaner.convert_product_weights(product_data)

    # Step 13: Upload cleaned product data to the database (dim_products)
    cleaned_product_table_name = "dim_products"
    db_connector.upload_to_db(cleaned_product_data, cleaned_product_table_name)

    # Print the first few rows of the uploaded product data
    if cleaned_product_data is not None:
        print(cleaned_product_data.head())

    # Step 14: Insert data into the sales_data database (dim_products)
    # Note: Adjust the table and database names as needed
    db_connector.insert_into_sales_data(cleaned_product_data, "dim_products")

    # Step 15: List the orders table in the RDS database
    engine = db_connector.init_db_engine()
    orders_table_name = data_extractor.list_orders_table(engine)

    # Step 16: Extract orders data
    orders_data = data_extractor.read_rds_table(db_connector, orders_table_name)

    # Display the first few rows of the orders data
    print("Orders Data:")
    print(orders_data.head())

    # Step 17: Clean orders data
    cleaned_orders_data = data_cleaner.clean_orders_data(orders_data)

    # Display the first few rows of the cleaned orders data
    print("Cleaned Orders Data:")
    print(cleaned_orders_data.head())

    # Step 18: Upload cleaned orders data to the database (orders_table)
    orders_table_name = "orders_table"  # Adjust the table name as needed
    db_connector.upload_to_db(cleaned_orders_data, orders_table_name)

    # Step 19: Retrieve and clean date events data
    date_events_s3_address = "s3://data-handling-public/date_details.json"
    date_events_data = data_extractor.extract_from_s3(date_events_s3_address)

    # Clean date events data (assuming you have a method named clean_date_events_data)
    cleaned_date_events_data = data_cleaner.clean_date_events_data(date_events_data)

    # Display the first few rows of the cleaned date events data
    print("Cleaned Date Events Data:")
    print(cleaned_date_events_data.head())

    # Step 20: Upload cleaned date events data to the database (dim_date_times)
    date_times_table_name = "dim_date_times"  # Adjust the table name as needed
    db_connector.upload_to_db(cleaned_date_events_data, date_times_table_name)

if __name__ == "__main__":
    main()

