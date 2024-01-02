import tabula
import pandas as pd
import requests
import boto3
from io import BytesIO
from database import Database
from database_connector import DatabaseConnector

class DataExtractor:
    def __init__(self):
        # Initialize the Database class
        self.db_manager = Database()

    def read_rds_table(self, db_connector, table_name):
        """
        Extract data from the specified table in the RDS database using the provided DatabaseConnector instance.
        Returns a pandas DataFrame.
        """
        # Connect to the database using the provided DatabaseConnector instance
        connection = db_connector.connect_to_database()

        if not connection:
            print("Error: Unable to connect to the database.")
            return None

        try:
            # Use pandas to read data from the specified table
            query = f"SELECT * FROM {table_name};"
            data = pd.read_sql_query(query, connection)
            print(f"Data extracted from table {table_name}.")
            return data
        except Exception as e:
            print(f"Error: Unable to extract data from table {table_name}. {e}")
            return None
        finally:
            # Close the database connection
            db_connector.close_connection(connection)

    def extract_user_data(self):
        """
        Extract user data from the RDS database and return a pandas DataFrame.
        """
        # Get the SQLAlchemy engine
        engine = self.db_manager.init_db_engine()

        # List tables in the database and identify the table containing user data
        user_data_table = self.list_user_data_table(engine)

        if user_data_table:
            # Create an instance of DatabaseConnector
            db_connector = DatabaseConnector()

            # Use the read_rds_table method to extract user data
            user_data = self.read_rds_table(db_connector, user_data_table)
            return user_data
        else:
            print("Error: No user data table found.")
            return None

    def list_user_data_table(self, engine):
        """
        List tables in the database and identify the table containing user data.
        Returns the name of the user data table.
        """
        # List tables in the database
        tables = self.db_manager.list_db_tables(engine)

        # Example: Assume the table containing user data has 'user' in its name
        user_data_table = next((table for table in tables if 'user' in table.lower()), None)

        if user_data_table:
            print(f"User data table found: {user_data_table}")
        else:
            print("Error: No user data table found.")

        return user_data_table
     
    def retrieve_pdf_data(self, pdf_link):
        """
        Retrieve data from a PDF document specified by the given link.

        Parameters:
        - pdf_link (str): https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf

        Returns:
        - pd.DataFrame: A pandas DataFrame containing the extracted data.
        """
        try:
            # Use tabula to extract tables from all pages of the PDF
            tables = tabula.read_pdf(pdf_link, pages="all", multiple_tables=True)

            # Concatenate the extracted tables into a single DataFrame
            extracted_data = pd.concat(tables, ignore_index=True)

            return extracted_data
        except Exception as e:
            print(f"Error extracting data from PDF: {e}")
            return None
        
    def list_number_of_stores(self, number_of_stores_endpoint, headers):
        """
        Retrieve the number of stores from the API.

        Parameters:
        - number_of_stores_endpoint (str): The endpoint URL for getting the number of stores.
        - headers (dict): The headers containing the API key.

        Returns:
        - int: The number of stores.
        """
        try:
             # Define the endpoint URL for getting the number of stores
            number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

            # Make a GET request to the API endpoint
            response = requests.get(number_of_stores_endpoint, headers=headers)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the JSON response and extract the number of stores
                data = response.json()
                number_of_stores = data.get('number_of_stores', 0)
                print(f"Number of stores: {number_of_stores}")
                return number_of_stores
            else:
                print(f"Error: Unable to retrieve the number of stores. Status code: {response.status_code}")
                return 0
        except Exception as e:
            print(f"Error: {e}")
            return 0
        
    def retrieve_store_details(self, store_number, headers):
        """
        Retrieve details for a specific store from the API.

        Parameters:
        - store_number (int): The store number.
        - headers (dict): The headers containing the API key.

        Returns:
        - dict: Details for the specified store.
        """
        try:
            # Construct the URL for the specific store using the provided endpoint
            store_details_endpoint = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"

            # Make a GET request to the API endpoint
            response = requests.get(store_details_endpoint, headers=headers)

            # Check if the request was successful (status code 200)
            if response.status_code == 200:
                # Parse the JSON response and extract store details
                store_details = response.json()
                print(f"Details for Store {store_number}: {store_details}")
                return store_details
            else:
                print(f"Error: Unable to retrieve details for Store {store_number}. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error: {e}")
            return None

    def retrieve_stores_data(self, store_details_endpoint, store_numbers, headers):
        """
        Retrieve store details from the API for the given store numbers.

        Parameters:
        - store_details_endpoint (str): The endpoint URL for retrieving store details.
        - store_numbers (list): List of store numbers to retrieve details for.
        - headers (dict): The headers containing the API key.

        Returns:
        - pd.DataFrame: A pandas DataFrame containing the extracted store details.
        """
        store_data_list = []

        try:
            # Loop through each store number and retrieve details
            for store_number in store_numbers:
                # Construct the URL for the specific store
                specific_store_endpoint = f"{store_details_endpoint}{store_number}"

                # Make a GET request to the API endpoint
                response = requests.get(specific_store_endpoint, headers=headers)

                # Check if the request was successful (status code 200)
                if response.status_code == 200:
                    # Parse the JSON response and append to the list
                    store_details = response.json()
                    store_data_list.append(store_details)
                else:
                    print(f"Error retrieving details for store {store_number}. Status code: {response.status_code}")

            # Convert the list of dictionaries to a pandas DataFrame
            store_data = pd.DataFrame(store_data_list)

            return store_data

        except Exception as e:
            print(f"Error: {e}")
            return None

    def extract_from_s3(self, s3_address):
        """
        Extract data from the specified S3 address.

        Parameters:
        - s3_address (str): The S3 address in the format "s3://bucket_name/object_key".

        Returns:
        - pd.DataFrame: A pandas DataFrame containing the extracted data.
        """
        try:
            # Parse the S3 address to get bucket name and object key
            bucket_name, object_key = self.parse_s3_address(s3_address)

            # Connect to S3 and retrieve the CSV file
            s3 = boto3.client('s3')
            response = s3.get_object(Bucket=bucket_name, Key=object_key)
            csv_data = pd.read_csv(BytesIO(response['Body'].read()))

            return csv_data
        except Exception as e:
            print(f"Error extracting data from S3: {e}")
            return None

    def parse_s3_address(self, s3_address):
        """
        Parse S3 address to get bucket name and object key.

        Parameters:
        - s3_address (str): The S3 address in the format "s3://bucket_name/object_key".

        Returns:
        - tuple: A tuple containing (bucket_name, object_key).
        """
        try:
            # Remove "s3://" prefix and split into bucket and object key
            parts = s3_address.replace("s3://", "").split("/")
            bucket_name = parts[0]
            object_key = "/".join(parts[1:])
            return bucket_name, object_key
        except Exception as e:
            print(f"Error parsing S3 address: {e}")
            return None, None

    def list_orders_table(self, engine):
        """
        List tables in the database and identify the table containing order information.
        Returns the name of the orders table.
        """
        # List tables in the database
        tables = self.db_manager.list_db_tables(engine)

        # Example: Assume the table containing order data has 'orders' in its name
        orders_table = next((table for table in tables if 'orders' in table.lower()), None)

        if orders_table:
            print(f"Orders table found: {orders_table}")
        else:
            print("Error: No orders table found.")

        return orders_table
