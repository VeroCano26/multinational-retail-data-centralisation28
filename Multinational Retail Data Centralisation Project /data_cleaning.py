import pandas as pd
import numpy as np

class DataCleaning:
    def __init__(self):
        # You can add any initialization steps here

     def clean_csv_data(self, csv_data):
        # Method to clean data extracted from CSV files
        pass

    def clean_api_data(self, api_data):
        # Method to clean data extracted from an API
        pass

    def clean_s3_data(self, s3_data):
        # Method to clean data extracted from an S3 bucket
        pass

    def clean_user_data(self, user_data):
        """
        Clean the user data and return the cleaned DataFrame.
        """
        # Make a copy of the original DataFrame to avoid modifying the original data
        cleaned_data = user_data.copy()

        # Handle NULL values
        cleaned_data.dropna(inplace=True)

        # Handle errors with dates (assuming a column named 'date' for example)
        cleaned_data['date'] = pd.to_datetime(cleaned_data['date'], errors='coerce')

        # Handle incorrectly typed values and rows filled with the wrong information
        # Add specific cleaning steps based on your data and requirements

        # Print information about the cleaning process (you can remove this in the final version)
        print("User data cleaning completed.")

        return cleaned_data

    def clean_card_data(self, card_data):
        """
        Clean the data extracted from the PDF document containing card details.

        Parameters:
        - card_data (pd.DataFrame): A pandas DataFrame containing the extracted card data.

        Returns:
        - pd.DataFrame: A cleaned pandas DataFrame.
        """
        try:
            # Drop rows with NULL values
            card_data = card_data.dropna()

            # Remove erroneous values or errors with formatting
            # Add additional cleaning steps as needed

            # Example: Remove rows with invalid card numbers (assuming card numbers are in a column named 'card_number')
            card_data = card_data[card_data['card_number'].apply(lambda x: self.is_valid_card_number(x))]

            # Reset index after cleaning
            card_data = card_data.reset_index(drop=True)

             # Upload cleaned card data to the database
            self.upload_card_data_to_db(card_data)

            return card_data
        except Exception as e:
            print(f"Error cleaning card data: {e}")
            return None

    def upload_card_data_to_db(self, card_data):
        """
        Upload cleaned card data to the database.

        Parameters:
        - card_data (pd.DataFrame): A pandas DataFrame containing the cleaned card data.
        """
        try:
            # Create an instance of DatabaseConnector
            db_connector = DatabaseConnector()

            # Upload the card data to the database
            db_connector.upload_to_db(card_data, table_name="dim_card_details")

            print("Card data uploaded to the database.")
        except Exception as e:
            print(f"Error uploading card data to the database: {e}")

    def is_valid_card_number(self, card_number):
        """
        Check if a card number is valid.

        Parameters:
        - card_number (str): The card number.

        Returns:
        - bool: True if the card number is valid, False otherwise.
        """
        # Implement your validation logic here
        # Example: Check if the card number has the correct length
        return len(card_number) == 16

    def clean_store_data(self, store_data):
        """
        Clean the data extracted from the API containing store details.

        Parameters:
        - store_data (pd.DataFrame): A pandas DataFrame containing the extracted store data.

        Returns:
        - pd.DataFrame: A cleaned pandas DataFrame.
        """
        try:
            # Drop rows with NULL values
            store_data = store_data.dropna()

            # Add additional cleaning steps as needed
            # Example: Remove rows with invalid store names (assuming store names are in a column named 'store_name')
            store_data = store_data[store_data['store_name'].apply(lambda x: self.is_valid_store_name(x))]

            # Reset index after cleaning
            store_data = store_data.reset_index(drop=True)

            # Print information about the cleaning process (you can remove this in the final version)
            print("Store data cleaning completed.")

            return store_data
        except Exception as e:
            print(f"Error cleaning store data: {e}")
            return None
        
    def upload_store_data_to_db(self, store_data):
        """
        Upload cleaned store data to the database.

        Parameters:
        - store_data (pd.DataFrame): A pandas DataFrame containing the cleaned store data.
        """
        try:
            # Create an instance of DatabaseConnector
            db_connector = DatabaseConnector()

            # Upload the store data to the database
            db_connector.upload_to_db(store_data, table_name="dim_store_details")

            print("Store data uploaded to the database.")
        except Exception as e:
            print(f"Error uploading store data to the database: {e}")

    def is_valid_store_name(self, store_name):
        """
        Check if a store name is valid.

        Parameters:
        - store_name (str): The store name.

        Returns:
        - bool: True if the store name is valid, False otherwise.
        """
        # Implement your validation logic here
        # Example: Check if the store name has the correct length
        return len(store_name) > 0  # Adjust this condition based on your requirements
    
    def convert_product_weights(self, products_df):
        """
        Convert weights in the product DataFrame to a common unit (kg).

        Parameters:
        - products_df (pd.DataFrame): The DataFrame containing product information.

        Returns:
        - pd.DataFrame: The cleaned DataFrame with weights represented in kg.
        """
        # Make a copy of the DataFrame to avoid modifying the original
        cleaned_df = products_df.copy()

        # Apply cleaning and conversion operations on the weight column
        cleaned_df['weight'] = cleaned_df['weight'].apply(self.clean_and_convert_weight)

        return cleaned_df

    def clean_and_convert_weight(self, weight_str):
        """
        Clean and convert a single weight string to kg.

        Parameters:
        - weight_str (str): The string representation of the weight.

        Returns:
        - float: The weight in kg.
        """
        try:
            # Add your cleaning and conversion logic here
            # Example: Remove excess characters and represent the weight as a float
            cleaned_weight_str = weight_str.replace('g', '').replace('ml', '').strip()
            weight_value = float(cleaned_weight_str)

            # If the original unit was ml, assume a 1:1 ratio to g and convert to kg
            if 'ml' in weight_str:
                weight_value /= 1000

            return weight_value
        except Exception as e:
            # Handle exceptions, e.g., if the conversion fails
            print(f"Error converting weight '{weight_str}': {e}")
            return np.nan  # Return NaN for cases where conversion fails

    def clean_products_data(self, products_data):
        """
        Clean the product data DataFrame.

        Parameters:
        - products_data (pd.DataFrame): A pandas DataFrame containing product data.

        Returns:
        - pd.DataFrame: A cleaned pandas DataFrame.
        """
        try:
            # Make a copy of the original DataFrame to avoid modifying the original data
            cleaned_data = products_data.copy()

            # Drop rows with NULL values
            cleaned_data.dropna(inplace=True)

            # Remove erroneous values or errors with formatting
            # Add additional cleaning steps based on your data and requirements
            # Example: Remove rows with negative prices
            cleaned_data = cleaned_data[cleaned_data['price'] >= 0]

            # Example: Convert categorical columns to appropriate data types
            cleaned_data['category'] = cleaned_data['category'].astype('category')

            # Example: Remove duplicates based on a subset of columns
            cleaned_data.drop_duplicates(subset=['product_id', 'name'], keep='first', inplace=True)

            # Print information about the cleaning process (you can remove this in the final version)
            print("Product data cleaning completed.")

            return cleaned_data

        except Exception as e:
            print(f"Error cleaning product data: {e}")
            return None
    
    def upload_product_data_to_db(self, product_data):
        """
        Upload cleaned product data to the database.

        Parameters:
        - product_data (pd.DataFrame): A pandas DataFrame containing the cleaned product data.
        """
        try:
            # Create an instance of DatabaseConnector
            db_connector = DatabaseConnector()

            # Upload the product data to the database
            db_connector.upload_to_db(product_data, table_name="dim_products")

            print("Product data uploaded to the database.")
        except Exception as e:
            print(f"Error uploading product data to the database: {e}")

    def clean_orders_data(self, orders_data):
        """
        Clean the orders data and return the cleaned DataFrame.

        Parameters:
        - orders_data (pd.DataFrame): A pandas DataFrame containing the orders data.

        Returns:
        - pd.DataFrame: A cleaned pandas DataFrame for the orders data.
        """
        # Make a copy of the original DataFrame to avoid modifying the original data
        cleaned_orders_data = orders_data.copy()

        try:
            # Remove specified columns (first_name, last_name, 1)
            columns_to_remove = ['first_name', 'last_name', 1]
            cleaned_orders_data = cleaned_orders_data.drop(columns=columns_to_remove, errors='ignore')

            # Additional cleaning steps can be added as needed

            # Print information about the cleaning process (you can remove this in the final version)
            print("Orders data cleaning completed.")

            return cleaned_orders_data
        except Exception as e:
            print(f"Error cleaning orders data: {e}")
            return None

