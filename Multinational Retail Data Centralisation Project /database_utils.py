import yaml
import psycopg2
from sqlalchemy import create_engine

class DatabaseConnector:
    def __init__(self):
        # Load database credentials from the db_creds.yaml file
        self.db_creds = self.read_db_creds()

    def read_db_creds(self):
        """
        Read the database credentials from the db_creds.yaml file.
        """
        try:
            with open("db_creds.yaml", "r") as file:
                creds = yaml.safe_load(file)
            return creds
        except Exception as e:
            print(f"Error: Unable to read database credentials. {e}")
            return None

    def connect_to_database(self):
        """
        Establish a connection to the database.
        """
        try:
            connection = psycopg2.connect(
                database=self.db_creds["RDS_DATABASE"],
                user=self.db_creds["RDS_USER"],
                password=self.db_creds["RDS_PASSWORD"],
                host=self.db_creds["RDS_HOST"],
                port=self.db_creds["RDS_PORT"]
            )
            print("Connected to the database.")
            return connection
        except psycopg2.Error as e:
            print(f"Error: Unable to connect to the database. {e}")
            return None

    def execute_query(self, connection, query):
        """
        Execute a SQL query on the connected database.
        """
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            print("Query executed successfully.")
        except psycopg2.Error as e:
            print(f"Error: Unable to execute the query. {e}")

    def close_connection(self, connection):
        """
        Close the connection to the database.
        """
        if connection:
            connection.close()
            print("Connection closed.")

    def upload_to_db(self, dataframe, table_name):
        """
        Upload a Pandas DataFrame to the specified table in the database.
        """
        try:
            # Connect to the database
            connection = self.connect_to_database()

            if not connection:
                print("Error: Unable to connect to the database.")
                return

            # Use SQLAlchemy to upload the DataFrame to the database
            engine = create_engine(
                f"postgresql://{self.db_creds['RDS_USER']}:{self.db_creds['RDS_PASSWORD']}@{self.db_creds['RDS_HOST']}:{self.db_creds['RDS_PORT']}/{self.db_creds['RDS_DATABASE']}"
            )
            dataframe.to_sql(table_name, con=engine, if_exists="replace", index=False)

            print(f"Data uploaded to table {table_name}.")
        except Exception as e:
            print(f"Error: Unable to upload data to the database. {e}")
        finally:
            # Close the database connection
            self.close_connection(connection)





