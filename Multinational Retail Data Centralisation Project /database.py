from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError
from database_connector import DatabaseConnector

class Database:
    def __init__(self):
        self.connector = DatabaseConnector()

    def init_db_engine(self):
        """
        Initialize and return an SQLAlchemy database engine.
        """
        db_creds = self.connector.read_db_creds()

        # Construct database URL for SQLAlchemy
        db_url = f"postgresql+psycopg2://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}"

        try:
            engine = create_engine(db_url)
            print("Database engine initialized.")
            return engine
        except SQLAlchemyError as e:
            print(f"Error: Unable to initialize the database engine. {e}")
            return None

    def list_db_tables(self, engine):
        """
        List all tables in the database.
        """
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        print("Tables in the database:")
        for table in tables:
            print(table)
        return tables


