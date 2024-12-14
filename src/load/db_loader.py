import psycopg2
from sqlalchemy import create_engine, text
import pandas as pd

def load_csv_to_rds(csv_file_path, db_config, table_name, primary_key_columns):
    """
    Function to load any DataFrame into a PostgreSQL table hosted on RDS, with a specified primary key.

    Args:
        df (pd.DataFrame): The DataFrame to load into the database.
        db_config (dict): Database configuration details with keys:
            - host: The RDS endpoint.
            - port: The port number for PostgreSQL.
            - database: The database name.
            - user: The database username.
            - password: The database password.
        table_name (str): The name of the target table in PostgreSQL.
        primary_key_column (str): The column name to be used as the primary key.

    Returns:
        None
    """
    try:
        # Load CSV data into a DataFrame
        df = pd.read_csv(csv_file_path)

        # Create a connection string for SQLAlchemy
        connection_string = (
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@"
            f"{db_config['host']}:{db_config['port']}/{db_config['database']}"
        )

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Load DataFrame to PostgreSQL (if the table exists, it will be replaced)
        df.to_sql(table_name, engine, if_exists='replace', index=False)

        # Ensure primary_key_columns is a list, even if it's a single column
        if isinstance(primary_key_columns, str):
            primary_key_columns = [primary_key_columns]

        # Create the primary key constraint on the specified column
        with engine.connect() as connection:
            # Create the primary key constraint with the appropriate columns
            primary_key_columns_str = ', '.join(primary_key_columns)
            connection.execute(text(f"""
                ALTER TABLE {table_name}
                ADD CONSTRAINT {table_name}_pk PRIMARY KEY ({primary_key_columns_str});
            """))

        print(f"Data successfully loaded into table '{table_name}' with primary key '{', '.join(primary_key_columns)}'.")

    except Exception as e:
        print(f"Error occurred while loading data to database: {e}")