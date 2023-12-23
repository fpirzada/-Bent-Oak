import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
import os

load_dotenv()


class CSVToPostgresSpark:
    def __init__(self, csv_path, db_username, db_password, db_host, db_port, db_name, schema_name, table_name):
        self.csv_path = csv_path
        self.db_username = db_username
        self.db_password = db_password
        self.db_host = db_host
        self.db_port = db_port
        self.db_name = db_name
        self.schema_name = schema_name
        self.table_name = table_name
        
        spark_memory_config = "8g"  # Adjust based on your available memory
        driver_jar_path ='psql/postgresql-42.7.1.jar'
        # Initialize a Spark session
        self.spark = SparkSession.builder \
            .appName("CSVToPostgres") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("spark.driver.memory", spark_memory_config) \
            .config("spark.executor.memory", spark_memory_config) \
            .config("spark.executor.instances", "4")  \
            .config("spark.executor.cores", "4")  \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.shuffle.partitions", "8") \
            .config("spark.jars",driver_jar_path) \
            .config('spark.driver.extraClassPath', driver_jar_path) \
            .config("spark.driver.extraClassPath", driver_jar_path) \
            .getOrCreate()
        
        # Set Spark log level to INFO
        self.spark.sparkContext.setLogLevel("INFO")


    def read_csv(self):
        """Read data from CSV file."""
        return self.spark.read.option("delimiter", "|").csv(self.csv_path, header=True, inferSchema=True)

    def create_database(self):
        """Create the PostgreSQL database if it does not exist."""
        try:
            # Connect to the default 'postgres' database
            conn = psycopg2.connect(
                host=self.db_host,
                port=self.db_port,
                user=self.db_username,
                password=self.db_password,
                dbname='postgres'
            )
            conn.autocommit = True
            cursor = conn.cursor()

            # Check if the database exists
            cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.db_name}'")
            exists = cursor.fetchone()

            if not exists:
                # Create the database
                cursor.execute(f"CREATE DATABASE {self.db_name}")

            logging.info(f"Database '{self.db_name}' created or already exists.")
        except Exception as e:
            logging.error(f"Error creating database '{self.db_name}': {str(e)}", exc_info=True)
        finally:
            cursor.close()
            conn.close()
            
    def create_or_replace_table(self, df):
        """Create or replace PostgreSQL table based on DataFrame columns."""
        try:
            # Check if the database exists, create it if not
            self.create_database()
            logging.info(f"create_database")
            print(df)
            # Check if the table exists
            if self.table_exists():
                # Table exists, so overwrite it
                df.write.format("jdbc").option("url", f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}") \
                    .option("dbtable", f"{self.schema_name}.{self.table_name}") \
                    .option("user", self.db_username) \
                    .option("password", self.db_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("overwrite") \
                    .save()
            else:
                # Table doesn't exist, so append to it
                df.write.format("jdbc").option("url", f"jdbc:postgresql://{self.db_host}:{self.db_port}/{self.db_name}") \
                    .option("dbtable", f"{self.schema_name}.{self.table_name}") \
                    .option("user", self.db_username) \
                    .option("password", self.db_password) \
                    .option("driver", "org.postgresql.Driver") \
                    .mode("append") \
                    .save()

            logging.info(f"Table '{self.table_name}' created or replaced.")
        except Exception as e:
            print(f"Error: {str(e)}")
            # Optionally, print the stack trace
            import traceback
            traceback.print_exc()
            logging.error(f"Error creating or replacing table '{self.table_name}': {str(e)}", exc_info=True)

    def table_exists(self):
        """Check if the table already exists."""
        return self.spark.sql(f"SHOW TABLES LIKE '{self.schema_name}.{self.table_name}'").count() > 0

    def import_data(self, df):
        """Import data into the PostgreSQL table."""
        try:
            # Check if the database and table exist, create them if not
            self.create_or_replace_table(df)

            # Assuming `your_dataframe` is the DataFrame you want to write to the database
            df.write \
                .format("jdbc") \
                .option("url", "jdbc:postgresql://your-database-host:5432/your-database-name") \
                .option("dbtable", "your_table_name") \
                .option("user", "your_username") \
                .option("password", "your_password") \
                .option("batchsize", 1000) \
                .mode("overwrite") \
                .save()

            logging.info(f"Data imported into '{self.table_name}' table.")
        except Exception as e:
            logging.error(f"Error importing data into '{self.table_name}': {str(e)}", exc_info=True)



class CSVFolderToPostgresSpark:
    def __init__(self, folder_path
                 ):
        self.folder_path = folder_path

    def process_folder(self):
        for file_name in os.listdir(self.folder_path):
            print(file_name)
            if file_name.endswith(".csv"):
                csv_path = os.path.join(self.folder_path, file_name)

                # Extract table name from the file name (without extension)
                table_name = os.path.splitext(file_name)[0]

                # Initialize the CSVToPostgresSpark class for each CSV file
                csv_to_postgres_spark = CSVToPostgresSpark(
                    csv_path,
                    os.getenv("DB_USERNAME"),
                    os.getenv("DB_PASSWORD"),
                    os.getenv("DB_HOST"),
                    os.getenv("DB_PORT"),
                    os.getenv("DB_NAME"),
                    os.getenv("SCHEMA_NAME"),
                    table_name
                )

                # Read CSV
                data_frame = csv_to_postgres_spark.read_csv()

                # Create database, table, and import data using the PySpark class
                csv_to_postgres_spark.import_data(data_frame)

if __name__ == "__main__":
    # Set up logging configuration
    logging.basicConfig(filename='csv_to_postgres.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    try:
        folder_path = 'ReadCsv/'

        csv_folder_to_postgres_spark = CSVFolderToPostgresSpark(
            folder_path
        )

        # Process all CSV files in the folder
        csv_folder_to_postgres_spark.process_folder()
    except Exception as e:
        print(e)