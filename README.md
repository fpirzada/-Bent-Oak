# CSV and PySpark to PostgreSQL Importer and Transformation

This project includes two Python scripts that use PySpark to read CSV files, create or replace PostgreSQL databases and tables, and import data from CSV files into PostgreSQL tables. It supports both individual CSV files and entire folders containing multiple CSV files.

## Dependencies

* PySpark
* psycopg2
* python-dotenv

## Configuration

### Environment Variables

Ensure the following environment variables are set:

* `DB_USERNAME`: PostgreSQL database username.
* `DB_PASSWORD`: PostgreSQL database password.
* `DB_HOST`: PostgreSQL database host.
* `DB_PORT`: PostgreSQL database port.
* `DB_NAME`: Default PostgreSQL database name for connection.
* `SCHEMA_NAME`: PostgreSQL schema name for the tables.

## CSV to PostgreSQL Importer

### Usage

1. **Clone the repository:**
   `git clone https://github.com/your-username/your-repository.git   

   cd your-repository`
2. **Create a virtual environment and install dependencies:**
   `python -m venv venv
   source venv/bin/activate

   # On Windows, use `venv\Scripts\activate `

   pip install -r requirements.txt`
3. **Set up the environment variables:**
   Create a `.env` file in the project root with the following content:
   `DB_USERNAME=your_db_username `
   `DB_PASSWORD=your_db_password  `
   `DB_HOST=your_db_host   
   DB_PORT=your_db_port   
   DB_NAME=your_db_name   
   SCHEMA_NAME=your_schema_name`
4. **ReadCsv:**
   put your csv file in that folder
5. **Run the script :**

   python CsvToPsql.py

   python TransparkData.py

The script logs its activities in the `csv_to_postgres.log` file. Check this file for detailed information on the execution process.

## License

This project is licensed under the MIT License.
