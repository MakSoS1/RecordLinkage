import pandas as pd
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from clickhouse_sqlalchemy import make_session, get_declarative_base, types, engines

def clean_and_prepare_data_from_clickhouse(table_name, output_clean_file, columns_mapping, create_full_name=False):
    """
    Function to clean, normalize, and prepare data from a ClickHouse table using pandas.
    Skips problematic rows when reading data.

    Args:
    - table_name (str): Name of the ClickHouse table to read data from.
    - output_clean_file (str): Path to the output CSV file after cleaning.
    - columns_mapping (dict): Dictionary mapping old columns to new columns.
    - create_full_name (bool): Whether to create 'full_name_norm' by concatenating first, middle, and last names.
    """
    # Set up ClickHouse connection
    uri = 'clickhouse+native://default:@clickhouse:9000/default'
    engine = None

    while engine is None:
        try:
            engine = create_engine(uri)
            connection = engine.connect()
            connection.close()
            print("Connected to ClickHouse.")
        except OperationalError:
            print("ClickHouse is not ready yet. Retrying in 5 seconds...")
            time.sleep(5)
    print("ClickHouse connection established.")

    # Step 1: Read data from ClickHouse table using pandas in chunks
    chunk_size = 10000  # Chunk size
    chunks = []
    total_rows = 0

    query = f"SELECT * FROM {table_name}"

    try:
        for chunk in pd.read_sql_query(query, engine, chunksize=chunk_size):
            chunks.append(chunk)
            total_rows += len(chunk)
        df_temp = pd.concat(chunks, ignore_index=True)
        print(f"Data from table '{table_name}' successfully fetched. Total rows: {total_rows}")
    except Exception as e:
        print(f"Error reading data from table '{table_name}': {e}")
        return

    # Convert all columns to string
    df_temp = df_temp.astype(str)

    # Step 2: Handle missing values
    for col in df_temp.columns:
        if col in columns_mapping.keys():
            df_temp[col] = df_temp[col].fillna('')
    print("Missing values handled.")

    # Step 3: Normalize data and create new columns with suffix _norm
    def normalize_column(col):
        return col.str.lower().str.strip().str.replace(r'\s+', ' ', regex=True)

    for old_col, new_col in columns_mapping.items():
        if old_col in df_temp.columns:
            df_temp[new_col] = normalize_column(df_temp[old_col])
    print("Data normalization completed.")

    # For the second dataset, create 'full_name_norm' by concatenating 'first_name_norm', 'middle_name_norm', 'last_name_norm'
    if create_full_name:
        df_temp['full_name_norm'] = (
            df_temp['first_name_norm'] + ' ' + df_temp['middle_name_norm'] + ' ' + df_temp['last_name_norm']
        ).str.strip()
        print("'full_name_norm' column created by concatenating first, middle, and last names.")

    # Step 4: Drop old columns
    df_temp = df_temp.drop(columns=list(columns_mapping.keys()))
    print(f"Old columns dropped: {list(columns_mapping.keys())}")

    # Step 5: Drop duplicate rows
    df_temp = df_temp.drop_duplicates()
    print("Duplicate rows dropped.")

    # Step 6: Save the cleaned file to the App folder in Docker
    try:
        df_temp.to_csv(output_clean_file, index=False, encoding='utf-8')
        print(f"Cleaned file saved as '{output_clean_file}'.")
    except Exception as e:
        print(f"Error saving cleaned file '{output_clean_file}': {e}")
        # If error occurs, try saving in chunks
        print("Attempting to save file in chunks...")
        try:
            chunk_size = 50000  # Reduced chunk size for saving
            for i, chunk_start in enumerate(range(0, len(df_temp), chunk_size)):
                chunk = df_temp.iloc[chunk_start:chunk_start + chunk_size]
                mode = 'w' if i == 0 else 'a'
                header = i == 0
                chunk.to_csv(output_clean_file, mode=mode, header=header, index=False, encoding='utf-8')
            print(f"File successfully saved in chunks as '{output_clean_file}'.")
        except Exception as e:
            print(f"Failed to save file even in chunks: {e}")

    print(f"Processing of table '{table_name}' completed.")
    print(f"Total rows processed: {total_rows}")



# Define the datasets with their respective configurations
datasets = [
    {
        "table_name": "table_dataset1",
        "output_clean_file": "/app/main1_clean.csv",  # Save to App folder
        "columns_mapping": {
            'full_name': 'full_name_norm',
            'email': 'email_norm',
            'address': 'address_norm',
            'birthdate': 'birthdate_norm',
            'phone': 'phone_norm'
        }
    },
    {
        "table_name": "table_dataset2",
        "output_clean_file": "/app/main2_clean.csv",  # Save to App folder
        "columns_mapping": {
            'first_name': 'first_name_norm',
            'middle_name': 'middle_name_norm',
            'last_name': 'last_name_norm',
            'birthdate': 'birthdate_norm',
            'phone': 'phone_norm',
            'address': 'address_norm'
        },
        "create_full_name": True  # Indicates that 'full_name_norm' should be created
    },
    {
        "table_name": "table_dataset3",
        "output_clean_file": "/app/main3_clean.csv",  # Save to App folder
        "columns_mapping": {
            'name': 'full_name_norm',
            'email': 'email_norm',
            'birthdate': 'birthdate_norm',
            'sex': 'sex_norm'
        }
    }
]

print("Processing datasets...")
# Process each dataset
for dataset in datasets:
    create_full_name = dataset.get('create_full_name', False)
    clean_and_prepare_data_from_clickhouse(
        dataset['table_name'],
        dataset['output_clean_file'],
        dataset['columns_mapping'],
        create_full_name
    )

