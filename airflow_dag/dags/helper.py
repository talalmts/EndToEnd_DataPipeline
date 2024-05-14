import pandas as pd
import duckdb
import logging

logging.basicConfig(format='%(asctime)s %(levelname)-8s [%(filename)s:%(lineno)d]:: %(message)s',
    datefmt='%Y-%m-%d:%H:%M:%S',
    level=logging.DEBUG)

def extract_data() -> tuple:
    """
    extract the data from the csv, typecast the input columns 
    and rename the columns name to a unified format
    
    Returns:
        tuple: return a tuple having dataframe for each csv import
    """
    logging.info("Initiating extracting data from csv")
    
    # load csv into dataframe and transform the input to required 
    df_exercise = pd.read_csv('data/exercises.csv', index_col=False)
    df_exercise = to_lowercase_column_name(dataframe=df_exercise)
    df_exercise = to_datetime(dataframe=df_exercise, datetime_column_list=['completed_at', 'updated_at'])
    
    df_step = pd.read_csv('data/steps.csv', index_col=False)
    df_step = to_lowercase_column_name(dataframe=df_step)
    df_step = to_datetime(dataframe=df_step, datetime_column_list=['submission_time', 'updated_at'])

    df_patient = pd.read_csv('data/patients.csv', index_col=0)
    df_patient = to_lowercase_column_name(dataframe=df_patient)
    
    logging.info("Extracting data complete")
    
    # return the dataframe to the load task
    return (df_exercise, df_step, df_patient)

def load_data(dataframes) -> None:
    """
    Create table and insert values into the Duckdb instance

    Args:
        dataframes (tuple): pandas dataframe of all the input data
    """
    logging.info("Loading data into local duckdb instance")
    # unpack the tuple
    df_exercise, df_step, df_patient = dataframes
    
    # establish connection to the local duckdb instances
    connection_duckdb = duckdb.connect('dags/dbt/caspar_challenge/caspar_datawarehouse.duckdb')
    # create landing schema
    connection_duckdb.execute("CREATE SCHEMA IF NOT EXISTS caspar_landing")
    
    # create table from dataframe in duckdb
    create_table_from_dataframe(duckdb_con=connection_duckdb, table_name='caspar_landing.patient', dataframe=df_patient)
    create_table_from_dataframe(duckdb_con=connection_duckdb, table_name='caspar_landing.exercise', dataframe=df_exercise)
    create_table_from_dataframe(duckdb_con=connection_duckdb, table_name='caspar_landing.step', dataframe=df_step)
    
    # check patient table if it has record or not
    record = connection_duckdb.sql("select * from caspar_landing.patient").fetchone()
    if record:
        logging.info("Successfully loaded data into Duckdb: {}".format(record))
    else:
        logging.error("Failed to load data into Duckdb")
    
    # close the connection
    connection_duckdb.close()
    return None

def to_lowercase_column_name(dataframe: pd.DataFrame) -> pd.DataFrame:
    """
    Update all the column name to lower case 

    Args:
        dataframe (pd.DataFrame): _description_

    Returns:
        pd.DataFrame: return the latest dataframe
    """
    lowercase_df_column = [item.lower() for item in dataframe.columns.tolist()]
    dataframe.columns = lowercase_df_column
    return dataframe
    
def create_table_from_dataframe(duckdb_con: duckdb.DuckDBPyConnection , table_name: str, dataframe: pd.DataFrame):
    """
    Create table and insert values into the local Duckdb instance
    """
    duckdb_con.register(table_name, dataframe)
    # create a new table from the contents of a DataFrame
    duckdb_con.execute(f"""CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM dataframe""".format(table_name=table_name))

def to_datetime(dataframe: pd.DataFrame, datetime_column_list: list) -> pd.DataFrame:
    """
    Typecast to datetime datatype

    Args:
        dataframe (pd.DataFrame): dataframe to be updated
        datetime_column_list (list): list of column to typecast 

    Returns:
        pd.DataFrame: return the latest dataframe
    """
    for column_name in datetime_column_list:
        dataframe[column_name] = pd.to_datetime(dataframe[column_name], utc=True)
    return dataframe