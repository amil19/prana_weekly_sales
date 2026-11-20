from sqlalchemy import create_engine
import sqlalchemy
import streamlit as st
import sales_files
from typing import TextIO
import polars as pl

engine = create_engine(st.secrets['URI'])

def push_data_to_db(df: pl.DataFrame,db_engine: sqlalchemy.Engine, table_name: str='csa_data'):
    """Writes a dataframe to the cloud. Appends data.

    Args:
        df (pl.DataFrame): DataFrame to write to cloud.
        db_engine (sqlalchemy.Engine): Database engine.
        table_name (str): Name of database table to write to. Defaults to 'csa_data'.
    """

    try:
        df.write_database(table_name=table_name, connection=db_engine, if_table_exists='append')
        st.session_state['upload_status'] = 'File uploaded successfully!'
        st.success(st.session_state['upload_status'])
    except Exception as e:
        st.session_state['upload_status'] = f'Error during upload: {e}'
        st.error(f"Database error: {e}")

@st.fragment
def upload_data(file: TextIO, df: pl.DataFrame, monthly: bool=False):
    """Function that receives the UI command to upload."""
    if file and st.session_state['upload_status'] == 'Uploading file...':
        if monthly:
            table_name = 'csa_data_monthly'
        else:
            table_name = 'csa_data'
        push_data_to_db(df,engine, table_name)
    else:
        st.error("File not processed.")