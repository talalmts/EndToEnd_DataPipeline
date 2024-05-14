import streamlit as st
import duckdb

connection_duckdb = duckdb.connect("airflow_dag/dags/dbt/caspar_challenge/caspar_datawarehouse.duckdb")
df = connection_duckdb.sql("select * from caspar_fact.patient_with_most_generated_minutes").df()

logo_path = "/airflow_dag/include/resource/caspar_icon.png"
# Set page title and favicon
st.set_page_config(page_title="Caspar Insights", page_icon=logo_path)

# Add content to the main area
st.title("Welcome to Caspar Insights!")

st.markdown("<h3 style='text-align: left; color: black;'>Patient with most generated minutes</h3>", unsafe_allow_html=True)
st.dataframe(df.set_index(df.columns[0]))

connection_duckdb.close()