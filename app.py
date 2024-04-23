import streamlit as st
from pipeline_02_usando_duckdb_banco import pipeline
 
st.write("Processador de Arquivos:")
if st.button('Processar'):
    with st.spinner('Processando...'):
        logs = pipeline()
        for log in logs:
            st.write(log)