import streamlit as st
import polars as pl
import connections
import sales_files
from reports import Reports


if 'upload_status' not in st.session_state:
    st.session_state['upload_status'] = ''
if 'rpt_dates' not in st.session_state:
    st.session_state['rpt_dates'] = ''

st.header("Prana Bestseller Weekly Sales Application")

tab_1, tab_2 = st.tabs(['Upload Data', 'View Reports'])
with tab_1:
    @st.fragment
    def upload():
        """Uploads a sales report to the cloud."""
        if st.button('Upload'):
            st.session_state['upload_status'] = 'Uploading file...'
            st.write(st.session_state['upload_status'])
            connections.upload_data(file,sales.df)

    file = st.file_uploader('Upload weekly sales csv','csv')
    if file is not None:
        sales = sales_files.CSA_Weekly_Sales(file)
        st.subheader("Data Preview")
        st.dataframe(sales.df.head())

        upload()

with tab_2:
    Reports.load_rpt_dates()
    rpt_dt = st.selectbox('Select Reporting Date', options=st.session_state['rpt_dates'],index=None)
    if rpt_dt is not None:
        results = Reports.load_report(rpt_dt)
        st.dataframe(results)
        marketshare = Reports.calc_marketshare(results)
        Reports.plot_marketshare(marketshare,rpt_dt)







