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
@st.fragment
def upload(df: pl.DataFrame, monthly: bool=False):
    """Uploads a sales report to the cloud."""
    if st.button('Upload'):
        st.session_state['upload_status'] = 'Uploading file...'
        st.write(st.session_state['upload_status'])
        connections.upload_data(file, df, monthly=monthly)
        if monthly:
            pass
        else:
            st.warning(body='Refresh page before attempting to view the sales report for this file.', icon="🚨")


tab_1, tab_2, tab_3 = st.tabs(['Upload Weekly Data', 'View Weekly Reports', 'Monthly Reports'])
with tab_1:
    file = st.file_uploader('Upload weekly sales csv','csv')
    if file is not None:
        sales = sales_files.CSA_Weekly_Sales(file)
        st.subheader("Data Preview")
        st.dataframe(sales.df.head())
        st.info(body='Data is ready to be uploaded to the database. Click below to proceed with upload.', icon="✅")
        upload(sales.df)

with tab_2:
    Reports.load_rpt_dates()
    rpt_dt = st.selectbox('Select Reporting Date', options=st.session_state['rpt_dates'],index=None)
    if rpt_dt is not None:
        results = Reports.load_report(rpt_dt)
        st.dataframe(results)
        marketshare = Reports.calc_marketshare(results)
        Reports.plot_marketshare(marketshare,rpt_dt)

with tab_3:
    file = st.file_uploader('Upload monthly sales csv','csv')
    if file is not None:
        sales = sales_files.CSA_Weekly_Sales(file)
        monthly_sales = sales_files.CSA_Monthly_Sales(sales.df,file)
        st.subheader("Data Preview")
        st.dataframe(monthly_sales.df.head())
        st.info(body='Data is ready to be uploaded to the database. Click below to proceed with upload.', icon="✅")
        upload(monthly_sales.df, monthly=True)
        top400 = sales_files.CSA_Monthly_Sales.calc_top_sales(monthly_sales.df)
        top400_w_dollars = sales_files.CSA_Monthly_Sales.calc_top_sales(monthly_sales.df, dollars=True)

        unit_ms = Reports.calc_marketshare(monthly_sales.df.rename({'totalSoldInSpan': 'sold_qty'}))
        dollar_ms = Reports.calc_marketshare(
            top400_w_dollars.rename({'dollars_sold':'totalDollars'})
        )

        rpt_dt = monthly_sales.df.select(pl.min('reportStartTime').dt.strftime("%B %Y")).item()

        st.subheader("Top 400 Units")
        st.dataframe(top400)
        sales_files.CSA_Monthly_Sales.plot_marketshare(
            df=unit_ms, rpt_dt = rpt_dt
        )
        st.subheader("Top 400 Dollars")
        st.dataframe(top400_w_dollars)
        sales_files.CSA_Monthly_Sales.plot_marketshare(
            df=dollar_ms, rpt_dt = rpt_dt, dollars=True
        )