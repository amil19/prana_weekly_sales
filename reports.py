import polars as pl
import streamlit as st
import connections

class Reports:
    top_400_query = '''
    with metrics as (
    select 
        "reportStartTime",
        avg("totalSoldInSpan") as m, 
        percentile_cont(0.5) WITHIN GROUP (ORDER BY "storeCount") as c
    from csa_data where 
    "reportStartTime" = '{rpt_dt}'
    group by "reportStartTime"
    ),
    sales as 
    (
    select 
        title,
        issue,
        publisher,
        "storeCount",sum("totalSoldInSpan") as sold_qty, 
        (sum("totalSoldInSpan")/"storeCount") as sold_per_store,
        ((select m from metrics) * (select c from metrics) + "storeCount" * (sum("totalSoldInSpan")/"storeCount")) / ((select m from metrics) + "storeCount") AS weighted_ranking
    from csa_data
    where
    "reportStartTime" = (select "reportStartTime" from metrics)
    group by title,issue,publisher,"storeCount"
    ) select RANK() OVER (ORDER BY sold_qty DESC) as rank, * from sales order by rank limit 400;
    '''

    @st.fragment
    def load_rpt_dates():
        """Loads reporting dates from the database"""
        if st.session_state['rpt_dates'] =='':
            with connections.engine.connect() as conn:
                rpt_dates = pl.read_database('Select distinct "reportStartTime" from csa_data', conn).with_columns(pl.col("reportStartTime").dt.strftime("%Y-%m-%d")).sort("reportStartTime").to_series().to_list()
            
            st.session_state['rpt_dates'] = rpt_dates

    @st.fragment
    def load_report(rpt_dt):
       """Retrieves the top 400 sales report for a given week."""
       with connections.engine.connect() as conn:
            try:
                results = pl.read_database(Reports.top_400_query.format(rpt_dt = rpt_dt),conn)
                return results
            except Exception as e:
                raise e