import polars as pl
import streamlit as st
import connections
import altair as alt

class Reports:

    top_400_query = """
    with title_sales as
    (
      select
        title,
        issue,
        publisher,
        max("storeCount") as "storeCount",
        sum("totalSoldInSpan") as sold_qty,
        sum("totalSoldInSpan")/max("storeCount") as sold_per_store
      from csa_data
      where
      "reportStartTime" = '{rpt_dt}'
      group by title,issue,publisher
    )
    select RANK() OVER (ORDER BY sold_qty DESC) as rank, * from title_sales order by rank limit 400;
    """

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

    @staticmethod   
    def calc_marketshare(df: pl.DataFrame):
        marketshare_df = df.group_by('publisher').agg(
            pl.sum("sold_qty").alias('total_sales'))
        
        return marketshare_df.sort('total_sales',descending=True).head(10)
    
    @staticmethod
    def plot_marketshare(df: pl.DataFrame,rpt_dt: str):

        chart = alt.Chart(df).transform_joinaggregate(
            TotalSales='sum(total_sales)',
        ).transform_calculate(
            PercentOfTotal="datum.total_sales / datum.TotalSales"
        ).mark_bar(color='green').encode(
            alt.X('PercentOfTotal:Q',
                  axis=alt.Axis(format='.0%',values=[0.0,.05,.1,.15,.2,.25,.3,.35,.4,.45,.5]), scale=alt.Scale(domain=[0, .5], nice=False)
                  ).title('Market Share'),
            y=alt.Y('publisher:N',
                    axis=alt.Axis(labelFontSize=14, labelLimit=200)
                    ).sort('-x').title(None))\
                .configure_axis(grid=False).properties(width=700,height=500, padding=20,
                                                       title=alt.TitleParams(
        "Publisher Market Share % (based on units)",fontSize=18,
        subtitle=[f'Week of {rpt_dt}'],subtitleFontSize=14)).configure_view(
    strokeWidth=0)

        st.altair_chart(chart)

        