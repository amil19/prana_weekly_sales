import polars as pl
import streamlit as st
import connections
import altair as alt
from rich.console import Console

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
    def load_report(rpt_dt) -> pl.DataFrame:
       """
       Retrieves the top 400 sales report for a given week.
       
       Args:
            rpt_dt(str): Sales start date to retrieve.

       Returns:
            pl.DataFrame: Top 400 items based on quantity sold in a period.
       """
       with connections.engine.connect() as conn:
            try:
                results = pl.read_database(Reports.top_400_query.format(rpt_dt = rpt_dt),conn)
                return results
            except Exception as e:
                raise e

    @staticmethod   
    def calc_marketshare(df: pl.DataFrame) -> pl.DataFrame:
        """Calculates the marketshare of sales by publisher.

        Args:
            df (pl.DataFrame): DataFrame containing sales.

        Returns:
            pl.DataFrame: Marketshare of top 10 publishers.
        """
        marketshare_df = df.group_by('publisher').agg(
            pl.sum("sold_qty").alias('total_sales'))
        
        return marketshare_df.sort('total_sales',descending=True).head(10)
    
    @staticmethod
    def plot_marketshare(df: pl.DataFrame,rpt_dt: str):
        """Plots the publisher marketshare for a given week.

        Args:
            df (pl.DataFrame): DataFrame containing that sales for a given week.
            rpt_dt (str): Start date of sales period.
        """

        chart = alt.Chart(df).transform_joinaggregate(
            TotalSales='sum(total_sales)'
            ).transform_calculate(
                PercentOfTotal="datum.total_sales / datum.TotalSales"
                ).mark_bar(color='green').encode(
                    alt.X('PercentOfTotal:Q',
                        axis=alt.Axis(format='.0%',
                                        values=[0.0,.05,.1,.15,.2,.25,.3,.35,.4,.45,.5]), 
                        scale=alt.Scale(domain=[0, .5], nice=False)
                        ).title('Market Share'),
                    y=alt.Y('publisher:N',
                    axis=alt.Axis(labelLimit=200)
                    ).sort('-x').title(None))
        
        text = chart.mark_text(
            align='center',
            baseline='bottom',
            dy=5,
            dx=25
        ).encode(
            alt.Text('PercentOfTotal:Q',format='.2%')
        )
        
        chart = chart+text

        chart = chart.configure_axis(grid=False,
                                    labelFontSize=14,
                                    titleFontSize=16)
        
        chart = chart.properties(width=700,height=500,padding={"left": 20, "right": 20, "top": 20, "bottom": 20},
                                title=alt.TitleParams(
                                    "Publisher Market Share %",
                                    fontSize=18,
                                    subtitle=[f'Based on unit sales during the week of {rpt_dt}'],
                                    subtitleFontSize=14,
                                    anchor='start'))

        chart = chart.configure_view(strokeWidth=0)

        st.altair_chart(chart)