import polars as pl
import streamlit as st
import altair as alt
import connections


class CSA_Weekly_Sales():

    pattern = r"([\w\d\s\W]*)[#]{1}[\w\d\s\W]*"
    publisher = pl.col("barcode").cast(str).str.slice(0,6).alias("publisher_code")
    title_code = pl.col("barcode").cast(str).str.slice(6,6).alias("title_code")
    #issue = pl.col("barcode").cast(str).str.slice(12,3).cast(pl.Int64).alias("issue")
    issue = pl.when(pl.col("issue_name").str.contains("#")).then(
        pl.col("issue_name").str.extract(r"(?:.*[#]{1})(\d+)").cast(pl.Int32)
    ).otherwise(pl.lit(1)).cast(pl.Int32).alias('issue')
    cover = pl.col("barcode").cast(str).str.slice(15,1).cast(pl.Int64).alias("cover")
    printing = pl.col("barcode").cast(str).str.slice(16,1).cast(pl.Int64).alias("printing")
    title = pl.col("title").str.extract(pattern,1)\
        .str.strip_chars()\
            .str.strip_chars("'")\
                .str.to_titlecase()


    cleaned_columns = [publisher,title_code,issue,cover,printing,title]

    schema = {"barcode": pl.String,
              "title":pl.String,
              "reportStartTime":pl.Datetime(time_unit='us'),
              "reportEndTime": pl.Datetime(time_unit='us'),
              "totalEverSold": pl.Int32,
              "totalSoldInSpan": pl.Int32,
              "totalPulled": pl.Int32,
              "storeCount": pl.Int32,
              "issue_name": pl.String}

    def __init__(self,file: str):
        """Initiliazes class to process a weekly sales file.

        Args:
            file (str): Weekly sales file.
        """
        self.file = file
        self.load_publisher_info()
        self.load_data()
        self.clean_data()
        self.join_pub_names()
        self.df = self.df.drop('issue_name')
            
    def load_data(self):
        """Generates DataFrame from file.
        """
        self.csv_columns = pl.scan_csv(self.file,ignore_errors=True).collect_schema().names()

        if 'title' not in self.csv_columns:
            new_schema = {"barcode": pl.String,
              "item_name":pl.String,
              "reportStartTime":pl.Datetime(time_unit='us'),
              "reportEndTime": pl.Datetime(time_unit='us'),
              "totalEverSold": pl.Int32,
              "totalSoldInSpan": pl.Int32,
              "totalPulled": pl.Int32,
              "storeCount": pl.Int32,
              "issue_name": pl.String,
              "type": pl.String}

            self.df = pl.scan_csv(self.file,schema_overrides=new_schema,missing_utf8_is_empty_string=True)\
                .rename({'item_name': 'title'}).filter(pl.col("type")=='comics').drop('type')
        else:
            self.df = pl.scan_csv(self.file,schema_overrides=self.schema)  
        
        self.df = self.df.cast({'barcode': pl.Int64},strict=False).drop_nulls().select(self.schema.keys())

    def clean_data(self):
        """Cleans and transforms the data."""        
        self.df = self.df.with_columns(pl.col("barcode").cast(pl.String).str.pad_start(17,"0")).with_columns(self.cleaned_columns)
    
    def load_publisher_info(self):
        """Loads publisher names and codes from the cloud."""
        query = '''select distinct "PUBLISHER" as publisher, substring("UPC_NO",1,6) as publisher_code from bestseller_data where "UPC_NO" is not null;'''
        with connections.engine.connect() as bs_conn:
            try:
                self.pub_names = pl.read_database(query,bs_conn).lazy()
            except Exception as e:
                raise e
    def join_pub_names(self):
        """Joins publisher info to the file."""
        prana_fix = pl.when(
            pl.col('title') == "Comics The Magazine"
        ).then(
            pl.lit("Prana Publishers")
            ).otherwise(pl.col("publisher")).alias("publisher")
        
        missing_publishers = pl.when(
            pl.col("publisher").is_null()
            ).then(
                pl.lit("Other")
                ).otherwise(
                    pl.col("publisher")
                    ).alias("publisher")

        self.df = self.df.join(self.pub_names, on = 'publisher_code',how='left')\
            .with_columns(missing_publishers)\
                .with_columns(prana_fix)\
                    .drop_nulls(subset=['title'])\
                        .unique(subset=['barcode'])\
                            .collect()
        

class CSA_Monthly_Sales():

    output_file_name = "top400.csv"
    output_file_name_dollars = output_file_name.replace(".csv","_w_dollars.csv")

    def __init__(self, sales_df: pl.DataFrame, sales_file: str):
        """Processes monthly sales file.

        Args:
            sales_df (pl.DataFrame): A processed CSA sales DataFrame.
            sales_file (str): The path to the monthly sales file
        """
        self.df = sales_df
        self.sales_file = pl.read_csv(sales_file,
                                      schema_overrides={
                                          'barcode': pl.String,
                                          'issue_name': pl.String
                                      },
                                      ignore_errors=True)
        
        self.df = self.join_issue_name(self.df, self.sales_file.select("barcode","issue_name"))
        self.df = self.prep_data(self.df)
        self.df = self.correct_ctm_pub(self.df)
        self.df = self.add_prices(self.df, self.sales_file.select('barcode','price'))

    @staticmethod
    def join_issue_name(df_1: pl.DataFrame, df_2: pl.DataFrame) -> pl.DataFrame:
        
        return df_1.join(df_2, on='barcode', how='inner')
        
    @staticmethod
    def prep_data(df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
        pl.col('issue_name').str.strip_chars("'")\
            .str.strip_chars().str.to_titlecase())        
        return df

    @staticmethod
    def correct_ctm_pub(df: pl.DataFrame) -> pl.DataFrame:
        df = df.with_columns(
            pl.when(pl.col("issue_name").str.contains("Comics The Magazine"))
            .then(pl.lit("Prana Publishers"))
            .otherwise(pl.col("publisher"))
            .alias("publisher")
        )
        return df

    @staticmethod
    def add_prices(df: pl.DataFrame, sales_df: pl.DataFrame):
        prices = sales_df
        df = df.join(prices, on='barcode', how='left')
        return df

    @staticmethod
    def calc_top_sales(df:pl.DataFrame,limit: int=400, dollars: bool=False) -> pl.DataFrame:
    
        # Add total Dollars
        df = df.with_columns(
            (pl.col("price")*pl.col("totalSoldInSpan")).alias("totalDollars")
            )
    
        top_sales = df.group_by('issue_name','publisher')\
        .agg(pl.max("storeCount").alias("storeCount"),
             pl.sum("totalSoldInSpan").alias("sold_qty"),
             (pl.sum("totalSoldInSpan")/pl.max("storeCount")).alias("sold_per_store"),
             pl.sum("totalDollars").alias('dollars_sold'),
             (pl.sum("totalDollars")/pl.max("storeCount")).alias('dollars_sold_per_store'),
             )\
                .with_columns(pl.col('sold_qty').rank(method='ordinal',descending=True).alias("rank")).sort('rank',descending=False).head(limit)
    
        if not dollars:
            top_sales = top_sales.drop('dollars_sold',"dollars_sold_per_store")
        else:
            top_sales = top_sales\
                .with_columns(
                    pl.col('dollars_sold').rank(method='ordinal',descending=True).alias("rank")
                    ).sort('rank',descending=False).head(limit)
        
        return top_sales

    @staticmethod
    def plot_marketshare(df: pl.DataFrame,rpt_dt: str, dollars: bool=False):
    
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
    
        if dollars:
            sub = f'Based on dollar sales in {rpt_dt}'
        else:
            sub=f'Based on unit sales in {rpt_dt}'
        
        chart = chart.properties(width=700, height=500, 
                                 padding={"top": 20, "bottom": 20, "left": 20, "right": 20},
                                 title=alt.TitleParams(
                                     "Publisher Market Share %",
                                     fontSize=18,
                                     subtitle=sub,
                                     subtitleFontSize=14,
                                     anchor='start'))
        
        chart = chart.configure_view(strokeWidth=0)

        st.altair_chart(chart)