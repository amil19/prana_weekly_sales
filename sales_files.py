import polars as pl
import streamlit as st
import connections


class CSA_Weekly_Sales():

    pattern = r"([\w\d\s\W]*)[#]{1}[\w\d\s\W]*"
    publisher = pl.col("barcode").cast(str).str.slice(0,6).alias("publisher_code")
    title_code = pl.col("barcode").cast(str).str.slice(6,6).alias("title_code")
    issue = pl.col("barcode").cast(str).str.slice(12,3).cast(pl.Int64).alias("issue")
    cover = pl.col("barcode").cast(str).str.slice(15,1).cast(pl.Int64).alias("cover")
    printing = pl.col("barcode").cast(str).str.slice(16,1).cast(pl.Int64).alias("printing")
    title = pl.col("title").str.extract(pattern,1).str.strip_chars()

    cleaned_columns = [publisher,title_code,issue,cover,printing,title]

    schema = {"barcode": pl.String,
              "title":pl.String,
              "reportStartTime":pl.Datetime(time_unit='us'),
              "reportEndTime": pl.Datetime(time_unit='us'),
              "totalEverSold": pl.Int32,
              "totalSoldInSpan": pl.Int32,
              "totalPulled": pl.Int32,
              "storeCount": pl.Int32}

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
            
    def load_data(self):
        """Generates DataFrame from file.
        """
        self.df = pl.scan_csv(self.file,schema=self.schema)\
            .cast({'barcode': pl.Int64},strict=False).drop_nulls().select(self.schema.keys())

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
        """Joins pubisher info to the file."""
        self.df = self.df.join(self.pub_names, on = 'publisher_code',how='left')\
            .with_columns(pl.when(pl.col("publisher").is_null()).then(pl.lit("Other")).otherwise(pl.col("publisher")).alias("publisher"))\
                .drop_nulls(subset=['title']).unique(subset=['barcode']).collect()
        
