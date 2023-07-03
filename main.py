import logging
import logging.handlers
import os

import requests

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

try:
    SOME_SECRET = os.environ["SOME_SECRET"]
except KeyError:
    SOME_SECRET = "Token not available!"
    #logger.info("Token not available!")
    #raise


if __name__ == "__main__":
    import pyodbc
    import pandas as pd
    import datetime
    import logging
    logging.basicConfig(level = logging.INFO, filename = 'sql.log')
    # Some other example server values are
    # server = 'localhost\sqlexpress' # for a named instance
    # server = 'myserver,port' # to specify an alternate port
    print(datetime.datetime.now())
    server = '172.17.13.41' 
    database = '3CDB' 
    username = '3CBI' 
    password = '9SymBGNV7Hc12t7feSY8'  
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    
    query = """SELECT trx.[location_no]
        ,trx.[location_id]
          ,sum([trans_amount]) as tr_sum
          ,count([trans_amount]) as tr_count
         ,max([date_trxservice]) as last_date
         ,min([date_trxservice]) as start_date
        ,loc.[location_name]
         ,loc.[location_desc]
          ,cur.currency_code
    FROM [3CDB].[ixs].[trx] trx
    left join [3CDB].[ccc].[location] loc on trx.location_no = loc.location_no
    left join [3CDB].[ccc].[currency] cur on loc.currency_id = cur.currency_id
    where [date_trxservice] >='01/04/2023'
    group by trx.[location_id],trx.[location_no],[location_name],currency_code,[location_desc]
    having sum([trans_amount]) > 0.01"""
    df = pd.read_sql(query, cnxn)
    # df.to_csv('integra_ds.csv', index = False)
    print(datetime.datetime.now())
    print(df.shape)
    server = '10.23.0.161' 
    database = 'Smartsheets' 
    username = 'Booking_Tool_User' 
    password = 'Foxinho2023'  
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    df = pd.read_csv('integra_ds.csv')
    
    # df = df2.copy()
    df['location_desc'] = df['location_desc'].astype(str).str[:50]
    df['tr_sum'] = df['tr_sum'].astype(float)
    df['last_date'] = pd.to_datetime(df['last_date'])
    df = df.fillna("None")
    # if mailbox == 'Sales2Board':
    cursor.execute("delete from [Smartsheets].[dbo].[Integra_transactions]")
    j = 0
    for index, row in df.iterrows():
    #     if not pd.isnull(row.sender):
        cursor.execute("INSERT INTO [Smartsheets].[dbo].[Integra_transactions] (location_no,location_id,[tr_sum],[tr_count],[start_date],[last_date],location_name,location_desc,currency_code) values(?,?,?,?,?,?,?,?,?)", 
            row['location_no'], row.location_id, row['tr_sum'],row['tr_count'],row['start_date'],row['last_date'],row['location_name'],row['location_desc'],row['currency_code'])
    
    cnxn.commit()
    cursor.close()
    print(datetime.datetime.now())
