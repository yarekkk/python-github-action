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
    print('start')
    import pyodbc
    import pandas as pd
    import datetime
    import sqlalchemy
    from sqlalchemy import event
    import logging
    import urllib
    
    print(datetime.datetime.now())
    
    logging.basicConfig(level = 20, filename = './run_sql_log1.log', filemode = 'w',format='Date-Time : %(asctime)s : Line No. : %(lineno)d - %(message)s')
    # Some other example server values are
    # server = 'localhost\sqlexpress' # for a named instance
    # server = 'myserver,port' # to specify an alternate port
    logging.info(f"start time is :{datetime.datetime.now()}")
    
    # try:
    server = '172.17.13.41' 
    database = '3CDB' 
    username = '3CBI' 
    password = '9SymBGNV7Hc12t7feSY8'  
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    # raise KeyError
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
    where [date_trxservice] >='2023-04-01'
    group by trx.[location_id],trx.[location_no],[location_name],currency_code,[location_desc]
    having sum([trans_amount]) > 0.01"""
    df = pd.read_sql(query, cnxn)
    # df.to_csv('integra_ds.csv', index = False)
    logging.info(f"time after reading info from sql :{datetime.datetime.now()}")
    logging.info(df.shape)
    server = '10.23.0.161' 
    database = 'Smartsheets' 
    username = 'Booking_Tool_User' 
    password = 'Foxinho2023'  
    params = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server + ';PORT=1433;DATABASE=' + database + ';UID=' + username + ';PWD=' + password
    db_params = urllib.parse.quote_plus(params)
    engine = sqlalchemy.create_engine("mssql+pyodbc:///?odbc_connect={}".format(db_params))
    @event.listens_for(engine, "before_cursor_execute")
    def receive_before_cursor_execute(
        conn, cursor, statement, params, context, executemany
            ):
                if executemany:
                    cursor.fast_executemany = True

    
    cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
    cursor = cnxn.cursor()
    # df = pd.read_csv('integra_ds.csv')

    # df = df2.copy()
    df['location_desc'] = df['location_desc'].astype(str).str[:50]
    df['tr_sum'] = df['tr_sum'].astype(float)
    df['last_date'] = pd.to_datetime(df['last_date'])
    df = df.fillna("None")
    df.to_sql('Integra_transactions', engine, index=False, if_exists="replace", schema="dbo")
    # if mailbox == 'Sales2Board':
    cursor.execute("""UPDATE [APP_FLOW].[dbo].[DATA_GATEWAY_LOCATIONS]
SET [TRANSACTION_VOLUME] = [tr_sum]
FROM [APP_FLOW].[dbo].[DATA_GATEWAY_LOCATIONS] GTW WITH(NOLOCK) INNER JOIN
     [Smartsheets].[dbo].[Integra_transactions] ITG WITH(NOLOCK) ON GTW.[LOCATION_ID] = ITG.[location_no]
WHERE [tr_sum] > 1""")
    # j = 0
    # for index, row in df.iterrows():
    # #     if not pd.isnull(row.sender):
    #     cursor.execute("INSERT INTO [Smartsheets].[dbo].[Integra_transactions] (location_no,location_id,[tr_sum],[tr_count],[start_date],[last_date],location_name,location_desc,currency_code) values(?,?,?,?,?,?,?,?,?)", 
    #         row['location_no'], row.location_id, row['tr_sum'],row['tr_count'],row['start_date'],row['last_date'],row['location_name'],row['location_desc'],row['currency_code'])

    cnxn.commit()
    cursor.close()
    logging.info(f"time after writing info to sql server :{datetime.datetime.now()}")
    # except Exception as error:
    #     print("An error occurred:", error)
    #     logging.exception("message")
        
