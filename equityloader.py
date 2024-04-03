import pandas as pd
import mysql.connector
from mysql.connector import IntegrityError


def record_processing():
    df_merge = pd.merge(df_mcap, df_eqdet, on='SYMBOL')
    count=0
    for idx in df_merge.index:
        market_cap = 0
        if type(df_merge['Market_capitalization'][idx]) is float:
            market_cap = float(df_merge['Market_capitalization'][idx])
        else:
            market_cap = 0

        value = (df_merge['SYMBOL'][idx], df_merge['ISIN NUMBER'][idx], df_merge['Company Name'][idx],
                 df_merge['SERIES'][idx], df_merge['DATE OF LISTING'][idx], int(df_merge['PAID UP VALUE'][idx]),
                 int(df_merge['FACE VALUE'][idx]), int(df_merge['MARKET LOT'][idx]),market_cap)
        print(value)
        cur.execute(query, value)
        count += 1

    print("total record inserted :" + str(count))
    mydb.commit()


if __name__ == '__main__':
    ##DB Configurations
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="asdf1234"
    )
    cur = mydb.cursor()
    query = " INSERT INTO security.equity_info (symbol,ISIN,name,series,listing_date,paidup_value,face_value,mkt_lot,cur_mkt_cap,created_at,created_by,updated_at,updated_by) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,current_timestamp,'amitsama',current_timestamp,'amitsama') "
    ## Reading from file.
    df_mcap = pd.read_excel("MCAP31122023_NSE.xlsx")
    df_eqdet = pd.read_csv("EQUITY_L.csv")

    ## Calling for data processing.
    record_processing()
    mydb.disconnect()
