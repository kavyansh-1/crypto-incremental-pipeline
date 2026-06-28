from db import get_connection

conn=get_connection() #To connect our database with python
cur=conn.cursor() #A pointer to execute commands and fetch results from and into databases

cur.execute("""
    INSERT INTO coin_prices(
            id,symbol,current_price,market_cap,
            market_cap_rank,total_volume,price_change_percentage_24h,last_updated,name)

    VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
""",(
    "bitcoin","btc",63934.21,1281388956196,
    1,13529331256,0.234,"2026-06-21T18:18:05.307Z","BITCOIN"
))


conn.commit()
print("Row Inserted successfully")

cur.close()
conn.close()