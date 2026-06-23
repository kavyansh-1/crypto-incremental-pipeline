from extract import fetch_coin_data
from db import get_connection,get_watermark,update_watermark,log_run
from datetime import datetime,timezone

PIPELINE_NAME="coingecko_prices"

def parse_timestamp(ts_string):
    formats=[
        "%Y-%m-%dT%H:%M:%S.%fZ",  # with microseconds
        "%Y-%m-%dT%H:%M:%SZ",      # without microseconds
    ]
    for fmt in formats:
      try:
        return datetime.strptime(ts_string,fmt).replace(tzinfo=timezone.utc)
      except ValueError:
          continue
    raise ValueError(f"Timestamp '{ts_string}' didn't match any known format")

def filter_new_coins(coins,watermark):
    new_coins=[]
    for coin in coins:
        coin_time=parse_timestamp(coin['last_updated'])
        if coin_time > watermark.replace(tzinfo=timezone.utc):
            new_coins.append(coin)
    return new_coins

def load_coins(coins):
    conn=get_connection()
    cur=conn.cursor()

    for coin in coins:
       cur.execute("""
    INSERT INTO coin_prices (
        id, symbol, name, current_price, market_cap,
        market_cap_rank, total_volume, price_change_percentage_24h, last_updated
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
            symbol=EXCLUDED.symbol,
            name=EXCLUDED.name,
            current_price=EXCLUDED.current_price,
            market_cap=EXCLUDED.market_cap,
            market_cap_rank=EXCLUDED.market_cap_rank,
            total_volume = EXCLUDED.total_volume,
            price_change_percentage_24h=EXCLUDED.price_change_percentage_24h,
            last_updated=EXCLUDED.last_updated
""", (
    coin["id"],
    coin["symbol"],
    coin["name"],
    coin["current_price"],
    coin["market_cap"],
    coin["market_cap_rank"],
    coin["total_volume"],
    coin["price_change_percentage_24h"],
    coin["last_updated"]
))
    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    started_at=datetime.now(timezone.utc)
    rows_fetched=0
    rows_loaded=0
    try:
       watermark=get_watermark(PIPELINE_NAME)
       print(f"Current watermark: {watermark}")

       all_coins=fetch_coin_data(max_pages=1)
       rows_fetched=len(all_coins)
       print(f"Fetched {len(all_coins)} coins from api")

       new_coins=filter_new_coins(all_coins,watermark)
       print(f"{len(new_coins)} coins are new/updated since last watermark")

       if new_coins:
           load_coins(new_coins)
           rows_loaded=len(new_coins)
           print(f"Upserted {len(new_coins)} rows")

           newset_timestamp=max(parse_timestamp(c['last_updated']) for c in new_coins)
           update_watermark(PIPELINE_NAME,newset_timestamp)
           print(F"Watermark updated on {newset_timestamp}")
       else:
           print("No new data,watermark unchanged")

       finished_at=datetime.now(timezone.utc)
       log_run(PIPELINE_NAME,"success",rows_fetched,rows_loaded,None,started_at,finished_at)
    
    except Exception as e:
        finished_at = datetime.now(timezone.utc)
        log_run(PIPELINE_NAME, "failed", rows_fetched, rows_loaded, str(e), started_at, finished_at)
        print(f"Pipeline failed: {e}")
        print("Run logged: failed")
