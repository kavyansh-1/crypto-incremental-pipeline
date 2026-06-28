import requests
from tenacity import retry,stop_after_attempt,wait_exponential

@retry(stop=stop_after_attempt(3),wait=wait_exponential(multiplier=1,min=2,max=10))
def fetch_coin_data(max_pages=1):
    url = "https://api.coingecko.com/api/v3/coins/markets"
    all_coins=[]

    for page in range(1,max_pages+1):
       params = {
        "vs_currency": "usd",
        "order": "market_cap_desc",
        "per_page": 100,
        "page": page
        }

       response=requests.get(url,params)

       if response.status_code != 200:
          print(f"API call failed on page {page} with status code: {response.status_code}")
          raise Exception(f"API returned status {response.status_code}")

       data = response.json()

       if not data:
           print(f"No more data after page {page-1}. Stopping")
           break
       
       all_coins.extend(data)
       print(f"Fetched page {page}: {len(data)} coins")

    return all_coins

if __name__ == "__main__":
    coins = fetch_coin_data(max_pages=2)
    print(f"Fetched {len(coins)} coins")
    print(coins[0])