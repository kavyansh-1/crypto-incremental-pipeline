import requests

url="https://api.coingecko.com/api/v3/coins/markets"
params={
    "vs_currency":"usd",
    "order":"market_cap_desc",
    "per_page":10,
    "page":1
}

response=requests.get(url,params=params)
print("Status code:",response.status_code)

data=response.json()
print("Number of records:",len(data))
print(data[0])