import requests
import json

url = "https://api-prod.etf.com/v2/fund/fund-details"

payload = {
    "query": "growthData",
    "variables": {
        "fund_id": "556",
        "ticker": "VOO",
        "fund_isin": ""
    }
}

headers = {
    "accept": "*/*",
    "content-type": "application/json",
    "origin": "https://www.etf.com",
    "referer": "https://www.etf.com/",
    "user-agent": "Mozilla/5.0"
}

response = requests.post(url, headers=headers, json=payload)

print(response.status_code)
print(response.text)