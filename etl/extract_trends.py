from pytrends.request import TrendReq
import pandas as pd

def fetch_trends(keywords):
    pytrends = TrendReq(hl='id-ID', tz=360)
    pytrends.build_payload(keywords, timeframe='today 3-m', geo='ID')
    data = pytrends.interest_over_time()
    return data

if __name__ == "__main__":
    keywords = ["takjil", "baju lebaran", "snack lebaran"]
    df = fetch_trends(keywords)
    df.to_csv("../data/trends_raw.csv")
    print("Data berhasil disimpan ke data/trends_raw.csv")