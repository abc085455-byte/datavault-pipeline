from scripts.extract.api_extractor import CoinGeckoExtractor

e = CoinGeckoExtractor()
data = e.fetch_market_data()

print(f"Coins mile: {len(data)}")
for coin in data:
    print(f"  {coin['name']:12} | Price: ${coin['current_price']:>12,.2f} | 24h: {coin['price_change_percentage_24h']:+.2f}%")