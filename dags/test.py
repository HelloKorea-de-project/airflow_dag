import pandas as pd

df = pd.read_csv('../external_pq/cheapest_flight_to_ICN_updated_2024-08-01.csv')

df.to_csv('cheapest_flight_to_ICN_updated_2024-08-01.csv', index=False)
# 8월 5일