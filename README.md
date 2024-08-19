## Airflow ##
<img width="680" alt="image" src="https://github.com/user-attachments/assets/1bf8e468-96b1-4e2c-ad43-23900fec8bf1">
<br>각각의 데이터 수집처마다 DAG를 작성했고, 크게는 항공권/날씨/환율/투어 정보로 나뉩니다.
<br>1. 항공권은 공공 데이터 포털 API를 이용한 인천공항에 취항한 공항 정보(service_airport_to_ICN), 운항 계획 수(arr_count_to_ICN), APIFY를 이용해 항공권 정보를 가져오는(flight_price)로 구성되어 있습니다
