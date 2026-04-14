from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import random
import os

default_args = {
    'owner': 'Hieu Tran',
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='daily_etl_pipeline_airflow3',
    description='ETL workflow demonstrating dynamic task mapping and assets',
    schedule='@daily',
    start_date=datetime(2026, 4, 14),
    catchup=False,
    default_args=default_args,
    tags=['airflow3', 'etl']
)
def daily_etl_pipeline():

    @task
    def extract_market_data():
        """
        Simulate extracting market data for popular companies.
        This task mimics pulling live stock prices or API data.
        """

        companies = [
            "Apple", "Amazon", "Google", "Microsoft",
            "Tesla", "Netflix", "NVIDIA", "Meta"
        ]

        # Simulate today's timestamped price data
        records = []
        for company in companies:
            price = round(random.uniform(100, 1500), 2)
            change = round(random.uniform(-5, 5), 2)
            records.append({
                'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'company': company,
                'price_usd': price,
                'daily_change_percent': change
            })

        df = pd.DataFrame(records)
        os.makedirs('/opt/airflow/tmp', exist_ok=True)
        raw_path = '/opt/airflow/tmp/market_data.csv'
        df.to_csv(raw_path, index=False)

        print(f"[EXTRACT] Market data successfully generated at {raw_path}")
        return raw_path

    @task
    def transform_market_data(raw_file:str):
        """
        Clean and analyze extracted market data.
        This task simulates transforming raw stock data
        to identify the top gainers and losers of the day.
        """

        df = pd.read_csv(raw_file)

        # Clean: ensure numeric fields are valid
        df['price_usd'] = pd.to_numeric(df['price_usd'], errors='coerce')
        df['daily_change_percent'] = pd.to_numeric(df['daily_change_percent'], errors='coerce')

        # Sort companies by daily change (descending = top gainers)
        df_sorted = df.sort_values(by='daily_change_percent', ascending=False)

        # Select top 3 gainers and bottom 3 losers
        top_gainers = df_sorted.head(3)
        top_losers = df_sorted.tail(3)

        # Save transformed fields
        os.makedirs('/opt/airflow/tmp', exist_ok=True)
        gainers_path = '/opt/airflow/tmp/top_gainers.csv'
        losers_path = '/opt/airflow/tmp/top_losers.csv'

        top_gainers.to_csv(gainers_path, index=False)
        top_losers.to_csv(losers_path, index=False)

        print(f"[TRANSFORM] Top gainers saved to {gainers_path}")
        print(f"[TRANSFORM] Top losers saved to {losers_path}")

        return {
            'gainers': gainers_path,
            'losers:': losers_path,
        }

    raw = extract_market_data()
    transform_market_data(raw)

dag = daily_etl_pipeline()