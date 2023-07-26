"""File to generate mock data about creatives"""
import numpy as np
import pandas as pd

np.random.seed(321)
N = 12
start_date = pd.to_datetime("2023-05-10")
dates = [start_date + pd.DateOffset(days=i) for i in range(N)]
hour = list(range(1, N + 1))
site = N * ["MLA"]
campaign_id = N * [12345]
line_item_id = N * [67890]
creative_id = [13579 + i for i in range(N)]
n_prints = (1e6 * np.random.rand(12)).astype(int)
n_clicks = (1e4 * np.random.rand(12)).astype(int)
n_conversions = (1e3 * np.random.rand(12)).astype(int)
strategy = (N // 3) * ["conversion", "awareness", "consideration"]


def generate_mock_data() -> pd.DataFrame:
    """Returns mock data"""
    df_mock = pd.DataFrame(
        {
            "ds": dates,
            "hours": hour,
            "site": site,
            "campaign_id": campaign_id,
            "line_item_id": line_item_id,
            "creative_id": creative_id,
            "n_prints": n_prints,
            "n_clicks": n_clicks,
            "n_conversions": n_conversions,
            "strategy": strategy,
        }
    )
    df_mock["n_prints"] = df_mock.apply(lambda x: x.n_prints if x.n_prints > x.n_clicks else x.n_clicks, axis=1)
    return df_mock
