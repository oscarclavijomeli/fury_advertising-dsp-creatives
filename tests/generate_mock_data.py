"""File to generate mock data about creatives"""
import numpy as np
import pandas as pd

np.random.seed(321)
N = 12
hour = list(range(N))
site = N * ["MLA"]
campaign_id = N * [12345]
line_item_id = N * [67890]
creative_id = [13579 + i for i in range(N)]
n_prints = (1e6 * np.random.rand(12)).astype(int)
n_clicks = (1e4 * np.random.rand(12)).astype(int)


def generate_mock_data() -> pd.DataFrame:
    """Returns mock data"""
    df = pd.DataFrame(
        {
            "hours": hour,
            "site": site,
            "campaign_id": campaign_id,
            "line_item_id": line_item_id,
            "creative_id": creative_id,
            "n_prints": n_prints,
            "n_clicks": n_clicks,
        }
    )
    df["n_prints"] = df.apply(lambda x: x.n_prints if x.n_prints > x.n_clicks else x.n_clicks, axis=1)
    return df
