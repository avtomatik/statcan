import pandas as pd

from statcan.src.core.constants import SERIES_IDS_PRICE_A, SERIES_IDS_PRICE_B
from stats.src.can.combine import combine_can_price_a, combine_can_price_b

df = pd.concat(
    [
        combine_can_price_a(SERIES_IDS_PRICE_A),
        combine_can_price_b(SERIES_IDS_PRICE_B),
    ],
    axis=1,
    sort=True
)

df['mean'] = df.mean(axis=1)
df['cum_mean'] = df.iloc[:, -1].add(1).cumprod()
df = df.div(df.loc[2012])
FILE_NAME = 'data_composed.csv'
df.to_csv(FILE_NAME)
