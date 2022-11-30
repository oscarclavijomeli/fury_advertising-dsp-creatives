CREATE OR REPLACE TABLE meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS
AS
SELECT
    site,
    campaign_id, line_item_id, creative_id,
    sum(n_prints) AS n_prints,
    sum(n_clicks) AS n_clicks,
    DATE_DIFF(MAX(ds), MIN(ds), DAY) + 1 AS days
FROM meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_PER_DAY
WHERE creative_id IS NOT NULL
GROUP BY 1,2,3,4
;