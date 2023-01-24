DECLARE SITE_ID STRING DEFAULT "{site_id}";

WITH server_timestamp AS
(
    SELECT *, CAST(CONCAT(ds, ' ', hour, ':00:00.000-0400') AS TIMESTAMP) AS ts
    FROM meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_PER_HOUR
    WHERE site = SITE_ID
)

SELECT
    site,
    campaign_id, line_item_id, creative_id,
    sum(n_prints) AS n_prints,
    sum(n_clicks) AS n_clicks,
    TIMESTAMP_DIFF(MAX(ts), MIN(ts), HOUR) + 1 AS hours
FROM server_timestamp
WHERE creative_id IS NOT NULL
GROUP BY 1,2,3,4
;