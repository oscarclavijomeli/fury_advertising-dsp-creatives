WITH max_ds_hour_table AS
(
  SELECT MAX(ds_hour) AS max_ds_hour
  FROM meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR
  WHERE site = '{site_id}'
),
lag_ds_hour_table AS
(
  SELECT ds_hour,
    CASE
      WHEN ds_hour = max_ds_hour THEN
        LAG(ds_hour) OVER(
          PARTITION BY site
          ORDER BY ds_hour
        )
      ELSE NULL
    END AS lag_ds_hour
  FROM meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR
  CROSS JOIN max_ds_hour_table
  WHERE site = '{site_id}'
),
start_hour_table AS
(
  SELECT MAX(lag_ds_hour) AS start_hour FROM lag_ds_hour_table
)

SELECT a.*, CAST(hour AS INT64) AS int_hour
FROM meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_PER_HOUR a
CROSS JOIN start_hour_table
WHERE ds >= DATE(SPLIT(START_HOUR, 'T')[offset(0)])
    AND CONCAT(ds, 'T', hour) >= start_hour
    AND site = '{site_id}'