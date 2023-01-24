DECLARE start_hour STRING;
DECLARE max_ds_hour STRING;

SET max_ds_hour = (
  SELECT MAX(ds_hour) AS max_ds_hour
  FROM meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR
);

IF (
    SELECT COUNT(ds_hour)
    FROM meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR
    WHERE site = '{site_id}'
) > 1 THEN

    CREATE TEMP TABLE lag_ds_hour_table AS
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
    WHERE site = '{site_id}';

    SET start_hour = (
      SELECT MAX(lag_ds_hour) FROM lag_ds_hour_table
    );
ELSE
    SET start_hour = (
      SELECT CONCAT(SPLIT(max_ds_hour, 'T')[offset(0)], 'T00:00:00.000-0400')
    );
END IF;

SELECT a.*, CAST(hour AS INT64) AS int_hour
FROM meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_PER_HOUR a
WHERE ds >= DATE(SPLIT(start_hour, 'T')[offset(0)])
    AND CONCAT(ds, 'T', hour) >= start_hour
    AND site = '{site_id}';