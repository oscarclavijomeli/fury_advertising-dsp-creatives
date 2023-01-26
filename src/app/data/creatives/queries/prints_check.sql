DECLARE SITE_ID STRING DEFAULT '{site_id}';
DECLARE TIME_ZONE STRING DEFAULT '{time_zone}';
DECLARE max_ds_hour STRING;
DECLARE START_HOUR STRING;
DECLARE START_DATE DATE;

IF EXISTS(
  SELECT 1
  FROM `meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR`
  WHERE site = SITE_ID
) THEN 
    SET max_ds_hour=(
      SELECT MAX(ds_hour)
      FROM `meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR`
      WHERE site = SITE_ID
    );
ELSE
    SET max_ds_hour=(
      SELECT CONCAT(CAST(CURRENT_DATE AS STRING), 'T00')
    );
END IF;

SET START_HOUR = (SELECT CONCAT(max_ds_hour, ':00:00.000-0400'))
;

SET START_DATE = (SELECT DATE(SPLIT(START_HOUR, 'T')[offset(0)]))
;

SELECT
    count(*) as prints
FROM
    meli-bi-data.MELIDATA.ADVERTISING
WHERE
    `ds` >= date(CURRENT_DATE-7)
    AND server_timestamp >= START_HOUR
    AND `event` = 'display_prints'
    AND site = SITE_ID
    AND json_extract_scalar(`event_data`, '$.content_source') = 'DSP'
    AND json_extract_scalar(event_data, '$.valid')= 'true'
    AND (NOT
        REGEXP_CONTAINS(LOWER(`device`.user_agent),
        '.*(libwww|wget|lwp|damnBot|bbbike|java|spider|crawl|slurp|bot|feedburner|googleimageproxy|google web preview).*')
    )
;
