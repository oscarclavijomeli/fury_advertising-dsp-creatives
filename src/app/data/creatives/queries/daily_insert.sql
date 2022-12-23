DECLARE SITE_ID STRING DEFAULT '{site_id}';
DECLARE TIME_ZONE STRING DEFAULT '{time_zone}';
DECLARE max_ds_hour STRING;
DECLARE START_HOUR STRING;
DECLARE START_DATE DATE;

SET max_ds_hour=(
  SELECT MAX(ds_hour)
  FROM `meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR`
  WHERE site = SITE_ID
);

SET START_HOUR = (SELECT CONCAT(max_ds_hour, ':00:00.000-0400'))
;

SET START_DATE = (SELECT DATE(SPLIT(START_HOUR, 'T')[offset(0)]))
;

INSERT INTO meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_PER_HOUR

WITH prints AS (
    SELECT
        id,
        event_data,
        `ds`,
        server_timestamp,
        site,
         json_extract_scalar(`event_data`, '$.print_id') AS print_id,
        `user`.`user_id` AS `user_id`,
        CAST(REPLACE(`server_timestamp`, "T", ' ') AS TIMESTAMP) AS ts
    FROM
        meli-bi-data.MELIDATA.ADVERTISING
    WHERE
        `ds` >= START_DATE
        AND server_timestamp >= START_HOUR
        AND `event` = 'display_prints'
        AND site = SITE_ID
        AND json_extract_scalar(`event_data`, '$.content_source') = 'DSP'
        AND json_extract_scalar(event_data, '$.valid')= 'true'
        AND (NOT
            REGEXP_CONTAINS(LOWER(`device`.user_agent),
            '.*(libwww|wget|lwp|damnBot|bbbike|java|spider|crawl|slurp|bot|feedburner|googleimageproxy|google web preview).*')
        )
),

clicks AS (
    SELECT
        `ds`,
        site,
        `user`.`user_id` AS `user_id`,
        json_extract_scalar(`event_data`, '$.print_id') AS print_id,
        json_extract_scalar(`event_data`, '$.click_id') AS click_id,
        CAST(REPLACE(`server_timestamp`, "T", ' ') AS TIMESTAMP) AS ts
    FROM
        meli-bi-data.MELIDATA.ADVERTISING
    WHERE
        `ds` >= START_DATE
        AND server_timestamp >= START_HOUR
        AND `event` = 'display_clicks'
        AND site = SITE_ID
        AND json_extract_scalar(`event_data`, '$.content_source') = 'DSP'
        AND (NOT
            REGEXP_CONTAINS(LOWER(`device`.user_agent),
            '.*(libwww|wget|lwp|damnBot|bbbike|java|spider|crawl|slurp|bot|feedburner|googleimageproxy|google web preview).*')
        )
),
prints_clicks AS (
    SELECT DISTINCT
        prints.id AS id,
        prints.ds AS ds,
        RIGHT(CONCAT('0', EXTRACT(HOUR FROM prints.ts AT TIME ZONE TIME_ZONE)), 2) AS hour,
        prints.site,
        CAST(
            json_extract_scalar(prints.event_data, '$.campaign_id') AS INT64
        ) AS campaign_id,
        CAST(json_extract_scalar(prints.event_data, '$.line_item_id') AS INT64) AS line_item_id,
        CAST(json_extract_scalar(prints.event_data, '$.creative_id') AS INT64) AS creative_id,
        CASE
            WHEN clicks.ds IS NOT NULL THEN 1 
            ELSE 0
        END AS target
    FROM prints
    LEFT JOIN clicks
    ON prints.print_id = clicks.print_id
        AND ((prints.user_id = clicks.user_id) OR clicks.user_id IS NULL)
        AND TIMESTAMP_DIFF(clicks.ts, prints.ts, SECOND) <= {click_window}
        AND prints.site = clicks.site
)

SELECT
    ds,
    hour,
    site,
    campaign_id,
    line_item_id,
    creative_id,
    CAST(NULL AS STRING) AS sample_type,
    COUNT(*) AS n_prints,
    SUM(target) AS n_clicks
FROM prints_clicks
GROUP BY 1,2,3,4,5,6,7
;

INSERT INTO meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR
SELECT SITE_ID, MAX(CONCAT(ds, 'T', hour))
FROM meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_PER_HOUR
WHERE site = SITE_ID
;

DELETE meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_PER_HOUR a
WHERE a.site = SITE_ID AND CONCAT(a.ds, 'T', a.hour) = (
    SELECT MAX(ds_hour)
    FROM meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR
    WHERE site = SITE_ID
)
;