-- HISTORICA

DECLARE TIME_ZONE STRING DEFAULT '-4';
DECLARE START_DATE DATE DEFAULT '2023-05-08';
DECLARE CLICK_WINDOW INT64 DEFAULT 84600;

CREATE TABLE meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_CONVERSIONS_PER_HOUR
PARTITION BY ds
CLUSTER BY site
AS

WITH campaign_strategy AS (
    SELECT DISTINCT
        CAST(campaign_id AS STRING) AS campaign_id,
        JSON_EXTRACT_SCALAR(JSON_EXTRACT_SCALAR(goal), "$.strategy") AS strategy
    FROM
        `meli-bi-data.SBOX_ADVERTISINGDISPLAY.campaign`
    WHERE
        campaign_type = 'PROGRAMMATIC'
),

prints AS (
    SELECT
        id,
        event_data,
        `ds`,
        site,
         json_extract_scalar(`event_data`, '$.print_id') AS print_id,
        `user`.`user_id` AS `user_id`,
        CAST(REPLACE(`server_timestamp`, "T", ' ') AS TIMESTAMP) AS ts
    FROM
        meli-bi-data.MELIDATA.ADVERTISING
    WHERE
        `ds` >= START_DATE
        AND `event` = 'display_prints'
        AND site IN ("MLA", "MLB", "MLC", "MCO", "MLM", "MLU", "MEC", "MPE")
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
        AND `event` = 'display_clicks'
        AND site IN ("MLA", "MLB", "MLC", "MCO", "MLM", "MLU", "MEC", "MPE")
        AND json_extract_scalar(`event_data`, '$.content_source') = 'DSP'
        AND (NOT
            REGEXP_CONTAINS(LOWER(`device`.user_agent),
            '.*(libwww|wget|lwp|damnBot|bbbike|java|spider|crawl|slurp|bot|feedburner|googleimageproxy|google web preview).*')
        )
),

conversions AS (
SELECT ds,
RIGHT(CONCAT('0', EXTRACT(HOUR FROM CAST(REPLACE(server_timestamp, "T", ' ') AS TIMESTAMP) AT TIME ZONE '-4')), 2) AS hour,
conv.site AS site_id,
JSON_VALUE(conv.event_data , '$.events[0].event_data.campaign_id') AS campaign_id,
JSON_VALUE(conv.event_data , '$.events[0].event_data.line_item_id') AS line_item_id,
JSON_VALUE(conv.event_data, '$.events[0].event_data.creative_id') AS creative_id,
COALESCE(COUNT(DISTINCT JSON_VALUE(conv.event_data , '$.conversion.order_id')), 0)  as n_conversions
FROM
meli-bi-data.MELIDATA.ADVERTISING AS conv
WHERE
conv.path = '/display/attribution/orders'
AND ds >= START_DATE
GROUP BY 1, 2, 3, 4, 5, 6
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
        AND TIMESTAMP_DIFF(clicks.ts, prints.ts, SECOND) <= CLICK_WINDOW
        AND prints.site = clicks.site
),

agg_prints_clicks AS(
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
  GROUP BY 1,2,3,4,5,6
)

SELECT a.*,
    COALESCE(c.n_conversions,0) AS n_conversions,
    s.strategy
FROM agg_prints_clicks AS a
LEFT JOIN conversions AS c
  ON a.ds = c.ds
      AND a.hour = c.hour
      AND a.site = c.site_id
      AND CAST(a.campaign_id AS INT64) = CAST(c.campaign_id AS INT64)
      AND CAST(a.line_item_id AS INT64) = CAST(c.line_item_id AS INT64)
      AND CAST(a.creative_id AS INT64) = CAST(c.creative_id AS INT64)
LEFT JOIN campaign_strategy AS s
  ON CAST(a.campaign_id AS INT64) = CAST(s.campaign_id AS INT64)
;


CREATE TABLE meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR_W_CONVERSIONS AS
SELECT site, MAX(CONCAT(ds, 'T', hour)) AS ds_hour
FROM meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_CONVERSIONS_PER_HOUR
WHERE site IN ("MLA", "MLB", "MLC", "MCO", "MLM", "MLU", "MEC", "MPE")
GROUP BY site
;

DELETE meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_CONVERSIONS_PER_HOUR a
WHERE a.site IN ("MLA", "MLB", "MLC", "MCO", "MLM", "MLU", "MEC", "MPE")
    AND CONCAT(a.ds, 'T', a.hour) = (
    SELECT MAX(hour)
    FROM meli-bi-data.SBOX_DSPCREATIVOS.BETA_ESTIMATION_LAST_DATE_HOUR_W_CONVERSIONS
    WHERE site IN ("MLA", "MLB", "MLC", "MCO", "MLM", "MLU", "MEC", "MPE")
)
;