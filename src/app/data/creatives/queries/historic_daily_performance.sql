CREATE TABLE meli-bi-data.SBOX_DSPCREATIVOS.BQ_PRINTS_CLICKS_PER_DAY
PARTITION BY ds
CLUSTER BY site
AS

WITH prints AS (
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
        `ds` >= '2022-09-01'
        AND `ds` < '2022-11-23'
        AND `event` = 'display_prints'
        AND site = 'MLA'
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
        `ds` >= '2022-09-01'
        AND `ds` < "2022-11-23"
        AND `event` = 'display_clicks'
        AND site = 'MLA'
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
        prints.site,
        CAST(
            json_extract_scalar(prints.event_data, '$.campaign_id') AS INT64
        ) AS campaign_id,
        CAST(json_extract_scalar(prints.event_data, '$.line_item_id') AS INT64) AS line_item_id,
        CAST(SUBSTR(json_extract_scalar(prints.event_data, '$.creative_id'), 1, 4) AS INT64) AS creative_id,
        CASE
            WHEN clicks.ds IS NOT NULL THEN 1 
            ELSE 0
        END AS target
    FROM prints
    LEFT JOIN clicks
    ON prints.print_id = clicks.print_id
        AND ((prints.user_id = clicks.user_id) OR clicks.user_id IS NULL)
        AND TIMESTAMP_DIFF(clicks.ts, prints.ts, SECOND) <= 86400
        AND prints.site = clicks.site
)

SELECT
    ds,
    site,
    campaign_id,
    line_item_id,
    creative_id,
    COUNT(*) AS n_prints,
    SUM(target) AS n_clicks
FROM prints_clicks
GROUP BY 1,2,3,4,5
