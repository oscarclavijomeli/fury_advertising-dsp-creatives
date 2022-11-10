WITH prints AS (
    SELECT
        `id`,
        `ds`,
        `event_data`,
        get_json_object(`event_data`, '$.content_source') AS content_source,
        get_json_object(`event_data`, '$.print_id') AS print_id,
        `user`.`user_id` AS `user_id`,
        unix_timestamp(replace(`server_timestamp`, "T", ' '), "yyyy-MM-dd HH:mm:ss.SSSZ") AS ts
    FROM
        advertising.adv_lake_raw
    WHERE
        `ds` >= '{start_date}'
        AND `ds` < '{end_date}'
        AND `site` = '{site}'
        AND `event` = 'display_prints'
        AND (
            LOWER(`device`.user_agent) NOT RLIKE '.*(libwww|wget|lwp|damnBot|bbbike|java|spider|crawl|slurp|bot|feedburner|googleimageproxy|google web preview).*'
        )
),
clicks AS (
    SELECT
        `ds`,
        `event`,
        `user`.`user_id` AS `user_id`,
        get_json_object(`event_data`, '$.print_id') AS print_id,
        get_json_object(`event_data`, '$.click_id') AS click_id,
        unix_timestamp(replace(`server_timestamp`, "T", ' '), "yyyy-MM-dd HH:mm:ss.SSSZ") AS ts
    FROM
        advertising.adv_lake_raw
    WHERE
        `ds` >= '{start_date}'
        AND `ds` < "{end_date}"
        AND `site` = '{site}'
        AND `event` = 'display_clicks'
        AND (
            LOWER(`device`.user_agent) NOT RLIKE '.*(libwww|wget|lwp|damnBot|bbbike|java|spider|crawl|slurp|bot|feedburner|googleimageproxy|google web preview).*'
        )
),
prints_clicks AS (
    SELECT
        prints.id AS id,
        SUBSTRING(prints.ds, 1, 10) AS `cday`,
        CAST(SUBSTRING(prints.ds, 12) AS INT) AS `chour`,
        prints.content_source,
        CAST(
            get_json_object(prints.event_data, '$.campaign_id') AS INT
        ) AS campaign_id,
        get_json_object(prints.event_data, '$.line_item_id') AS line_item_id,
        get_json_object(prints.event_data, '$.creative_id') AS creative_id,
        CASE
            WHEN clicks.ds IS NOT NULL THEN 1 
            ELSE 0
        END AS target
    FROM prints
    LEFT JOIN clicks
    ON prints.print_id = clicks.print_id
        AND ((prints.user_id = clicks.user_id) OR clicks.user_id IS NULL)
        AND clicks.ts - prints.ts <= {click_window}
)

SELECT
    cday,
    chour,
    content_source,
    campaign_id,
    line_item_id,
    creative_id,
    SUM(target) AS n_clicks,
    COUNT(*) AS n_prints
FROM prints_clicks
GROUP BY 1,2,3,4,5,6
