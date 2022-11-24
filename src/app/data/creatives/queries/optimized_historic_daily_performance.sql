
WITH prints AS (
    SELECT
        `ds`,
        site,
        CAST(
            json_extract_scalar(event_data, '$.campaign_id') AS INT64
        ) AS campaign_id,
        CAST(json_extract_scalar(event_data, '$.line_item_id') AS INT64) AS line_item_id,
        CAST(SUBSTR(json_extract_scalar(event_data, '$.creative_id'), 1, 4) AS INT64) AS creative_id,
        COUNT(*) AS n_prints
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
    GROUP BY 1,2,3,4,5
),
clicks AS (
    SELECT
        `ds`,
        site,
        CAST(
            json_extract_scalar(event_data, '$.campaign_id') AS INT64
        ) AS campaign_id,
        CAST(json_extract_scalar(event_data, '$.line_item_id') AS INT64) AS line_item_id,
        CAST(SUBSTR(json_extract_scalar(event_data, '$.creative_id'), 1, 4) AS INT64) AS creative_id,
        COUNT(*) AS n_clicks
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
    GROUP BY 1,2,3,4,5
)

SELECT
    COALESCE(a.ds, b.ds) AS ds,
    COALESCE(a.site, b.site) AS site,
    COALESCE(a.campaign_id, b.campaign_id) AS campaign_id,
    COALESCE(a.line_item_id, b.line_item_id) AS line_item_id,
    COALESCE(a.creative_id, b.creative_id) AS creative_id,
    COALESCE(a.n_prints, 0) AS n_prints,
    COALESCE(b.n_clicks, 0) AS n_clicks
FROM prints a
LEFT JOIN clicks b
USING(ds, site, campaign_id, line_item_id, creative_id)
