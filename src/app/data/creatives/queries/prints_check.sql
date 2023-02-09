DECLARE SITE_ID STRING DEFAULT '{site_id}';
DECLARE TIME_ZONE STRING DEFAULT '{time_zone}';

SELECT
    count(*) as prints
FROM
    meli-bi-data.MELIDATA.ADVERTISING
WHERE
    `ds` >= date(EXTRACT(DATE FROM CURRENT_DATETIME(TIME_ZONE))-7)
    AND `event` = 'display_prints'
    AND site = SITE_ID
    AND json_extract_scalar(`event_data`, '$.content_source') = 'DSP'
    AND json_extract_scalar(event_data, '$.valid')= 'true'
    AND (NOT
        REGEXP_CONTAINS(LOWER(`device`.user_agent),
        '.*(libwww|wget|lwp|damnBot|bbbike|java|spider|crawl|slurp|bot|feedburner|googleimageproxy|google web preview).*')
    )
;
