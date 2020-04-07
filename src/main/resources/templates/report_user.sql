
SELECT date_format(from_unixtime(ts / 1000, 'UTC'), '%Y-%m-%d') AS day,
       coalesce(publisherid, 0)                                AS publisher_id,
       coalesce(pubappid, 0)                                  AS pub_app_id,
       coalesce(plat, -1)                                       AS platform,
       country,
       count(DISTINCT ip)                                       AS ip_count,
       count(DISTINCT did)                                      AS did_count,
       count(DISTINCT uid)                                      AS dau,
       count(DISTINCT if(type = 6, uid, '')) - 1                AS due

FROM [(${tableName})]
WHERE
    ts IS NOT NULL
    AND y='[(${year})]'
    AND m='[(${month})]'
    AND d='[(${day})]'
GROUP BY
    date_format(from_unixtime(ts / 1000, 'UTC'), '%Y-%m-%d'),
    coalesce(publisherid, 0),
    coalesce(pubappid, 0),
    country,
    coalesce(plat, -1)
;