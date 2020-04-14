SELECT date_format(from_unixtime(ts / 1000,'UTC+8'), '%Y-%m-%d')  AS day,
       date_format(from_unixtime(ts / 1000, 'UTC+8'), '%H') AS hour,
       country,
       coalesce(plat, -1)                                 AS platform,
       coalesce(publisher_id, 0)                          AS publisher_id,
       coalesce(pub_app_id,0)                             AS pub_app_id,
       coalesce(pid,0)                                    AS placement_id,
       coalesce(iid,0)                                    AS instance_id,
       coalesce(mid,0)                                    AS adn_id,
       coalesce(abt,0)                                    AS abt,
       sum(if(type = 2, 1, 0))                            AS waterfall_request,
       sum(if(type = 3, 1, 0))                            AS waterfall_filled,
       sum(if(type = 4, 1, 0))                            AS instance_request,
       sum(if(type = 5, 1, 0))                            AS instance_filled,
       sum(if(type = 8, 1, 0))                            AS video_start,
       sum(if(type = 9, 1, 0))                            AS video_complete,
       sum(if(type = 501, 1, 0))                          AS called_show,
       sum(if(type = 502, 1, 0))                          AS is_ready_true,
       sum(if(type = 503, 1, 0))                          AS is_ready_false,
       sum(if(type = 7, 1, 0))                            AS click,
       sum(if(type = 6, 1, 0))                            AS impr
FROM [(${tableName})]
WHERE
    ts IS NOT NULL
    AND y='[(${year})]'
    AND m='[(${month})]'
    AND d='[(${day})]'
    AND h='[(${hour})]'
GROUP BY
    date_format(from_unixtime(ts / 1000, 'UTC+8'), '%Y-%m-%d'),
    date_format(from_unixtime(ts / 1000, 'UTC+8'), '%H'),
    country,
    coalesce(plat, -1),
    coalesce(publisher_id, 0),
    coalesce(pub_app_id,0),
    coalesce(pid,0),
    coalesce(iid,0),
    coalesce(mid,0),
    coalesce(abt,0)
;