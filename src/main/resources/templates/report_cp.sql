SELECT date_format(from_unixtime(ts / 1000,'UTC'), '%Y-%m-%d')  AS day,
       date_format(from_unixtime(ts / 1000, 'UTC'), '%H')       AS hour,
       coalesce(publisherId, 0)                                 AS publisher_id,
       coalesce(pubAppId, 0)                                    AS pub_app_id,
       coalesce(pid, 0)                                         AS placement_id,
       coalesce(cy, '')                                         AS country,
       coalesce(appId, '')                                      AS app_id,
       coalesce(cid, 0)                                         AS campaign_id,
       coalesce(crid, 0)                                        AS creative_id,

       sum(coalesce(impr, 0))                                   AS impr,
       sum(coalesce(click, 0))                                  AS click,
       sum(coalesce(cost, 0))                                   AS win_price
FROM cptk
WHERE
    ts IS NOT NULL
    AND y='[(${year})]'
    AND m='[(${month})]'
    AND d='[(${day})]'
    AND h='[(${hour})]'
GROUP BY
    date_format(from_unixtime(ts / 1000,'UTC'), '%Y-%m-%d'),
    date_format(from_unixtime(ts / 1000, 'UTC'), '%H'),
    coalesce(publisherId, 0),
    coalesce(pubAppId, 0),
    coalesce(pid, 0),
    coalesce(cy, ''),
    coalesce(appId, ''),
    coalesce(cid, 0),
    coalesce(crid, 0)
;