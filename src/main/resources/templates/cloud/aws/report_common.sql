SELECT date_format(from_unixtime(serverTs / 1000, 'UTC'), '%Y-%m-%d')                       AS day,
       date_format(from_unixtime(serverTs / 1000, 'UTC'), '%H')                             AS hour,
       country,
       coalesce(plat, -1)                                                                   AS platform,
       coalesce(publisherId, 0)                                                             AS publisher_id,
       coalesce(pubAppId, 0)                                                                AS pub_app_id,
       coalesce(pid, 0)                                                                     AS placement_id,
       coalesce(iid, 0)                                                                     AS instance_id,
       coalesce(scene, 0)                                                                   AS scene_id,
       coalesce(mid, 0)                                                                     AS adn_id,
       coalesce(abt, 0)                                                                     AS abt,
       coalesce(bid, 0)                                                                     AS bid,
       appv                                                                                 AS app_version,
       sdkv                                                                                 AS sdk_version,
       osv                                                                                  AS os_version,

       sum(if(type = 2 OR wfReq > 0, coalesce(wfReq, 1), 0))                                AS waterfall_request,
       sum(if(type = 3 OR wfFil > 0, coalesce(wfFil, 1), 0))                                AS waterfall_filled,
       sum(if(type = 4 OR insReq > 0, coalesce(insReq, 1), 0))                              AS instance_request,
       sum(if(type = 5 OR insFil > 0, coalesce(insFil, 1), 0))                              AS instance_filled,
       sum(if(type = 8 OR vdStart > 0, coalesce(vdStart, 1), 0))                            AS video_start,
       sum(if(type = 9 OR vdEnd > 0, coalesce(vdEnd, 1), 0))                                AS video_complete,
       sum(if(type = 501 OR calledShow > 0, coalesce(calledShow, 1), 0))                    AS called_show,
       sum(if(type = 502 OR readyTrue > 0, coalesce(readyTrue, 1), 0))                      AS is_ready_true,
       sum(if(type = 503 OR readyFalse > 0, coalesce(readyFalse, 1), 0))                    AS is_ready_false,
       sum(if(type = 7 OR click > 0, coalesce(click, 1), 0))                                AS click,
       sum(if(type = 6 OR impr > 0, coalesce(impr, 1), 0))                                  AS impr,
       sum(if(type = 270 OR bidReq > 0, coalesce(bidReq, 1), 0))                            AS bid_req,
       sum(if(type = 271 OR bidRes > 0, coalesce(bidRes, 1), 0))                            AS bid_resp,
       sum(if(type = 271 OR bidResPrice > 0, coalesce(bidResPrice, coalesce(price, 0)), 0)) AS bid_resp_price,
       sum(if(type = 273 OR bidWin > 0, coalesce(bidWin, 1), 0))                            AS bid_win,
       sum(if(type = 273 OR bidWinPrice > 0, coalesce(bidWinPrice, coalesce(price, 0)), 0)) AS bid_win_price
FROM lr
WHERE serverTs IS NOT NULL
  AND y = '[(${year})]'
  AND m = '[(${month})]'
  AND d = '[(${day})]'
  AND h = '[(${hour})]'
GROUP BY date_format(from_unixtime(serverTs / 1000, 'UTC'), '%Y-%m-%d'),
         date_format(from_unixtime(serverTs / 1000, 'UTC'), '%H'),
         country,
         coalesce(plat, -1),
         coalesce(publisherId, 0),
         coalesce(pubAppId, 0),
         coalesce(pid, 0),
         coalesce(iid, 0),
         coalesce(scene, 0),
         coalesce(mid, 0),
         coalesce(abt, 0),
         coalesce(bid, 0),
         appv,
         sdkv,
         osv
;
