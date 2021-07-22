SELECT pubAppId,
       country,
       '[(${year})][(${month})][(${day})]' as day,
           CAST(SUM(IF(runk = floor(max_runk * 0.1), bidwinprice, 0))/1000 AS DECIMAL(10,8)) AS top10_price,
           CAST(SUM(IF(runk = floor(max_runk * 0.2), bidwinprice, 0))/1000 AS DECIMAL(10,8)) AS top20_price,
           CAST(SUM(IF(runk = floor(max_runk * 0.3), bidwinprice, 0))/1000 AS DECIMAL(10,8)) AS top30_price,
           CAST(SUM(IF(runk = floor(max_runk * 0.4), bidwinprice, 0))/1000 AS DECIMAL(10,8)) AS top40_price,
           CAST(SUM(IF(runk = floor(max_runk * 0.5), bidwinprice, 0))/1000 AS DECIMAL(10,8)) AS top50_price
FROM (SELECT pubAppId,
    country,
    did,
    bidwinprice,
    ROW_NUMBER() OVER (PARTITION BY pubAppId, country ORDER BY bidwinprice DESC) AS runk,
    COUNT() OVER (PARTITION BY pubAppId, country )                         AS max_runk
    FROM (
        SELECT pubAppId, country, did, SUM(bidwinprice) AS bidwinprice
        FROM lr
        WHERE serverTs IS NOT NULL
        AND y='[(${year})]'
        AND m='[(${month})]'
        AND d='[(${day})]'
        AND bidwinprice > 0
        GROUP BY pubAppId, country, did
    )) as a
WHERE max_runk > 10
GROUP BY pubAppId,country;