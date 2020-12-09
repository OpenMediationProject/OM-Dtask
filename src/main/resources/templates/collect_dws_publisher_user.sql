INSERT INTO dws_publisher_user (publisher_id, pub_app_id, country, uid, is_new, ymd)
SELECT statistical_user.publisher_id,
       statistical_user.pub_app_id,
       statistical_user.country,
       statistical_user.uid,
       IF(all_user.uid IS NULL, 1, 0) AS is_new,
       '[(${year})][(${month})][(${day})]'          AS ymd
FROM (SELECT publisherId       AS publisher_Id,
             pubAppId          AS pub_app_id,
             country,
             coalesce(uid, '') AS uid
      FROM lr
      WHERE ts IS NOT NULL
        AND y='[(${year})]'
        AND m='[(${month})]'
        AND d='[(${day})]'
        AND publisherId IS NOT NULL
        AND pubAppId IS NOT NULL
        AND country IS NOT NULL
        AND uid IS NOT NULL
      GROUP BY publisherId, pubAppId, country, coalesce(uid, '')
     ) AS statistical_user
         LEFT JOIN
     (SELECT publisher_id,
             pub_app_id,
             country,
             uid
      FROM dws_publisher_user
      WHERE is_new = 1
     ) AS all_user
     ON statistical_user.publisher_id = all_user.publisher_id
         AND statistical_user.pub_app_id = all_user.pub_app_id
         AND statistical_user.country = all_user.country
         AND statistical_user.uid = all_user.uid