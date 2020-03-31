ALTER TABLE [(${tableName})] ADD IF NOT EXISTS PARTITION (y='[(${year})]', m='[(${month})]', d='[(${day})]', h='[(${hour})]') location 's3://[(${s3Bucket})]/[(${tableDataPath})]/[(${tableName})]/[(${year})]/[(${month})]/[(${day})]/[(${hour})]/';