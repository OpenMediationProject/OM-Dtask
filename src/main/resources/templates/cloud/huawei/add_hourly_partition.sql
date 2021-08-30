ALTER TABLE [(${tableName})] ADD IF NOT EXISTS PARTITION (y='[(${year})]', m='[(${month})]', d='[(${day})]', h='[(${hour})]') location 'obs://[(${dliBucket})]/[(${tableDataPath})]/[(${tableName})]/[(${year})]/[(${month})]/[(${day})]/[(${hour})]/';