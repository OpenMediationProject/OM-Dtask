// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.util;

import com.alibaba.fastjson.JSONObject;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.support.JdbcUtils;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JSONRowMapper implements RowMapper<JSONObject> {

    public static final JSONRowMapper INSTANCE = new JSONRowMapper(false);
    public static final JSONRowMapper INSTANCE_CAMEL = new JSONRowMapper(true);

    public static JSONRowMapper getInstance() {
        return INSTANCE;
    }

    public static JSONRowMapper getCamelInstance() {
        return INSTANCE_CAMEL;
    }

    private final boolean useCamel;

    private JSONRowMapper(boolean useCamel) {
        this.useCamel = useCamel;
    }

    @Override
    public JSONObject mapRow(ResultSet rs, int rowNum) throws SQLException {
        ResultSetMetaData rsmd = rs.getMetaData();
        int columnCount = rsmd.getColumnCount();
        JSONObject jo = new JSONObject(columnCount);
        for (int i = 1; i <= columnCount; i++) {
            String name = JdbcUtils.lookupColumnName(rsmd, i);
            if (useCamel) {
                name = JdbcUtils.convertUnderscoreNameToPropertyName(name);
            }
            jo.putIfAbsent(name, JdbcUtils.getResultSetValue(rs, i));
        }
        return jo;
    }

}
