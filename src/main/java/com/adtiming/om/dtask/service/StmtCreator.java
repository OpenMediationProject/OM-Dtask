package com.adtiming.om.dtask.service;

import org.springframework.jdbc.core.ArgumentPreparedStatementSetter;
import org.springframework.jdbc.core.PreparedStatementCreator;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class StmtCreator implements PreparedStatementCreator {
    private final String sql;
    private final Object[] args;

    public StmtCreator(String sql, Object... args) {
        this.sql = sql;
        this.args = args;
    }

    @Override
    public PreparedStatement createPreparedStatement(Connection con) throws SQLException {
        PreparedStatement stmt = con.prepareStatement(sql,
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
        stmt.setFetchDirection(ResultSet.FETCH_FORWARD);
        stmt.setFetchSize(10000);
        if (args != null && args.length > 0)
            new ArgumentPreparedStatementSetter(args).setValues(stmt);
        return stmt;
    }
}