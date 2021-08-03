// Copyright 2021 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.web;

import com.adtiming.om.dtask.util.JSONRowMapper;
import com.adtiming.om.dtask.util.Util;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;

@RestController
public class APIController {

    private static final Logger log = LogManager.getLogger();

    @Resource
    private JdbcTemplate jdbcTemplate;

    /**
     * used for om-server init
     *
     * @return snode config
     */
    @GetMapping("/snode/config/get")
    public ResponseEntity<?> getSnodeConfig(
            HttpServletRequest req,
            @RequestParam("nodeid") String nodeid,
            @RequestParam("dcenter") int dcenter, @RequestParam(value = "nc", defaultValue = "1") int needCreateNode) {
        log.debug("snode/config/get, nodeid: {}, dcenter: {}", nodeid, dcenter);
        String sql = "select id,name,kafka_status,kafka_servers,redis_servers,cloud_type,cloud_config" +
                " from om_server_dcenter where id=?";
        JSONObject o = jdbcTemplate.queryForObject(sql, JSONRowMapper.getCamelInstance(), dcenter);
        o.put("dcenter", dcenter);
        if (o.getIntValue("kafkaStatus") == 0) {
            o.remove("kafkaServers");
        }
        if (StringUtils.isEmpty(o.getString("cloudType"))) {
            o.remove("cloudConfig");
        }

        if (needCreateNode == 1) {
            sql = "select id from om_server_node where nodeid=?";
            List<Integer> existsId = jdbcTemplate.queryForList(sql, Integer.class, nodeid);
            if (existsId.isEmpty()) {
                KeyHolder keyHolder = new GeneratedKeyHolder();
                jdbcTemplate.update(conn -> {
                    // language=MySQL
                    String insertSql = "insert into om_server_node (nodeid, dcenter, ip) values (?,?,?)";
                    PreparedStatement ps = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
                    ps.setString(1, nodeid);
                    ps.setInt(2, dcenter);
                    ps.setString(3, Util.getClientIP(req));
                    return ps;
                }, keyHolder);
                o.put("id", keyHolder.getKey().intValue());
            } else {
                o.put("id", existsId.get(0));
            }
        }

        return ResponseEntity.ok(o);
    }

    /**
     * used for om-server init
     *
     * @return snode config
     */
    @GetMapping("/snode/config/list")
    public ResponseEntity<?> getSnodeConfigs(
            HttpServletRequest req,
            @RequestParam("nodeid") String nodeid,
            @RequestParam("dcenter") int dcenter, @RequestParam(value = "nc", defaultValue = "1") int needCreateNode) {
        log.debug("snode/config/list, nodeid: {}", nodeid);
        String sql = "select id dcenter,name,kafka_status,kafka_servers,redis_servers,cloud_type,cloud_config" +
                " from om_server_dcenter";
        List<JSONObject> list = jdbcTemplate.query(sql, JSONRowMapper.getCamelInstance());
        JSONObject o = new JSONObject();
        if (!list.isEmpty()) {
            for (JSONObject dcMap : list) {
                int kafkaStatus = dcMap.getIntValue("kafkaStatus");
                if (kafkaStatus == 0) {
                    dcMap.remove("kafkaServers");
                }
                String cloudType = dcMap.getString("cloudType");
                if (StringUtils.isEmpty(cloudType)) {
                    dcMap.remove("cloudConfig");
                }
            }
            o.put("data", list);
        }
        if (needCreateNode == 1) {
            sql = "select id from om_server_node where nodeid=?";
            List<Integer> existsId = jdbcTemplate.queryForList(sql, Integer.class, nodeid);
            if (existsId.isEmpty()) {
                KeyHolder keyHolder = new GeneratedKeyHolder();
                jdbcTemplate.update(conn -> {
                    // language=MySQL
                    String insertSql = "insert into om_server_node (nodeid, dcenter, ip) values (?,?,?)";
                    PreparedStatement ps = conn.prepareStatement(insertSql, Statement.RETURN_GENERATED_KEYS);
                    ps.setString(1, nodeid);
                    ps.setInt(2, dcenter);
                    ps.setString(3, Util.getClientIP(req));
                    return ps;
                }, keyHolder);
                o.put("id", keyHolder.getKey().intValue());
            } else {
                o.put("id", existsId.get(0));
            }
        }
        return ResponseEntity.ok(o);
    }

}
