// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.util.Util;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Kafka Producer
 */
@Service
public class KafkaService {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplateW;

    private Map<Integer, Producer<String, String>> dcProducers;
    @PostConstruct
    private void init() {
        String sql = "select id,name,kafka_status,kafka_servers,redis_servers," +
                "s3_status,s3_region,s3_bucket,s3_access_key_id,s3_secret_access_key" +
                " from om_server_dcenter where kafka_status=1";
        List<Map<String, Object>> list = jdbcTemplateW.queryForList(sql);
        if (!list.isEmpty()) {
            List<String> kafkaServers = list.stream().map(o-> Util.getString(o, "kafka_servers"))
                    .filter(StringUtils::isNoneBlank).collect(Collectors.toList());
            if (!kafkaServers.isEmpty()) {
                Map<String, Object> props = new HashMap<>();
                props.put(ProducerConfig.ACKS_CONFIG, "all");
                props.put(ProducerConfig.RETRIES_CONFIG, 3);
                props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
                props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
                props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
                props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60 * 1000);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
                dcProducers = new HashMap<>(list.size());
                for (Map<String, Object> dc : list) {
                    String kafkaServer = Util.getString(dc, "kafka_servers");
                    if (StringUtils.isBlank(kafkaServer)) continue;
                    int dcenter = Util.getInt(dc, "id");
                    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
                    Producer<String, String> producer = new KafkaProducer<>(props);
                    dcProducers.put(dcenter, producer);
                    LOG.info("init kafka producer, servers: {}", kafkaServers);
                }
            }
        }
    }

    public Producer<String, String> getProducerByDcenter(int dc) {
        if (dcProducers == null)  return null;
        return dcProducers.get(dc);
    }

    public Map<Integer, Producer<String, String>> getProducers() {
        return dcProducers;
    }

    public boolean isEnabled() {
        return dcProducers != null;
    }

}
