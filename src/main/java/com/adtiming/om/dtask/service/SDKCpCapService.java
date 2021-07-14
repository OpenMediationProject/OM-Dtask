package com.adtiming.om.dtask.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.Resource;
import java.io.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@Service
public class SDKCpCapService {
    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplateW;

    @Resource
    private KafkaService kafkaService;

    @Scheduled(cron = "1 */10 * * * ?")
    private void pushCpCapToDcenter() {
        if (cfg.isDev())
            return;
        long start = System.currentTimeMillis();
        LOG.info("load cp campaign daily_cap start");
        try {
            // language=MySQL
            String sql = "SELECT id,publisher_id,daily_cap from cp_campaign where status=1 and run_status=1 and daily_cap>0";
            Set<Long> cids = new HashSet<>();
            Map<Long, Integer> newCampaignCap = new HashMap<>();
            Map<Long, Integer> campaignPublisher = new HashMap<>();
            jdbcTemplateW.query(sql, rs -> {
                long cid = rs.getLong("id");
                int cap = rs.getInt("daily_cap");
                cids.add(cid);
                newCampaignCap.put(cid, cap);
                campaignPublisher.put(cid, rs.getInt("publisher_id"));
            });
            if (newCampaignCap.isEmpty()) {
                LOG.info("load cp campaign daily_cap end, cost:{}, empty campaign", System.currentTimeMillis() - start);
                return;
            }

            Map<Long, Integer> campaignChangeCap = new HashMap<>();
            Map<Long, Integer> oldCampaignImprCap = loadOldCpCampaignCap();
            for (Map.Entry<Long, Integer> map : newCampaignCap.entrySet()) {
                long cid = map.getKey();
                int cap = map.getValue();
                Integer oldCap = oldCampaignImprCap.get(cid);
                if (oldCap != null) {
                    if (cap != oldCap) {
                        campaignChangeCap.put(cid, cap - oldCap);
                    }
                } else {
                    campaignChangeCap.put(cid, cap);
                }
            }
            if (campaignChangeCap.isEmpty()) {
                LOG.info("load cp campaign cap end,cost:{}, campaign cap has no change", System.currentTimeMillis() - start);
                return;
            }
            Map<Integer, Producer<String, String>> producerMap = kafkaService.getProducers();
            if (CollectionUtils.isEmpty(producerMap)) {
                LOG.info("loss kafka producer configuration, cost:{}", System.currentTimeMillis() - start);
                return;
            }

            campaignChangeCap.forEach((campaignId, changeCap) -> {
                Integer pubId = campaignPublisher.get(campaignId);
                if (pubId == null) return;

                producerMap.forEach((dc, producer) -> {
                    Object[] msg = {campaignId, changeCap};
                    String topic = "cp_campaign_cap" + dc;
                    producer.send(new ProducerRecord<>(topic, StringUtils.join(msg, '\1')));
                });
            });

            saveCampaignCap(newCampaignCap);
            LOG.info("load cp campaign cap end,cost:{}", System.currentTimeMillis() - start);
        } catch (Exception e) {
            LOG.error("loadCpCampaignCap error:", e);
        }
    }

    private void saveCampaignCap(Map<Long, Integer> campaign_cap) {
        String day = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        try (ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream("data/cp_campaign_impr_cap_" + day + ".jobj"))) {
            out.writeObject(campaign_cap);
        } catch (IOException e) {
            LOG.error("save campaign sdk impr cap error", e);
        }
    }

    private Map<Long, Integer> loadOldCpCampaignCap() {
        String day = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File file = new File("data/cp_campaign_impr_cap_" + day + ".jobj");
        if (file.exists()) {
            try (ObjectInputStream in = new ObjectInputStream(new FileInputStream(file))) {
                return (Map<Long, Integer>) in.readObject();
            } catch (Exception e) {
                LOG.error("loadOldCpCampaignCap error", e);
            }
        }
        return Collections.emptyMap();
    }
}
