// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.util.RangeExp;
import com.adtiming.om.pb.AdNetworkPB;
import com.adtiming.om.pb.DevPB;
import com.adtiming.om.pb.PlacementPB;
import com.adtiming.om.pb.PubAppPB;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.math.BigDecimal;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Component
public class SDKCacheBuilder extends PbBuiler {

    private static final Logger log = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplate;

    private final String MODEL_SPLIT_STR = "\n";

    @Scheduled(fixedDelay = 60000)
    private void buildCache() {
        if (cfg.isDev()) {
            return;
        }
        long start = System.currentTimeMillis();
        log.debug("build sdk cache start");

        buildPublisherApp();
        buildPlacement();
        buildAdNetowrk();
        buildAdNetowrkApp();
        buildInstance();
        buildInstanceRule();

        buildSdkDevApp();
        buildSdkDevDevice();

        buildAbTest();
        log.debug("build sdk cache finished, cost:{} ms", System.currentTimeMillis() - start);
    }

    void buildPublisherApp() {
        build("om_publisher_app", cfg.dir, out -> {
            String sql = "select id,name,pub_app_id,sdk_version,app_version,osv_max,osv_min," +
                    "make_device_blacklist,brand_model_blacklist from om_publisher_app_block_rule where status=1";
            Map<Integer, List<PubAppPB.PublisherAppBlockRule>> appBlocRule = new HashMap<>();
            jdbcTemplate.query(sql, rs -> {
                PubAppPB.PublisherAppBlockRule.Builder rule = PubAppPB.PublisherAppBlockRule.newBuilder()
                        .setId(rs.getInt("id"))
                        .setPubAppId(rs.getInt("pub_app_id"))
                        .setSdkVersion(rs.getString("sdk_version"))
                        .setAppVersion(rs.getString("app_version"))
                        .setOsvMax(rs.getString("osv_max"))
                        .setOsvMin(rs.getString("osv_min"));
                String makeDeviceBlacklist = rs.getString("make_device_blacklist");
                if (StringUtils.isNoneBlank(makeDeviceBlacklist)) {
                    rule.addAllMakeDeviceBlacklist(Arrays.stream(makeDeviceBlacklist.split("[,\n]"))
                            .filter(StringUtils::isNotBlank)
                            .map(o -> o.trim().toLowerCase())
                            .collect(Collectors.toList()));
                }
                String brandModelBlacklist = rs.getString("brand_model_blacklist");
                if (StringUtils.isNoneBlank(brandModelBlacklist)) {
                    rule.addAllBrandModelBlacklist(Arrays.stream(brandModelBlacklist.split(MODEL_SPLIT_STR))
                            .filter(StringUtils::isNotBlank)
                            .map(o -> o.trim().toLowerCase())
                            .collect(Collectors.toList()));
                }
                appBlocRule.computeIfAbsent(rs.getInt("pub_app_id"), k -> new ArrayList<>()).add(rule.build());
            });

            /*sql = "select id,name,pub_app_id,frequency,con_type,brand,model,gender,interest,iap_min,iap_max" +
                    " from om_segment where status=1 order by priority desc";
            Map<Integer, List<PubAppPB.Segment>> appSegments = new HashMap<>();
            jdbcTemplate.query(sql, rs -> {
                PubAppPB.Segment.Builder seg = PubAppPB.Segment.newBuilder()
                        .setId(rs.getInt("id"))
                        .setPubAppId(rs.getInt("pub_app_id"))
                        .setFrequency(rs.getInt("frequency"))
                        .setBrand(rs.getString("brand"))
                        .setModel(rs.getString("model"))
                        .setIapMin(rs.getFloat("iap_min"))
                        .setIapMax(rs.getFloat("iap_max"));
                appSegments.computeIfAbsent(rs.getInt("pub_app_id"), k -> new ArrayList<>()).add(seg.build());
            });*/

            sql = "SELECT a.id,a.publisher_id,a.plat,a.app_name,a.app_key,a.bundle_id,a.create_time,a.sdk_event_ids," +
                    "b.impr_callback_switch,a.sdk_report_uar_regions,a.sdk_report_uar_manual,a.sdk_report_uar_auto" +
                    " FROM om_publisher_app a" +
                    " LEFT JOIN om_publisher b ON b.id=a.publisher_id" +
                    " WHERE a.status=1 AND b.status=1";
            jdbcTemplate.query(sql, rs -> {
                int pubAppId = rs.getInt("id");
                String sdkEventIds = rs.getString("sdk_event_ids");
                PubAppPB.PublisherApp.Builder pb = PubAppPB.PublisherApp.newBuilder()
                        .setId(pubAppId)
                        .setPublisherId(rs.getInt("publisher_id"))
                        .setPlat(rs.getInt("plat"))
                        .setAppName(rs.getString("app_name"))
                        .setAppKey(StringUtils.defaultIfEmpty(rs.getString("app_key"), ""))
                        .setBundleId(StringUtils.defaultIfEmpty(rs.getString("bundle_id"), ""))
                        .setCreateTime((int) (rs.getTimestamp("create_time").getTime() / 1000))
                        .setImprCallbackSwitch(rs.getInt("impr_callback_switch"))
                        .addAllBlockRules(appBlocRule.getOrDefault(pubAppId, Collections.emptyList()));
                //.addAllSegments(appSegments.getOrDefault(pubAppId, Collections.emptyList()));
                if (StringUtils.isNoneBlank(sdkEventIds)) {
                    pb.addAllEventIds(Arrays.stream(sdkEventIds.split(","))
                            .map(NumberUtils::toInt).collect(Collectors.toList()));
                }
                String sdkReportUarRegions = rs.getString("sdk_report_uar_regions");
                String sdkReportUarManual = rs.getString("sdk_report_uar_manual");
                String sdkReportUarAuto = rs.getString("sdk_report_uar_auto");
                if (StringUtils.isNotBlank(sdkReportUarRegions)) {
                    Set<String> regions = new HashSet<>(str2list(sdkReportUarRegions, ","));
                    if (StringUtils.isNotBlank(sdkReportUarManual)) {
                        try {
                            JSONObject o = JSON.parseObject(sdkReportUarManual);
                            for (String country : o.keySet()) {
                                if (!regions.contains(country)) {
                                    continue;
                                }
                                JSONArray uarx = o.getJSONArray(country);
                                PubAppPB.PublisherApp.CountryUar.Builder cupb =
                                        PubAppPB.PublisherApp.CountryUar.newBuilder().setCountry(country);
                                for (Object uar : uarx) {
                                    cupb.addUarx(TypeUtils.castToFloat(uar));
                                }
                                pb.addCountryUars(cupb.build());
                            }
                        } catch (Exception e) {
                            log.error("parse json sdkReportUarManual error", e);
                        }
                    } else {
                        if (StringUtils.isNotBlank(sdkReportUarAuto)) {
                            try {
                                JSONObject o = JSON.parseObject(sdkReportUarAuto);
                                for (String country : o.keySet()) {
                                    if (!regions.contains(country)) {
                                        continue;
                                    }
                                    JSONArray uarx = o.getJSONArray(country);
                                    PubAppPB.PublisherApp.CountryUar.Builder cupb =
                                            PubAppPB.PublisherApp.CountryUar.newBuilder().setCountry(country);
                                    for (Object uar : uarx) {
                                        cupb.addUarx(TypeUtils.castToFloat(uar));
                                    }
                                    pb.addCountryUars(cupb.build());
                                }
                            } catch (Exception e) {
                                log.error("parse json sdkReportUarAuto error", e);
                            }
                        }
                    }
                }
                out.writeDelimited(pb.build());
            });
        });
    }

    void buildPlacement() {
        build("om_placement", cfg.dir, w -> {
            Map<Integer, List<PlacementPB.Scene>> sceneMap = new HashMap<>();
            String sql = "select a.id,a.name,a.placement_id,a.frequency_cap,a.frequency_unit,a.is_default" +
                    " from om_placement_scene a" +
                    " left join om_placement b on (a.placement_id=b.id)" +
                    " where a.status=1 and b.status=1";
            jdbcTemplate.query(sql, rs -> {
                int placementId = rs.getInt("placement_id");
                PlacementPB.Scene.Builder scene = PlacementPB.Scene.newBuilder()
                        .setId(rs.getInt("id"))
                        .setName(rs.getString("name"))
                        .setPlacementId(rs.getInt("placement_id"))
                        .setFrequencyCap(rs.getInt("frequency_cap"))
                        .setFrequencyUnit(rs.getInt("frequency_unit"))
                        .setIsDefault(rs.getInt("is_default"));
                sceneMap.computeIfAbsent(placementId, k -> new ArrayList<>()).add(scene.build());
            });

            Map<Integer, Map<String, PlacementPB.Placement.CountrySettings>> countrySettings = new HashMap<>();
            sql = "select a.placement_id,a.country,a.floor_price_switch,a.floor_price,a.max_price," +
                    "a.hour0,a.hour1,a.hour2,a.hour3,a.hour4,a.hour5,a.hour6" +
                    " from om_placement_country a" +
                    " left join om_placement b on b.id=a.placement_id" +
                    " left join om_publisher_app c on c.id=b.pub_app_id" +
                    " left join om_publisher d on d.id=b.publisher_id" +
                    " where b.status=1 and c.status=1 and d.status=1";
            jdbcTemplate.query(sql, rs -> {
                PlacementPB.Placement.CountrySettings.Builder cs = PlacementPB.Placement.CountrySettings.newBuilder();
                if (rs.getInt("floor_price_switch") == 0) {
                    cs.setFloorPrice(-1F).setMaxPrice(-1F);
                } else {
                    BigDecimal floorPrice = rs.getBigDecimal("floor_price");
                    BigDecimal maxPrice = rs.getBigDecimal("max_price");
                    cs.setFloorPrice(floorPrice == null ? -1F : floorPrice.floatValue());
                    cs.setMaxPrice(maxPrice == null ? -1F : maxPrice.floatValue());
                }
                for (int i = 0; i < 7; i++) {
                    int hour = rs.getInt("hour" + i);
                    if (hour > 0) cs.putPeriod(i, hour);
                }
                countrySettings
                        .computeIfAbsent(rs.getInt("placement_id"), k -> new HashMap<>())
                        .put(rs.getString("country"), cs.build());
            });

            final Map<Integer, Integer> defaultInventoryIntervalStep = new HashMap<>();
            defaultInventoryIntervalStep.put(5, 30);
            defaultInventoryIntervalStep.put(8, 300);
            defaultInventoryIntervalStep.put(10, 3600);
            final Pattern regIis = Pattern.compile("(\\d+):(\\d+)");

            sql = "SELECT a.id,a.publisher_id,a.pub_app_id,a.ad_type,b.plat,b.app_id,a.floor_price_switch,a.floor_price," +
                    "a.main_placement,a.ic_url,a.hb_status,a.batch_size,a.preload_timeout,a.fan_out," +
                    "a.inventory_count,a.inventory_interval,a.inventory_interval_step,a.reload_interval," +
                    "a.frequency_cap,a.frequency_unit,a.frequency_interval," +
                    "a.osv_max,a.osv_min,a.osv_blacklist,a.osv_whitelist,a.make_blacklist,a.make_whitelist," +
                    "a.brand_blacklist,a.brand_whitelist,a.model_blacklist,a.model_whitelist," +
                    "a.sdkv_blacklist,a.did_blacklist,a.name" +
                    " FROM om_placement a" +
                    " LEFT JOIN om_publisher_app b ON b.id=a.pub_app_id" +
                    " LEFT JOIN om_publisher c ON c.id=a.publisher_id" +
                    " WHERE a.status=1 AND b.status=1 AND c.status=1";
            jdbcTemplate.query(sql, rs -> {
                PlacementPB.Placement.Builder pb = PlacementPB.Placement.newBuilder()
                        .setId(rs.getInt("id"))
                        .setPublisherId(rs.getInt("publisher_id"))
                        .setPubAppId(rs.getInt("pub_app_id"))
                        .setAppId(rs.getString("app_id"))
                        .setPlat(rs.getInt("plat"))
                        .setMainPlacement(rs.getInt("main_placement") > 0)
                        .setAdTypeValue(rs.getInt("ad_type"))
                        .setIcUrl(StringUtils.defaultIfEmpty(rs.getString("ic_url"), ""))
                        .setAllowHb(rs.getInt("hb_status") > 0)
                        .setPreloadTimeout(rs.getInt("preload_timeout"))
                        .setFanOut(rs.getInt("fan_out") == 1)
                        .setBatchSize(rs.getInt("batch_size"))
                        .setFrequencyCap(rs.getInt("frequency_cap"))
                        .setFrequencyUnit(rs.getInt("frequency_unit"))
                        .setFrequencyInterval(rs.getInt("frequency_interval"))
                        .setInventoryCount(rs.getInt("inventory_count"))
                        .setInventoryInterval(rs.getInt("inventory_interval"))
                        .setReloadInterval(rs.getInt("reload_interval"))
                        .setOsvMax(StringUtils.defaultIfBlank(rs.getString("osv_max"), ""))
                        .setOsvMin(StringUtils.defaultIfBlank(rs.getString("osv_min"), ""))
                        .addAllOsvBlacklist(str2list(rs.getString("osv_blacklist")))
                        .addAllOsvWhitelist(str2list(rs.getString("osv_whitelist")))
                        .addAllMakeBlacklist(str2list(rs.getString("make_blacklist")))
                        .addAllMakeWhitelist(str2list(rs.getString("make_whitelist")))
                        .addAllBrandBlacklist(str2list(rs.getString("brand_blacklist"), MODEL_SPLIT_STR))
                        .addAllBrandWhitelist(str2list(rs.getString("brand_whitelist"), MODEL_SPLIT_STR))
                        .addAllModelBlacklist(str2list(rs.getString("model_blacklist"), MODEL_SPLIT_STR))
                        .addAllModelWhitelist(str2list(rs.getString("model_whitelist"), MODEL_SPLIT_STR))
                        .addAllDidBlacklist(str2list(rs.getString("did_blacklist")))
                        .addAllSdkvBlacklist(str2list(rs.getString("sdkv_blacklist")))
                        .setName(StringUtils.defaultString(rs.getString("name"), ""));

                if (rs.getInt("floor_price_switch") == 0) {
                    pb.setFloorPrice(-1F);
                } else {
                    BigDecimal floorPrice = rs.getBigDecimal("floor_price");
                    pb.setFloorPrice(floorPrice == null ? -1F : floorPrice.floatValue());
                }
                pb.addAllScenes(sceneMap.getOrDefault(pb.getId(), Collections.emptyList()));
                pb.putAllCountrySettings(countrySettings.getOrDefault(pb.getId(), Collections.emptyMap()));

                String inventoryIntervalStep = rs.getString("inventory_interval_step");
                if (StringUtils.isBlank(inventoryIntervalStep)) {
                    pb.putAllInventoryIntervalStep(defaultInventoryIntervalStep);
                } else {
                    for (String nv : inventoryIntervalStep.split("[\r\n]")) {
                        nv = nv.trim();
                        Matcher m = regIis.matcher(nv);
                        if (m.find()) {
                            pb.putInventoryIntervalStep(Integer.parseInt(m.group(1)), Integer.parseInt(m.group(2)));
                        }
                    }
                }

                w.writeDelimited(pb.build());
            });
        });
    }

    void buildAdNetowrk() {
        build("om_adnetwork", cfg.dir, out -> {
            String sql = "SELECT id,name,class_name,type,sdk_version,bid_endpoint,descn,bid_type,expired_time FROM om_adnetwork WHERE status=1";
            jdbcTemplate.query(sql, rs -> {
                AdNetworkPB.AdNetwork adn = AdNetworkPB.AdNetwork.newBuilder()
                        .setId(rs.getInt("id"))
                        .setName(StringUtils.defaultIfEmpty(rs.getString("name"), ""))
                        .setClassName(StringUtils.defaultIfEmpty(rs.getString("class_name"), ""))
                        .setType(rs.getInt("type"))
                        .setSdkVersion(StringUtils.defaultIfEmpty(rs.getString("sdk_version"), ""))
                        .setBidEndpoint(StringUtils.defaultIfEmpty(rs.getString("bid_endpoint"), ""))
                        .setDescn(StringUtils.defaultIfEmpty(rs.getString("descn"), ""))
                        .setBidType(rs.getInt("bid_type"))
                        .setExpiredTime(rs.getInt("expired_time"))
                        .build();
                out.writeDelimited(adn);
            });
        });
    }

    void buildAdNetowrkApp() {
        build("om_adnetwork_app", cfg.dir, out -> {
            String sql = "select id,name,pub_app_id,adn_id,sdk_version,app_version,osv_max,osv_min," +
                    "make_device_blacklist,brand_model_blacklist from om_adnetwork_app_block_rule where status=1";
            Map<String, List<AdNetworkPB.AdNetworkAppBlockRule>> blockRule = new HashMap<>();
            jdbcTemplate.query(sql, rs -> {
                AdNetworkPB.AdNetworkAppBlockRule.Builder rule = AdNetworkPB.AdNetworkAppBlockRule.newBuilder()
                        .setId(rs.getInt("id"))
                        .setPubAppId(rs.getInt("pub_app_id"))
                        .setAdnId(rs.getInt("adn_id"))
                        .setSdkVersion(rs.getString("sdk_version"))
                        .setAppVersion(rs.getString("app_version"))
                        .setOsvMax(rs.getString("osv_max"))
                        .setOsvMin(rs.getString("osv_min"))
                        .addAllMakeDeviceBlacklist(str2list(rs.getString("make_device_blacklist"), String::toLowerCase))
                        .addAllBrandModelBlacklist(str2list(rs.getString("brand_model_blacklist"), String::toLowerCase, MODEL_SPLIT_STR));
                String key = rs.getInt("pub_app_id") + "_" + rs.getInt("adn_id");
                blockRule.computeIfAbsent(key, k -> new ArrayList<>()).add(rule.build());
            });

            sql = "SELECT id,adn_id,pub_app_id,adn_app_key FROM om_adnetwork_app WHERE status=1";
            jdbcTemplate.query(sql, rs -> {
                String key = rs.getInt("pub_app_id") + "_" + rs.getInt("adn_id");
                AdNetworkPB.AdNetworkApp adnApp = AdNetworkPB.AdNetworkApp.newBuilder()
                        .setId(rs.getInt("id"))
                        .setAdnId(rs.getInt("adn_id"))
                        .setPubAppId(rs.getInt("pub_app_id"))
                        .setAppKey(StringUtils.defaultIfEmpty(rs.getString("adn_app_key"), ""))
                        //.setAbtValue(rs.getInt("ab_test_mode"))
                        .addAllBlockRules(blockRule.getOrDefault(key, Collections.emptyList()))
                        .build();
                out.writeDelimited(adnApp);
            });
        });
    }

    void buildInstance() {
        String name = "om_instance";
        build(name, cfg.dir, out -> {
            Map<Integer, Map<String, AdNetworkPB.Instance.CountrySettings>> icp = new HashMap<>();
            Map<Integer, Map<String, Float>> instanceCountryEcpm = new HashMap<>();
            String sql = "select a.instance_id,a.country,hour0,hour1,hour2,hour3,hour4,hour5,hour6,a.manual_ecpm from om_instance_country a left join om_instance b on (a.instance_id = b.id) where b.status=1";
            jdbcTemplate.query(sql, rs -> {
                AdNetworkPB.Instance.CountrySettings.Builder cp = AdNetworkPB.Instance.CountrySettings.newBuilder();
                for (int i = 0; i < 7; i++) {
                    int hour = rs.getInt("hour" + i);
                    if (hour > 0) {
                        cp.putPeriod(i, hour);
                    }
                }
                icp.computeIfAbsent(rs.getInt("instance_id"), k -> new HashMap<>())
                        .put(rs.getString("country"), cp.build());
                instanceCountryEcpm.computeIfAbsent(rs.getInt("instance_id"), k -> new HashMap<>())
                        .put(rs.getString("country"), rs.getFloat("manual_ecpm"));
            });

            sql = "SELECT DISTINCT a.id,a.adn_id,a.pub_app_id,a.placement_id,a.placement_key," +
                    "a.osv_max,a.osv_min,a.make_whitelist,a.make_blacklist,a.brand_whitelist,a.brand_blacklist,a.hb_status," +
                    "a.model_whitelist,a.model_blacklist,a.frequency_cap,a.frequency_unit,a.frequency_interval,a.name" +
                    " FROM om_instance a " +
                    " LEFT JOIN om_adnetwork_app b ON b.pub_app_id=a.pub_app_id and b.adn_id=a.adn_id" +
                    " WHERE a.status=1 AND b.status=1";
            jdbcTemplate.query(sql, rs -> {
                int instanceId = rs.getInt("id");
                AdNetworkPB.Instance.Builder instance = AdNetworkPB.Instance.newBuilder()
                        .setId(instanceId)
                        .setAdnId(rs.getInt("adn_id"))
                        .setPubAppId(rs.getInt("pub_app_id"))
                        .setPlacementId(rs.getInt("placement_id"))
                        .setPlacementKey(StringUtils.defaultIfEmpty(rs.getString("placement_key"), ""))
                        .setOsvMax(StringUtils.defaultIfBlank(rs.getString("osv_max"), ""))
                        .setOsvMin(StringUtils.defaultIfBlank(rs.getString("osv_min"), ""))
                        .addAllMakeBlacklist(str2list(rs.getString("make_blacklist")))
                        .addAllMakeWhitelist(str2list(rs.getString("make_whitelist")))
                        .addAllBrandBlacklist(str2list(rs.getString("brand_blacklist"), MODEL_SPLIT_STR))
                        .addAllBrandWhitelist(str2list(rs.getString("brand_whitelist"), MODEL_SPLIT_STR))
                        .addAllModelBlacklist(str2list(rs.getString("model_blacklist"), MODEL_SPLIT_STR))
                        .addAllModelWhitelist(str2list(rs.getString("model_whitelist"), MODEL_SPLIT_STR))
                        .setFrequencyCap(rs.getInt("frequency_cap"))
                        .setFrequencyUnit(rs.getInt("frequency_unit"))
                        .setFrequencyInterval(rs.getInt("frequency_interval"))
                        //.setAbtValue(rs.getInt("ab_test_mode"))
                        .setHbStatus(rs.getInt("hb_status") == 1)
                        .setName(StringUtils.defaultIfEmpty(rs.getString("name"), ""))
                        .putAllCountrySettings(icp.getOrDefault(instanceId, Collections.emptyMap()))
                        .putAllCountryManualEcpm(instanceCountryEcpm.getOrDefault(instanceId, Collections.emptyMap()));
                out.writeDelimited(instance.build());
            });
        });
    }

    void buildSdkDevApp() {
        build("om_dev_app", cfg.dir, out -> {
            String sql = "SELECT publisher_id,pub_app_id,adn_id FROM om_dev_app where status=1";
            jdbcTemplate.query(sql, rs -> {
                out.writeDelimited(DevPB.SdkDevApp.newBuilder()
                        .setPublisherId(rs.getInt("publisher_id"))
                        .setPubAppId(rs.getInt("pub_app_id"))
                        .setAdnId(rs.getInt("adn_id"))
                        .build());
            });
        });
    }

    void buildSdkDevDevice() {
        build("om_dev_device", cfg.dir, out -> {
            String sql = "SELECT publisher_id,device_id,ab_test_mode FROM om_dev_device where status=1";
            jdbcTemplate.query(sql, rs -> {
                out.writeDelimited(DevPB.SdkDevDevice.newBuilder()
                        .setPublisherId(rs.getInt("publisher_id"))
                        .setDeviceId(rs.getString("device_id"))
                        .setAbtValue(rs.getInt("ab_test_mode"))
                        .build());
            });
        });
    }

    void buildInstanceRule() {
        build("om_instance_rule", cfg.dir, out -> {
            String weightSql = "select a.rule_id,a.instance_id,a.sort_type,a.priority,a.weight,a.group_id,d.hb_status,a.ab_test" +
                    " from om_placement_rule_instance a" +
                    " left join om_placement_rule b on (a.rule_id=b.id)" +
                    " left join om_placement c on (b.placement_id=c.id)" +
                    " left join om_instance d on (a.instance_id=d.id)" +
                    " left join om_placement_rule_group e on (a.group_id=e.id)" +
                    " where a.status=1 and b.status=1 and c.status=1 and d.status=1" +
                    " order by a.rule_id,a.group_id,a.priority";
            Map<Integer, Map<Integer, Integer>> ruleInstanceWeight = new HashMap<>(1000);
            Map<Integer, Map<Integer, Map<Integer, List<AdNetworkPB.InstanceRuleMediation>>>> ruleGroupInstance = new HashMap<>(1000);
            jdbcTemplate.query(weightSql, rs -> {
                int sortType = rs.getInt("sort_type");
                int ruleId = rs.getInt("rule_id");
                int instanceId = rs.getInt("instance_id");
                int groupId = rs.getInt("group_id");
                AdNetworkPB.InstanceRuleMediation.Builder irm = AdNetworkPB.InstanceRuleMediation.newBuilder()
                        .setRuleId(ruleId)
                        .setGroupId(groupId)
                        .setInstanceId(instanceId)
                        .setAbTest(rs.getInt("ab_test"));
                if (sortType == 1) {//绝对优先级
                    ruleInstanceWeight.computeIfAbsent(rs.getInt("rule_id"), k -> new HashMap<>(10))
                            .put(rs.getInt("instance_id"), rs.getInt("priority"));
                    ruleGroupInstance.computeIfAbsent(ruleId, k -> new HashMap<>())
                            .computeIfAbsent(groupId, k -> new HashMap<>())
                            .computeIfAbsent(irm.getAbTest(), k -> new ArrayList<>())
                            .add(irm.setPriority(rs.getInt("priority")).build());
                } else {
                    int weight = rs.getInt("weight");
                    if (weight > 0) {
                        ruleInstanceWeight.computeIfAbsent(rs.getInt("rule_id"), k -> new HashMap<>(10))
                                .put(rs.getInt("instance_id"), weight);
                        ruleGroupInstance.computeIfAbsent(ruleId, k -> new HashMap<>())
                                .computeIfAbsent(groupId, k -> new HashMap<>())
                                .computeIfAbsent(irm.getAbTest(), k -> new ArrayList<>())
                                .add(irm.setPriority(weight).build());
                    }
                }
            });

            String groupSql = "select a.id,a.rule_id,a.group_level,a.auto_switch,a.ab_test" +
                    " from om_placement_rule_group a" +
                    " left join om_placement_rule b on (a.rule_id=b.id)" +
                    " left join om_placement c on (b.placement_id=c.id)" +
                    " where b.status=1 and c.status=1";
            Map<Integer, List<AdNetworkPB.InstanceRuleGroup>> ruleGroupMap = new HashMap<>(1000);
            jdbcTemplate.query(groupSql, rs -> {
                int ruleId = rs.getInt("rule_id");
                int groupId = rs.getInt("id");
                int abt = rs.getInt("ab_test");
                AdNetworkPB.InstanceRuleGroup.Builder prg = AdNetworkPB.InstanceRuleGroup.newBuilder()
                        .setRuleId(ruleId)
                        .setGroupId(groupId)
                        .setGroupLevel(rs.getInt("group_level"))
                        .setAutoSwitch(rs.getInt("auto_switch"))
                        .setAbTest(abt)
                        .addAllInstanceWeight(ruleGroupInstance.getOrDefault(ruleId, Collections.emptyMap())
                                .getOrDefault(groupId, Collections.emptyMap()).getOrDefault(abt, Collections.emptyList()));
                ruleGroupMap.computeIfAbsent(ruleId, k -> new ArrayList<>())
                        .add(prg.build());
            });

            String abtSql = "select a.id,a.rule_id,a.name,a.a_per,a.b_per" +
                    " from om_placement_rule_abt a" +
                    " left join om_placement_rule b on (a.rule_id=b.id)" +
                    " left join om_placement c on (b.placement_id=c.id)" +
                    " where a.status=1 and b.status=1 and c.status=1";
            Map<Integer, AdNetworkPB.InstanceRuleAbt> ruleAbtMap = new HashMap<>(1000);
            jdbcTemplate.query(abtSql, rs -> {
                int ruleId = rs.getInt("rule_id");
                AdNetworkPB.InstanceRuleAbt.Builder abt = AdNetworkPB.InstanceRuleAbt.newBuilder()
                        .setId(rs.getInt("id"))
                        .setRuleId(ruleId)
                        .setName(StringUtils.defaultString(rs.getString("name")))
                        .setAPer(rs.getInt("a_per"))
                        .setBPer(rs.getInt("b_per"));
                ruleAbtMap.put(ruleId, abt.build());
            });

            String sql = "SELECT a.id,a.publisher_id,a.pub_app_id,a.placement_id,e.countries,a.ab_test," +
                    "a.auto_opt,a.sort_type,a.priority,a.status,a.create_user_id,a.create_time,a.priority," +
                    "e.frequency, e.con_type, e.brand_whitelist, e.brand_blacklist, e.model_whitelist, e.model_blacklist," +
                    "e.gender, e.age_max, e.age_min, e.interest, e.iap_min, e.iap_max, e.channel, e.channel_bow, e.model_type, " +
                    "e.osv_exp,e.sdkv_exp,e.appv_exp,e.require_did,e.custom_tags,a.name,a.algorithm_id,a.ab_test" +
                    " FROM om_placement_rule a" +
                    " left join om_placement b on (a.placement_id=b.id)" +
                    " left join om_publisher_app c on (a.pub_app_id=c.id)" +
                    " left join om_publisher d on (a.publisher_id=d.id)" +
                    " left join om_placement_rule_segment e on (a.segment_id=e.id)" +
                    " where a.status=1 and b.status=1 and c.status=1 and d.status=1";
            jdbcTemplate.query(sql, rs -> {
                String countries = rs.getString("countries");
                if (StringUtils.isBlank(countries)) {
                    return;
                }
                int ruleId = rs.getInt("id");
                RangeExp osvExp = new RangeExp(rs.getString("osv_exp"));
                RangeExp sdkvExp = new RangeExp(rs.getString("sdkv_exp"));
                RangeExp appvExp = new RangeExp(rs.getString("appv_exp"));
                Map<Integer, Integer> instanceWeight = ruleInstanceWeight.getOrDefault(ruleId, Collections.emptyMap());
                AdNetworkPB.InstanceRule.Builder sb = AdNetworkPB.InstanceRule.newBuilder()
                        .setId(ruleId)
                        .setPublisherId(rs.getInt("publisher_id"))
                        .setPubAppId(rs.getInt("pub_app_id"))
                        .setPlacementId(rs.getInt("placement_id"))
                        .addAllCountry(str2list(countries, ","))
                        .setSortType(rs.getInt("sort_type"))
                        .setAbtValue(rs.getInt("ab_test"))
                        .setAutoSwitch(rs.getInt("auto_opt"))
                        .setPriority(rs.getInt("priority"))
                        .putAllInstanceWeight(instanceWeight)
                        .setFrequency(rs.getInt("frequency"))
                        .setConType(rs.getInt("con_type"))
                        .addAllBrandWhitelist(str2list(rs.getString("brand_whitelist"), MODEL_SPLIT_STR))
                        .addAllBrandBlacklist(str2list(rs.getString("brand_blacklist"), MODEL_SPLIT_STR))
                        .addAllModelWhitelist(str2list(rs.getString("model_whitelist"), MODEL_SPLIT_STR))
                        .addAllModelBlacklist(str2list(rs.getString("model_blacklist"), MODEL_SPLIT_STR))
                        .setIapMin(rs.getFloat("iap_min"))
                        .setIapMax(rs.getFloat("iap_max"))
                        .setGender(rs.getInt("gender"))
                        .setAgeMin(rs.getInt("age_min"))
                        .setAgeMax(rs.getInt("age_max"))
                        .setModelType(rs.getInt("model_type"))
                        .addAllInterest(str2list(rs.getString("interest"), MODEL_SPLIT_STR))
                        .addAllChannel(str2list(rs.getString("channel"), MODEL_SPLIT_STR))
                        .setChannelBow(rs.getInt("channel_bow") == 1)
                        .setRequireDid(rs.getInt("require_did"))
                        .setName(StringUtils.defaultString(rs.getString("name")))
                        .setAlgorithmId(rs.getInt("algorithm_id"))
                        .setAbTest(rs.getInt("ab_test"));

                AdNetworkPB.InstanceRuleAbt abt = ruleAbtMap.get(ruleId);
                if (abt != null) {
                    sb.setRuleAbt(abt);
                }

                Map<Integer, List<AdNetworkPB.InstanceRuleMediation>> biddingIns = ruleGroupInstance.getOrDefault(ruleId, Collections.emptyMap())
                        .getOrDefault(0, Collections.emptyMap());
                if (!biddingIns.isEmpty()) {
                    biddingIns.forEach((abModel, bidIns) -> {
                        AdNetworkPB.InstanceRuleGroup.Builder prg = AdNetworkPB.InstanceRuleGroup.newBuilder()
                                .setRuleId(ruleId)
                                .setGroupId(0)
                                .setGroupLevel(0)
                                .setAutoSwitch(1)
                                .setAbTest(abModel)
                                .addAllInstanceWeight(bidIns);
                        sb.addGroups(prg.build());
                    });
                }
                sb.addAllGroups(ruleGroupMap.getOrDefault(ruleId, Collections.emptyList()));
                if (osvExp.hasRange()) {
                    sb.addAllOsvRange(osvExp.getRanges());
                }
                if (osvExp.hasItems()) {
                    sb.addAllOsvWhite(osvExp.getItems());
                }
                if (sdkvExp.hasRange()) {
                    sb.addAllSdkvRange(sdkvExp.getRanges());
                }
                if (sdkvExp.hasItems()) {
                    sb.addAllSdkvWhite(sdkvExp.getItems());
                }
                if (appvExp.hasRange()) {
                    sb.addAllAppvRange(appvExp.getRanges());
                }
                if (appvExp.hasItems()) {
                    sb.addAllAppvWhite(appvExp.getItems());
                }
                String ctg = rs.getString("custom_tags");
                if (StringUtils.isNoneBlank(ctg)) {
                    try {
                        JSONObject cTags = JSON.parseObject(ctg);
                        for (String name : cTags.keySet()) {
                            AdNetworkPB.CustomTag.Builder customTag = AdNetworkPB.CustomTag.newBuilder();
                            JSONObject cons = cTags.getJSONObject(name);
                            int type = cons.getInteger("type");
                            customTag.setName(name);
                            customTag.setType(type);
                            JSONArray arr = cons.getJSONArray("conditions");
                            if (arr != null && arr.size() > 0) {
                                for (int i = 0; i < arr.size(); i++) {
                                    AdNetworkPB.TagCondition.Builder tcb = AdNetworkPB.TagCondition.newBuilder();
                                    JSONObject con = arr.getJSONObject(i);
                                    String op = con.getString("operator");
                                    String value = con.getString("value");
                                    if (StringUtils.isBlank(value)) continue;
                                    tcb.setOperator(op);
                                    if ("in".equals(op) || "notin".equals(op)) {
                                        List<String> vals = Arrays.stream(value.split(",")).collect(Collectors.toList());
                                        tcb.addAllValues(vals);
                                    } else {
                                        tcb.setValue(value);
                                    }
                                    customTag.addConditions(tcb);
                                }
                                sb.putCustomTags(name, customTag.build());
                            }
                        }
                    } catch (Exception e) {
                        log.error("custom tag parse error, rule:{}, customTag:{}", sb.getId(), ctg, e);
                    }
                }
                out.writeDelimited(sb.build());
            });
        });
    }

    void buildAbTest() {
        build("om_abtest", cfg.dir, out -> {
            String sql = "SELECT a.placement_id,a_per,b_per,a_rule_id,b_rule_id,b.segment_id,c.countries" +
                    " FROM om_placement_abt a" +
                    " left join om_placement_rule b on (a.a_rule_id=b.id)" +
                    " left join om_placement_rule_segment c on (b.segment_id=c.id)" +
                    " where a.status=1 and b.status=1";
            jdbcTemplate.query(sql, rs -> {
                int aPer = rs.getInt("a_per");
                int bPer = rs.getInt("b_per");
                String countries = rs.getString("countries");
                if (StringUtils.isNoneBlank(countries)) {
                    for (String country : countries.split(",")) {
                        out.writeDelimited(PlacementPB.PlacementAbTest.newBuilder()
                                .setPlacementId(rs.getInt("placement_id"))
                                .setAPer(aPer)
                                .setBPer(bPer)
                                .setARuleId(rs.getInt("a_rule_id"))
                                .setBRuleId(rs.getInt("b_rule_id"))
                                .setSegmentId(rs.getInt("segment_id"))
                                .setCountry(country)
                                .build());
                    }
                }
            });
        });
    }

}
