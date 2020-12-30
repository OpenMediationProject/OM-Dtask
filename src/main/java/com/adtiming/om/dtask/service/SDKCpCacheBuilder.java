package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.util.RangeExp;
import com.adtiming.om.pb.CrossPromotionPB.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.adtiming.om.dtask.dto.CpCampaignTarget.*;


@Component
public class SDKCpCacheBuilder extends PbBuiler {

    private static final Logger LOG = LogManager.getLogger();

    @Resource
    private AppConfig cfg;

    @Resource
    private JdbcTemplate jdbcTemplateW;

    @Scheduled(fixedDelay = 60000)
    private void buildCache() {
        if (!cfg.isProd()) {
            return;
        }
        long start = System.currentTimeMillis();
        LOG.debug("build cross promotion cache start");
        if (!prepareData()) {
            LOG.warn("build cross promotion cache abort, table not exists");
            return;
        }
        buildCampaign();
        buildCampaignTargeting();
        buildCreative();
        buildMaterial();
        buildTemplate();
        buildApp();
        LOG.debug("build cross promotion cache finished, cost:{} ms", System.currentTimeMillis() - start);
    }

    boolean prepareData() {
        String checkSql = "select count(*) from information_schema.TABLES" +
                " where TABLE_SCHEMA=schema() and TABLE_NAME='cp_campaign'";
        if (jdbcTemplateW.queryForObject(checkSql, Integer.class) == 0) {
            return false;
        }

        LocalDateTime dt = LocalDateTime.now(ZoneOffset.UTC);
        int bitHour = 1 << dt.getHour();
        int dayIndex = dt.getDayOfWeek().getValue() % 7;// 0:Sunday,1:Monday
        String hourField = "p.hour" + dayIndex;

        jdbcTemplateW.batchUpdate(
                // 清理临时表
                "delete from cp_tmp_cid",
                "delete from cp_tmp_crid",

                // 生成临时需要导出的 campaignId
                "insert into cp_tmp_cid (id)" +
                        " select a.id from cp_campaign a" +
                        " left join cp_campaign_period p on a.id = p.campaign_id" +
                        " where a.status = 1" +
                        " and a.start_time <= CURRENT_TIMESTAMP" +
                        " and a.end_time >= CURRENT_TIMESTAMP" +
                        " and (" + hourField + " is null or "  + hourField + " = 0 or " + hourField + " & " + bitHour + " = " + bitHour + ")",

                // 生成临时需要导出的 creativeId
                "insert into cp_tmp_crid (id)" +
                        " select a.id from cp_creative a" +
                        " right join cp_tmp_cid tmp on tmp.id=a.campaign_id" +
                        " where a.status=1 and a.weight>0"
        );
        return true;
    }

    /**
     * build cp_campaign
     */
    void buildCampaign() {
        build("cp_campaign", cfg.dir, out -> {
            Map<Long, Map<String, Float>> cidCountryBidpriceMap = new HashMap<>(1000);
            String sqlCountryBidPrices = "select a.campaign_id, a.country, a.bidprice" +
                    " from cp_campaign_bidprice a" +
                    " right join cp_tmp_cid tmp on tmp.id=a.campaign_id";
            jdbcTemplateW.query(sqlCountryBidPrices, rs -> {
                cidCountryBidpriceMap
                        .computeIfAbsent(rs.getLong("campaign_id"), k -> new HashMap<>(20))
                        .put(rs.getString("country"), rs.getFloat("bidprice"));
            });

            String sqlCampaign = "select a.id,a.ska_campaign_id,a.publisher_id,a.type,a.name,a.app_id,a.app_name," +
                    "a.preview_url,a.platform,a.billing_type,a.price,a.daily_cap,a.daily_budget,a.max_bidprice," +
                    "a.bidprice,a.impr_cap,a.impr_freq,a.ad_domain,a.click_url,a.click_tk_urls,a.impr_tk_urls,a.open_type" +
                    " from cp_campaign a" +
                    " right join cp_tmp_cid tmp on tmp.id=a.id";
            jdbcTemplateW.query(sqlCampaign, rs -> {
                long cid = rs.getLong("id");
                out.writeDelimited(CpCampaign.newBuilder()
                        .setId(cid)
                        .setSkaCampaignId(rs.getInt("ska_campaign_id"))
                        .setPublisherId(rs.getInt("publisher_id"))
                        .setType(rs.getInt("type"))
                        .setName(rs.getString("name"))
                        .setAppId(rs.getString("app_id"))
                        .setAppName(rs.getString("app_name"))
                        .setPreviewUrl(StringUtils.defaultString(rs.getString("preview_url")))
                        .setPlatform(rs.getInt("platform"))
                        .setBillingType(rs.getInt("billing_type"))
                        .setPrice(rs.getFloat("price"))
                        .setDailyCap(rs.getInt("daily_cap"))
                        .setDailyBudget(rs.getFloat("daily_budget"))
                        .setMaxBidprice(rs.getFloat("max_bidprice"))
                        .setBidprice(rs.getFloat("bidprice"))
                        .putAllCountryBidprice(cidCountryBidpriceMap.getOrDefault(cid, Collections.emptyMap()))
                        .setImprCap(rs.getInt("impr_cap"))
                        .setImprFreq(rs.getInt("impr_freq"))
                        .setAdDomain(StringUtils.defaultString(rs.getString("ad_domain")))
                        .setClickUrl(rs.getString("click_url"))
                        .addAllClickTkUrls(str2list(rs.getString("click_tk_urls")))
                        .addAllImprTkUrls(str2list(rs.getString("impr_tk_urls")))
                        .setOpenType(rs.getInt("open_type"))
                        .build());
            });
        });
    }

    void buildCampaignTargeting() {
        build("cp_campaign_targeting", cfg.dir, out -> {
            String sql = "select a.campaign_id,a.type,a.content" +
                    " from cp_campaign_targeting a" +
                    " right join cp_tmp_cid tmp on tmp.id=a.campaign_id" +
                    " order by a.campaign_id";// 排序可以保证 campaign 流式输出
            AtomicLong lastCid = new AtomicLong();
            CpCampaignTargeting.Builder[] holder = {null};
            jdbcTemplateW.query(sql, rs -> {
                long cid = rs.getLong("campaign_id");
                int type = rs.getInt("type");
                String content = rs.getString("content");
                if (cid < 1L) {
                    return;
                }
                CpCampaignTargeting.Builder b = holder[0];
                if (cid != lastCid.get()) {
                    if (b != null) {
                        out.writeDelimited(b.build());
                    }
                    lastCid.set(cid);
                    holder[0] = b = CpCampaignTargeting.newBuilder();
                    b.setCampaignId(cid);
                }

                switch (type) {
                    case PUBAPP_WHITE:
                        b.addPubappWhite(NumberUtils.toInt(content));
                        break;
                    case PUBAPP_BLACK:
                        b.addPubappBlack(NumberUtils.toInt(content));
                        break;
                    case PLACEMENT_WHITE:
                        b.addPlacementWhite(NumberUtils.toInt(content));
                        break;
                    case PLACEMENT_BLACK:
                        b.addPlacementBlack(NumberUtils.toInt(content));
                        break;
                    case MAKE_WHITE:
                        b.addMakeWhite(content);
                        break;
                    case MAKE_BLACK:
                        b.addMakeBlack(content);
                        break;
                    case BRAND_WHITE:
                        b.addBrandWhite(content);
                        break;
                    case BRAND_BLACK:
                        b.addBrandBlack(content);
                        break;
                    case MODEL_WHITE:
                        b.addModelWhite(content);
                        break;
                    case MODEL_BLACK:
                        b.addModelBlack(content);
                        break;
                    case DEVICE_TYPE_WHITE:
                        b.addDevicetypeWhite(NumberUtils.toInt(content));
                        break;
                    case DEVICE_TYPE_BLACK:
                        b.addDevicetypeBlack(NumberUtils.toInt(content));
                        break;
                    case CONNECTION_TYPE:
                        b.setContype(NumberUtils.toInt(content));
                        break;
                    case MCCMNC_WHITE:
                        b.addMccmncWhite(content);
                        break;
                    case MCCMNC_BLACK:
                        b.addMccmncBlack(content);
                        break;
                    case OSV_WHITE_EXP:
                        RangeExp r = new RangeExp(content);
                        if (r.hasItems()) b.addAllOsvWhite(r.getItems());
                        if (r.hasRange()) b.addAllOsvWhiteRange(r.getRanges());
                        break;
                    case OSV_BLACK_EXP:
                        r = new RangeExp(content);
                        if (r.hasItems()) b.addAllOsvBlack(r.getItems());
                        if (r.hasRange()) b.addAllOsvBlackRange(r.getRanges());
                        break;
                    case COUNTRY_WHITE:
                        b.addCountryWhite(content);
                        break;
                    case COUNTRY_BLACK:
                        b.addCountryBlack(content);
                        break;
                    // in case there are other targetings
                    default:
                        LOG.error("targeting type error {}", type);
                }

            });
            // write last one
            if (holder[0] != null) {
                out.writeDelimited(holder[0].build());
            }
        });
    }

    void buildCreative() {
        build("cp_creative", cfg.dir, out -> {
            Map<Long, List<Long>> creativeMaterialIdsMap = new HashMap<>(1000);
            String sql = "select a.creative_id,a.material_id from cp_creative_material a" +
                    " right join cp_tmp_crid tmp on tmp.id=a.creative_id";
            jdbcTemplateW.query(sql, rs -> {
                creativeMaterialIdsMap
                        .computeIfAbsent(rs.getLong("creative_id"), k -> new ArrayList<>())
                        .add(rs.getLong("material_id"));
            });

            sql = "select a.id,a.campaign_id,a.name,a.type,a.title,a.descn,a.play_url,a.weight," +
                    "a.template,a.endcard_template" +
                    " from cp_creative a" +
                    " right join cp_tmp_crid tmp on tmp.id=a.id";
            jdbcTemplateW.query(sql, rs -> {
                long crid = rs.getLong("id");
                out.writeDelimited(CpCreative.newBuilder()
                        .setId(crid)
                        .setCampaignId(rs.getLong("campaign_id"))
                        .setName(StringUtils.defaultIfEmpty(rs.getString("name"), ""))
                        .setTypeValue(rs.getInt("type"))
                        .setTitle(StringUtils.defaultIfEmpty(rs.getString("title"), ""))
                        .setDescn(StringUtils.defaultIfEmpty(rs.getString("descn"), ""))
                        .setPlayUrl(StringUtils.defaultIfEmpty(rs.getString("play_url"), ""))
                        .setWeight(rs.getInt("weight"))
                        .setTemplate(rs.getInt("template"))
                        .setEndcardTemplate(rs.getInt("endcard_template"))
                        .addAllMaterialIds(creativeMaterialIdsMap.getOrDefault(crid, Collections.emptyList()))
                        .build());
            });

        });
    }

    void buildMaterial() {
        build("cp_material", cfg.dir, out -> {
            String sql = "select DISTINCT a.id,a.type,a.url,a.mime_type,a.width,a.height,a.size,a.video_duration" +
                    " from cp_material a" +
                    " left join cp_creative_material b on b.material_id=a.id" +
                    " right join cp_tmp_crid tmp on tmp.id=b.creative_id" +
                    " where a.status=1";
            jdbcTemplateW.query(sql, rs -> {
                out.writeDelimited(CpMaterial.newBuilder()
                        .setId(rs.getInt("id"))
                        .setTypeValue(rs.getInt("type"))
                        .setUrl(rs.getString("url"))
                        .setMimeType(StringUtils.defaultIfEmpty(rs.getString("mime_type"), ""))
                        .setWidth(rs.getInt("width"))
                        .setHeight(rs.getInt("height"))
                        .setSize(rs.getInt("size"))
                        .setVideoDuration(rs.getInt("video_duration"))
                        .build());
            });
        });
    }

    private void buildTemplate() {
        build("cp_template", cfg.dir, out -> {
            String sql = "select id,name,type,url,width,height,need_carousel" +
                    " from cp_template where status=1";
            jdbcTemplateW.query(sql, rs -> {
                out.writeDelimited(H5Template.newBuilder()
                        .setId(rs.getInt("id"))
                        .setType(rs.getInt("type"))
                        .setUrl(rs.getString("url"))
                        .setWidth(rs.getInt("width"))
                        .setHeight(rs.getInt("height"))
                        .setNeedCarousel(rs.getInt("need_carousel") == 1)
                        .build());
            });
        });
    }

    private void buildApp() {
        build("cp_app", cfg.dir, out -> {
            String sql = "select a.id, a.plat, a.app_id, a.name, a.icon, a.os_require, a.rating_value, a.rating_count," +
                    " a.category, a.category_id, a.sub_category_id1, a.sub_category_id2" +
                    " from om_app a" +
                    " inner join cp_campaign c on c.app_id=a.app_id" +
                    " inner join cp_tmp_cid tmp on tmp.id=c.id" +
                    " group by a.id";
            jdbcTemplateW.query(sql, rs -> {
                String osRequire = rs.getString("os_require");
                int plat = rs.getInt("plat");

                String osvMin = "";
                String osvMax = "";

                if (StringUtils.isNotBlank(osRequire)) {
                    if (plat == 1) {
                        if (osRequire.contains("Varies with device")) {
                            osvMin = "";
                            osvMax = "";
                        } else if (osRequire.contains(" and up")) {
                            osvMin = osRequire.replace(" and up", "");
                            osvMax = "";
                        } else if (osRequire.contains(" - ")) {
                            String[] arr = osRequire.split(" - ");
                            osvMin = arr[0];
                            osvMax = arr[1];
                        }
                    } else if (plat == 0) {
                        osvMin = osRequire;
                    }
                }

                out.writeDelimited(App.newBuilder()
                        .setId(rs.getInt("id"))
                        .setAppId(rs.getString("app_id"))
                        .setPlat(plat)
                        .setName(StringUtils.defaultIfEmpty(rs.getString("name"), ""))
                        .setIcon(StringUtils.defaultIfEmpty(rs.getString("icon"), ""))
                        .setOsvMin(osvMin)
                        .setOsvMax(osvMax)
                        .setRatingValue(rs.getFloat("rating_value"))
                        .setRatingCount(rs.getInt("rating_count"))
                        .setCategory(StringUtils.defaultIfEmpty(rs.getString("category"), ""))
                        .setCategoryId(rs.getInt("category_id"))
                        .setSubCategoryId1(rs.getInt("sub_category_id1"))
                        .setSubCategoryId2(rs.getInt("sub_category_id2"))
                        .build());
            });
        });
    }

}
