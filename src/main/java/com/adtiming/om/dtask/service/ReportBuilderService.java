// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import com.adtiming.om.dtask.dto.DataSourceType;
import com.adtiming.om.dtask.dto.MailSender;
import com.adtiming.om.dtask.dto.ReportBuilderDTO;
import com.adtiming.om.dtask.dto.ReportBuilderTaskDTO;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.util.TypeUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.mail.HtmlEmail;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.activation.DataHandler;
import javax.annotation.Resource;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMultipart;
import javax.mail.internet.MimeUtility;
import javax.mail.util.ByteArrayDataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Report builder on time service
 *
 * @author dianbo ruan
 */
@Service
public class ReportBuilderService {

    private static final Logger LOG = LogManager.getLogger();

    public static final int SCHEDULE_DAILY = 1;
    public static final int SCHEDULE_WEEKLY = 2;
    public static final int SCHEDULE_MONTHLY = 3;

    private static final String SPLIT_CHAR = ",";
    private static Map<String, String> TITLES;

    private static final DateTimeFormatter FMT_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final BigDecimal d100 = new BigDecimal(100);
    private static final BigDecimal d1000 = new BigDecimal(1000);

    static {
        Map<String, String> m = new HashMap<>();
        // dimensions
        m.put("day", "Date");
        m.put("country", "Region");
        m.put("adn_id", "AdNetwork");
        m.put("pub_app_id", "App");
        m.put("placement_id", "Placement");
        m.put("instance_id", "Instance");

        // metrics
        m.put("request", "Requests");
        m.put("filled", "Fills");
        m.put("fill_rate", "Fill Rate");
        m.put("is_ready", "Ad-Availability Check");
        m.put("is_ready_true", "Ad-Availability Response");
        m.put("is_ready_rate", "Availability Rate");
        m.put("impr", "Impressions");
        m.put("click", "Clicks");
        m.put("ctr", "CTR");
        m.put("cost", "Revenue");
        m.put("ecpm", "eCPM");
        m.put("dau", "DAU");
        m.put("impr_dau", "Impressions per DAU");
        m.put("arp_dau", "ARPDAU");
        m.put("deu", "DEU");
        m.put("impr_deu", "Impressions per DEU");
        m.put("arp_deu", "ARPDEU");
        m.put("engagement_rate", "Engagement Rate");
        TITLES = m;
    }

    @Resource
    private DictManager dictManager;

    @Resource
    private ObjectMapper objectMapper;

    @Resource
    private JdbcTemplate jdbcTemplate;

    @Scheduled(cron = "0 */10 * * * ?", zone = "UTC")
    public void scheduleReport() {
        MailSender mailSender = getMailSender();
        if (mailSender == null) {
            return;
        }

        List<ReportBuilderDTO> builders = getReportBuilders();
        for (ReportBuilderDTO dto : builders) {
            buildTask(dto);
        }
        doTasks(builders, mailSender);
    }

    MailSender getMailSender() {
        String reportSender = dictManager.val("/om/report_sender");
        if (StringUtils.isBlank(reportSender)) {
            LOG.error("No report sender set yet");
            return null;
        }
        try {
            return objectMapper.readValue(reportSender, MailSender.class);
        } catch (IOException e) {
            throw new RuntimeException("parse ", e);
        }
    }

    public void doTasks(List<ReportBuilderDTO> builders, MailSender mailSender) {
        Map<Integer, ReportBuilderDTO> builderMap = new HashMap<>(builders.size());
        builders.forEach(o -> builderMap.put(o.getId(), o));

        try {
            String selectTasksSql = "SELECT * FROM om_report_builder_task where " +
                    " reparation_count < 5 and status != 1 and create_time >= ?";
            LocalDate lastMonthFirstDay = LocalDate.now().plusMonths(-1).withDayOfMonth(1);
            List<ReportBuilderTaskDTO> tasks = jdbcTemplate.query(selectTasksSql, ReportBuilderTaskDTO.ROWMAPPER, lastMonthFirstDay);
            LOG.info("Do tasks: {}", JSON.toJSONString(tasks));
            for (ReportBuilderTaskDTO task : tasks) {
                int builderId = task.getBuilderId();
                ReportBuilderDTO builder = builderMap.get(builderId);
                if (builder != null) {
                    doTask(task, builder, mailSender);
                } else {
                    LOG.error("task not found, builderId: {}", builderId);
                    String updateTaskSql = "update om_report_builder_task set status = ?, reparation_count = ? where id = ?";
                    jdbcTemplate.update(updateTaskSql, 3, task.getReparationCount(), task.getId());
                }
            }
        } catch (Exception e) {
            LOG.error("Do task error", e);
        }
    }

    public void doTask(ReportBuilderTaskDTO task, ReportBuilderDTO builder, MailSender mailSender) {
        try {
            builder.setTaskDay(task.getDay());
            List<String> reportLines = buildReport(builder);
            if (reportLines == null || reportLines.size() <= 1) {
                throw new RuntimeException("Get report null");
            }
            String result = StringUtils.join(reportLines, '\n');
            boolean sendStatus = sendToUser(builder, result, mailSender);
            if (sendStatus) {
                String updateTaskSql = "update om_report_builder_task set status = ?, reparation_count = ?, report_line_size = ?  where id = ?";
                jdbcTemplate.update(updateTaskSql, 1, task.getReparationCount() + 1, reportLines.size() - 1, task.getId());
                LOG.info("Task {} execute successfully", JSONObject.toJSONString(task));
            } else {
                throw new RuntimeException("Task " + JSONObject.toJSONString(task) + "error");
            }
        } catch (Exception e) {
            LOG.error("Do task {} error:", task.getId(), e);
            String updateTaskSql = "update om_report_builder_task set status = ?, reparation_count = ? where id = ?";
            int result = jdbcTemplate.update(updateTaskSql, 2, task.getReparationCount() + 1, task.getId());
            if (result <= 0) {
                LOG.error("Update task {} error where execute failed", JSON.toJSONString(task), e);
            }
        } finally {
            String updateBuilderSql = "update om_report_builder set last_execute_time = ?, execute_times = ? where id = ?";
            int result = jdbcTemplate.update(updateBuilderSql, new Date(), builder.getExecuteTimes(), builder.getId());
            if (result <= 0) {
                LOG.error("Update builder id {} error", builder.getId());
            }
        }
    }

    boolean sendToUser(ReportBuilderDTO dto, String report, MailSender mailSender) {
        try {
            HtmlEmail email = new HtmlEmail();
            email.setSSLOnConnect(mailSender.isSsl());
            email.setHostName(mailSender.getHost());
            email.setAuthentication(mailSender.getEmail(), mailSender.getPwd());
            email.setCharset("UTF-8");
            email.setFrom(mailSender.getEmail(), "OM-Report", "UTF-8");
            email.setSubject("OM-Report For You");
            email.setHtmlMsg("<h3>Please check attachment</h3>");

            // attachment
            String fileName = dto.getName() + "_report_" + FMT_DATE.format(dto.getTaskDay());
            MimeBodyPart contentPart = new MimeBodyPart();
            ByteArrayOutputStream buf = new ByteArrayOutputStream(report.length() / 5);
            ZipOutputStream out = new ZipOutputStream(buf);
            ZipEntry entry = new ZipEntry(fileName + ".csv");
            out.putNextEntry(entry);
            IOUtils.write(report, out, StandardCharsets.UTF_8);
            out.close();
            ByteArrayDataSource ds = new ByteArrayDataSource(buf.toByteArray(), "application/zip");
            contentPart.setDataHandler(new DataHandler(ds));
            contentPart.setFileName(MimeUtility.encodeText(fileName + ".zip"));
            MimeMultipart part = new MimeMultipart();
            part.addBodyPart(contentPart);
            email.addPart(part);

            for (String recipient : dto.getRecipients().split("[,; ]")) {
                if (StringUtils.isNotEmpty(recipient)) {
                    email.addTo(recipient);
                }
            }
            email.send();
            return true;
        } catch (Exception e) {
            LOG.error("Send report {} error:", dto.getName(), e);
        }
        return false;
    }

    private void buildTask(ReportBuilderDTO builder) {
        LocalDate now = LocalDate.now();
        switch (builder.getSchedule()) {
            case SCHEDULE_DAILY:
                addTask(builder, now.plusDays(-1));
                break;
            case SCHEDULE_WEEKLY:
                if (builder.getWeeklyDay() == (now.getDayOfWeek().getValue() % 7)) {
                    addTask(builder, now.plusDays(-8));
                }
                break;
            case SCHEDULE_MONTHLY:
                if (now.getDayOfMonth() == 4) {
                    addTask(builder, now.plusMonths(-1));
                }
                break;
            default:
                LOG.warn("scheduleType error: {}", builder.getSchedule());
        }
    }

    private void addTask(ReportBuilderDTO builder, LocalDate day) {
        try {
            String taskSql = "INSERT INTO `om_report_builder_task` (`day`, `builder_id`, `create_time`) " +
                    "VALUES (?, ?, ?)";
            String selectTaskSql = "SELECT * FROM om_report_builder_task where builder_id = ? and day >= ? order by create_time desc";
            List<Map<String, Object>> tasks = jdbcTemplate.queryForList(selectTaskSql, builder.getId(), day);
            if (CollectionUtils.isEmpty(tasks)) {
                int result = jdbcTemplate.update(taskSql, day, builder.getId(), new Date());
                if (result > 0) {
                    LOG.info("Add task of day {} of builder id {} successfully", day, builder.getId());
                } else {
                    LOG.warn("Add task of day {} of builder id {} failed", day, builder.getId());
                }
            }
        } catch (Exception e) {
            LOG.error("Add task {} error", JSON.toJSONString(builder), e);
        }
    }

    public List<String> buildReport(ReportBuilderDTO builder) {
        try {
            List<Map<String, Object>> adNetWorkReport = getAdNetWorkReport(builder);
            List<Map<String, Object>> lRReport = getLRReport(builder);
            List<Map<String, Object>> dauReport = getDauReport(builder);

            // [dimension, dimensionValue, dimensionLabel]
            Map<String, Map<Object, Object>> dimensionLables = new HashMap<>();

            Map<Object, Optional<Map<String, Object>>> mergeResult = Stream.concat(
                    adNetWorkReport.stream(), Stream.concat(lRReport.stream(), dauReport.stream()))
                    .collect(Collectors.groupingBy(report -> {
                        List<Object> keys = new ArrayList<>();
                        for (String dim : builder.getDimensionSet()) {
                            Object dimValue = report.get(dim);
                            keys.add(dimValue);
                            if (dim.endsWith("_id")) {
                                dimensionLables.computeIfAbsent(dim, k -> new HashMap<>())
                                        .put(TypeUtils.castToLong(dimValue), dimValue);
                            }
                        }
                        return StringUtils.join(keys, SPLIT_CHAR);
                    }, Collectors.reducing((a, b) -> {
                        a.putAll(b);
                        return a;
                    })));

            fillDimensionLables(dimensionLables);

            List<Map<String, Object>> results = new ArrayList<>(mergeResult.size());
            mergeResult.forEach((k, opt) -> opt.ifPresent(results::add));
            if (builder.getDimensionSet().contains("day")) {
                results.sort((a, b) -> {
                    Date aDay = (Date) a.get("day");
                    Date bDay = (Date) b.get("day");
                    return aDay.compareTo(bDay);
                });
            }

            List<String> reportLines = buildLines(builder, results, dimensionLables);
            LOG.info("Report lines {}", reportLines.size());
            return reportLines;
        } catch (Exception e) {
            LOG.error("Do report error:", e);
        }
        return null;
    }

    private void fillDimensionLables(Map<String, Map<Object, Object>> dimensionLables) {
        dimensionLables.forEach((dim, vl) -> {
            if (vl.isEmpty()) {
                return;
            }
            String ids = StringUtils.join(vl.keySet(), ',');
            String sql = null;
            switch (dim) {
                case "adn_id":
                    sql = "select id,class_name label from om_adnetwork where id in (" + ids + ")";
                    break;
                case "pub_app_id":
                    sql = "select id,app_id label from om_publisher_app where id in (" + ids + ")";
                    break;
                case "placement_id":
                    sql = "select id,name label from om_placement where id in (" + ids + ")";
                    break;
                case "instance_id":
                    sql = "select id,name label from om_instance where id in (" + ids + ")";
                    break;
                default:
                    break;
            }
            if (sql != null) {
                jdbcTemplate.query(sql, rs -> {
                    vl.put(rs.getLong("id"), rs.getString("label"));
                });
            }
        });
    }

    private List<String> buildLines(ReportBuilderDTO builder, List<Map<String, Object>> results,
                                    Map<String, Map<Object, Object>> dimensionLables) {

        Set<String> dimensionSet = builder.getDimensionSet();
        Set<String> metricSet = builder.getMetricSet();

        String requestField, filledField;
        if (dimensionSet.contains("adn_id") || StringUtils.isNotBlank(builder.getCondAdnList())
                || dimensionSet.contains("instance_id") || StringUtils.isNotBlank(builder.getCondInstanceList())) {
            requestField = "instance_request";
            filledField = "instance_filled";
        } else {
            requestField = "waterfall_request";
            filledField = "waterfall_filled";
        }

        List<String> reportResults = new ArrayList<>();
        List<String> lineValues = new ArrayList<>(dimensionSet.size() + metricSet.size());

        for (String dimension : dimensionSet) {
            lineValues.add(TITLES.getOrDefault(dimension, dimension));
        }
        for (String metric : metricSet) {
            lineValues.add(TITLES.getOrDefault(metric, metric));
        }

        reportResults.add(StringUtils.join(lineValues, SPLIT_CHAR));
        lineValues.clear();

        for (Map<String, Object> result : results) {
            JSONObject row = new JSONObject(result);
            long request = row.getLongValue(requestField);
            long filled = row.getLongValue(filledField);
            long impr = row.getLongValue("impr");
            long click = row.getLongValue("click");
            long dau = row.getLongValue("dau");
            long deu = row.getLongValue("deu");
            BigDecimal revenue = row.getBigDecimal("cost");

            row.put("request", request);
            row.put("filled", filled);

            if (metricSet.contains("fill_rate")) {
                BigDecimal fillRate = BigDecimal.ZERO;
                if (request != 0) {
                    fillRate = divide(filled, request);
                    if (fillRate.compareTo(d100) > 0) {
                        fillRate = d100;
                    }
                } else if (filled > 0) {
                    fillRate = d100;
                }
                row.put("fill_rate", fillRate);
            }

            if (metricSet.contains("is_ready") || metricSet.contains("is_ready_rate")) {
                long isReadyTrue = row.getLongValue("is_ready_true");
                long isReadyFalse = row.getLongValue("is_ready_false");
                long isReady = isReadyTrue + isReadyFalse;
                row.put("is_ready", isReady);
                row.put("is_ready_rate", divide(isReadyTrue, isReady));
            }

            if (metricSet.contains("ctr")) {
                if (impr > 0) {
                    row.put("ctr", divide(click, impr));
                } else if (click > 0) {
                    row.put("ctr", "1");
                }
            }

            if (metricSet.contains("ecpm") && revenue != null) {
                row.put("ecpm", divide(revenue.multiply(d1000), impr));
            }
            if (metricSet.contains("impr_dau")) {
                row.put("impr_dau", divide(impr, dau));
            }
            if (metricSet.contains("impr_deu")) {
                row.put("impr_deu", divide(impr, deu));
            }
            if (metricSet.contains("arp_dau")) {
                row.put("arp_dau", divide(revenue, dau));
            }
            if (metricSet.contains("arp_deu")) {
                row.put("arp_deu", divide(revenue, deu));
            }
            if (metricSet.contains("engagement_rate")) {
                row.put("engagement_rate", divide(deu, dau));
            }

            // put values into line
            for (String dimension : dimensionSet) {
                Object v = row.get(dimension);
                Map<Object, Object> vl = dimensionLables.get(dimension);
                if (vl != null && v != null) {
                    v = vl.getOrDefault(TypeUtils.castToLong(v), v);
                }
                putValue(lineValues, v, "");
            }
            for (String metric : metricSet) {
                putValue(lineValues, row.get(metric), "0");
            }
            reportResults.add(StringUtils.join(lineValues, SPLIT_CHAR));
            lineValues.clear();
        }
        return reportResults;
    }

    private void putValue(List<String> lineValues, Object value, String defaultValue) {
        lineValues.add(value == null ? defaultValue : value.toString());
    }

    List<Map<String, Object>> getAdNetWorkReport(ReportBuilderDTO builder) {
        String sumClause = " sum(api_request) as api_request,sum(api_filled) as api_filled,sum(api_click) as click, " +
                "sum(api_impr) as impr, sum(api_video_start) as api_video_start,sum(api_video_complete) as " +
                "api_video_complete, sum(revenue) as revenue, sum(cost) as cost ";

        String reportSql = buildStatSql(builder, "stat_adnetwork", sumClause);
        LOG.info("Ad network report sql {}", reportSql);
        List<Map<String, Object>> report = jdbcTemplate.queryForList(reportSql);
        LOG.info("Ad network report size: {}", report.size());
        return report;
    }

    List<Map<String, Object>> getLRReport(ReportBuilderDTO builder) {
        String sumClause = " sum(waterfall_request) as waterfall_request,sum(waterfall_filled) as waterfall_filled," +
                "sum(instance_request) as instance_request, sum(instance_filled) as instance_filled," +
                "sum(video_start) as video_start,sum(video_complete) as video_complete,sum(called_show) as called_show," +
                "sum(is_ready_true) as is_ready_true,sum(is_ready_false) as is_ready_false ";

        String reportSql = buildStatSql(builder, "stat_lr", sumClause);
        LOG.info("LR network report sql {}", reportSql);
        List<Map<String, Object>> report = jdbcTemplate.queryForList(reportSql);
        LOG.info("LR network report size: {}", report.size());
        return report;
    }

    public List<Map<String, Object>> getDauReport(ReportBuilderDTO builder) {
        if (builder.getDataSource() != DataSourceType.USER_ACTIVITY.ordinal())
            return Collections.emptyList();
        String reportSql = buildDauStatSql(builder);
        LOG.info("Dau network report sql {}", reportSql);
        List<Map<String, Object>> report = jdbcTemplate.queryForList(reportSql);
        LOG.info("Dau network report size: {}", report.size());
        return report;
    }

    private String buildDauStatSql(ReportBuilderDTO builder) {
        try {
            Set<String> dauAllowDimensions = new HashSet<>(Arrays.asList("pub_app_id", "country", "day"));
            Set<String> dauDimensionSet = new LinkedHashSet<>();
            for (String dimension : builder.getDimensionSet()) {
                if (dauAllowDimensions.contains(dimension)) {
                    dauDimensionSet.add(dimension);
                }
            }
            String dauDimensions = StringUtils.join(dauDimensionSet, ',');
            StringBuilder statSql = new StringBuilder();
            statSql.append("select ").append(dauDimensions).append(",");
            statSql.append(" sum(ip_count) as ip_count,sum(dau) as dau,sum(deu) as deu");
            statSql.append(" from stat_dau where 1=1 ");
            addDayCondition(builder, statSql);

            if (StringUtils.isNotBlank(builder.getCondPubAppList())) {
                statSql.append(" and pub_app_id in(").append(builder.getCondPubAppList()).append(")");
            }
            addCountryCondition(builder, statSql);
            statSql.append(" group by ");
            statSql.append(dauDimensions);
            LOG.info("Build ad network stat sql: {}", statSql.toString());
            return statSql.toString();
        } catch (Exception e) {
            LOG.error("Build stat sql error: {}", JSONObject.toJSONString(builder), e);
        }
        return null;
    }

    private String buildStatSql(ReportBuilderDTO builder, String tableName, String sumClause) {
        try {
            StringBuilder statSql = new StringBuilder();
            statSql.append("select ").append(builder.getDimensions()).append(",");
            statSql.append(sumClause);
            statSql.append(" from ").append(tableName).append(" where 1=1 ");
            addDayCondition(builder, statSql);
            if (StringUtils.isNotBlank(builder.getCondAdnList())) {
                statSql.append(" and adn_id in(").append(builder.getCondAdnList()).append(")");
            }
            if (StringUtils.isNotBlank(builder.getCondPubAppList())) {
                statSql.append(" and pub_app_id in(").append(builder.getCondPubAppList()).append(")");
            }
            if (StringUtils.isNotBlank(builder.getCondPlacementList())) {
                statSql.append(" and placement_id in(").append(builder.getCondPlacementList()).append(")");
            }
            if (StringUtils.isNotBlank(builder.getCondInstanceList())) {
                statSql.append(" and instance_id in(").append(builder.getCondInstanceList()).append(")");
            }
            addCountryCondition(builder, statSql);
            statSql.append(" group by ");
            statSql.append(builder.getDimensions());

            return statSql.toString();
        } catch (Exception e) {
            LOG.error("Build stat sql error: {}", JSON.toJSONString(builder), e);
        }
        return null;
    }

    List<ReportBuilderDTO> getReportBuilders() {
        String sql = "SELECT id,publisher_id,name,data_source,recipients,schedule,weekly_day,dimensions,metrics," +
                "cond_day_period,cond_adn_list,cond_pub_app_list,cond_placement_list,cond_instance_list," +
                "cond_country_list,creator_id,execute_times,last_execute_time,create_time,lastmodify" +
                " FROM om_report_builder where status = 1";
        return jdbcTemplate.query(sql, ReportBuilderDTO.ROWMAPPER);
    }

    private void addDayCondition(ReportBuilderDTO builder, StringBuilder statSql) {
        LocalDate now = LocalDate.now();
        LocalDate lastDay = builder.getTaskDay().plusDays(-1);
        LocalDate beginDay = builder.getTaskDay().plusDays(-(builder.getCondDayPeriod() + 1));
        switch (builder.getSchedule()) {
            case SCHEDULE_DAILY:
            case SCHEDULE_WEEKLY:
                statSql.append(" and day >='").append(FMT_DATE.format(beginDay)).append("'");
                statSql.append(" and day <='").append(FMT_DATE.format(lastDay)).append("'");
                break;
            case SCHEDULE_MONTHLY:
                LocalDate firstDayOfLastMonth = now.plusMonths(-1).withDayOfMonth(1);
                LocalDate lastDayOfLastMonth = now.withDayOfMonth(1).plusDays(-1);
                statSql.append(" and day >='").append(FMT_DATE.format(firstDayOfLastMonth)).append("'");
                statSql.append(" and day <='").append(FMT_DATE.format(lastDayOfLastMonth)).append("'");
                break;
            default:
                statSql.append(" and day = CURRENT_DATE");
                LOG.warn("scheduleType error: {}", builder.getSchedule());
        }
    }

    private void addCountryCondition(ReportBuilderDTO builder, StringBuilder statSql) {
        if (StringUtils.isNotBlank(builder.getCondCountryList())) {
            String countries = StringUtils.join(builder.getCondCountryList().split(SPLIT_CHAR), "','");
            statSql.append(" and country in('").append(countries).append("')");
        }
    }

    private BigDecimal divide(long a, long b) {
        return b != 0 ?
                new BigDecimal(a).divide(new BigDecimal(b), 6, RoundingMode.HALF_UP)
                : BigDecimal.ZERO;
    }

    private BigDecimal divide(BigDecimal a, long b) {
        if (a == null || b == 0)
            return BigDecimal.ZERO;
        return a.divide(new BigDecimal(b), 6, RoundingMode.HALF_UP);
    }

}
