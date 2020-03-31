// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.dto;

import com.adtiming.om.dtask.util.Util;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

public class ReportBuilderDTO {

    public static final RowMapper<ReportBuilderDTO> ROWMAPPER = BeanPropertyRowMapper.newInstance(ReportBuilderDTO.class);

    private int id;
    private LocalDate taskDay;
    private int publisherId;
    private String name;
    private int dataSource;
    private String recipients;
    private int schedule;
    private int weeklyDay;
    private int status;

    //day,country,adnId,pubAppId,placementId,instanceId
    private String dimensions;
    private Set<String> dimensionSet;

    // request,filled,fillRate,dau,deu,arpDau,arpDeu,engagementRate,impr,click,ctr
    private String metrics;
    private Set<String> metricSet;

    private int condDayPeriod;
    private String condAdnList;
    private String condCountryList;
    private String condPubAppList;
    private String condPlacementList;
    private String condInstanceList;
    private Integer creatorId;
    private Integer executeTimes;

    private LocalDateTime lastExecuteTime;
    private LocalDateTime lastmodify;
    private LocalDateTime createTime;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public LocalDate getTaskDay() {
        return taskDay;
    }

    public void setTaskDay(LocalDate taskDay) {
        this.taskDay = taskDay;
    }

    public int getPublisherId() {
        return publisherId;
    }

    public void setPublisherId(int publisherId) {
        this.publisherId = publisherId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getDataSource() {
        return dataSource;
    }

    public void setDataSource(int dataSource) {
        this.dataSource = dataSource;
    }

    public String getRecipients() {
        return recipients;
    }

    public void setRecipients(String recipients) {
        this.recipients = recipients;
    }

    public int getSchedule() {
        return schedule;
    }

    public void setSchedule(int schedule) {
        this.schedule = schedule;
    }

    public int getWeeklyDay() {
        return weeklyDay;
    }

    public void setWeeklyDay(int weeklyDay) {
        this.weeklyDay = weeklyDay;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    /**
     * @return underscored dimensions
     */
    public String getDimensions() {
        return dimensions;
    }

    /**
     * @return underscored dimension Set
     */
    public Set<String> getDimensionSet() {
        return dimensionSet;
    }

    /**
     * set and underscore dimensions
     *
     * @param dimensions
     */
    public void setDimensions(String dimensions) {
        this.dimensions = Util.underscoreName(dimensions);
        this.dimensionSet = new LinkedHashSet<>(Arrays.asList(this.dimensions.split(",")));
    }

    /**
     * @return underscored metric Set
     */
    public Set<String> getMetricSet() {
        return metricSet;
    }

    /**
     * @return underscored metrics
     */
    public String getMetrics() {
        return metrics;
    }

    /**
     * set and underscore metrics
     *
     * @param metrics
     */
    public void setMetrics(String metrics) {
        this.metrics = Util.underscoreName(metrics);
        this.metricSet = new LinkedHashSet<>(Arrays.asList(this.metrics.split(",")));
    }

    public int getCondDayPeriod() {
        return condDayPeriod;
    }

    public void setCondDayPeriod(int condDayPeriod) {
        this.condDayPeriod = condDayPeriod;
    }

    public String getCondAdnList() {
        return condAdnList;
    }

    public void setCondAdnList(String condAdnList) {
        this.condAdnList = condAdnList;
    }

    public String getCondCountryList() {
        return condCountryList;
    }

    public void setCondCountryList(String condCountryList) {
        this.condCountryList = condCountryList;
    }

    public String getCondPubAppList() {
        return condPubAppList;
    }

    public void setCondPubAppList(String condPubAppList) {
        this.condPubAppList = condPubAppList;
    }

    public String getCondPlacementList() {
        return condPlacementList;
    }

    public void setCondPlacementList(String condPlacementList) {
        this.condPlacementList = condPlacementList;
    }

    public String getCondInstanceList() {
        return condInstanceList;
    }

    public void setCondInstanceList(String condInstanceList) {
        this.condInstanceList = condInstanceList;
    }

    public Integer getCreatorId() {
        return creatorId;
    }

    public void setCreatorId(Integer creatorId) {
        this.creatorId = creatorId;
    }

    public Integer getExecuteTimes() {
        return executeTimes;
    }

    public void setExecuteTimes(Integer executeTimes) {
        this.executeTimes = executeTimes;
    }

    public LocalDateTime getLastExecuteTime() {
        return lastExecuteTime;
    }

    public void setLastExecuteTime(LocalDateTime lastExecuteTime) {
        this.lastExecuteTime = lastExecuteTime;
    }

    public LocalDateTime getLastmodify() {
        return lastmodify;
    }

    public void setLastmodify(LocalDateTime lastmodify) {
        this.lastmodify = lastmodify;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }
}