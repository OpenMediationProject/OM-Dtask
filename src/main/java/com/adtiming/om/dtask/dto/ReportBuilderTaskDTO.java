package com.adtiming.om.dtask.dto;

import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.RowMapper;

import java.time.LocalDate;
import java.time.LocalDateTime;

public class ReportBuilderTaskDTO {

    public static final RowMapper<ReportBuilderTaskDTO> ROWMAPPER = BeanPropertyRowMapper.newInstance(ReportBuilderTaskDTO.class);

    private int id;
    private LocalDate day;
    private int builderId;
    private int status;//0:init,1:success,2:failed,3:builder stoped
    private int reparationCount;
    private int reportLineSize;
    private LocalDateTime createTime;
    private LocalDateTime lastmodify;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public LocalDate getDay() {
        return day;
    }

    public void setDay(LocalDate day) {
        this.day = day;
    }

    public int getBuilderId() {
        return builderId;
    }

    public void setBuilderId(int builderId) {
        this.builderId = builderId;
    }

    public int getStatus() {
        return status;
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public int getReparationCount() {
        return reparationCount;
    }

    public void setReparationCount(int reparationCount) {
        this.reparationCount = reparationCount;
    }

    public int getReportLineSize() {
        return reportLineSize;
    }

    public void setReportLineSize(int reportLineSize) {
        this.reportLineSize = reportLineSize;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }

    public LocalDateTime getLastmodify() {
        return lastmodify;
    }

    public void setLastmodify(LocalDateTime lastmodify) {
        this.lastmodify = lastmodify;
    }
}
