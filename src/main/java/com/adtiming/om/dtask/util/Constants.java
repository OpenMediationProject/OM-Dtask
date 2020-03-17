package com.adtiming.om.dtask.util;

import java.time.format.DateTimeFormatter;

public class Constants {
    public static final DateTimeFormatter FORMATTER_HH = DateTimeFormatter.ofPattern("HH");
    public static final DateTimeFormatter FORMATTER_DD = DateTimeFormatter.ofPattern("dd");
    public static final DateTimeFormatter FORMATTER_MM = DateTimeFormatter.ofPattern("MM");
    public static final DateTimeFormatter FORMATTER_YYYY = DateTimeFormatter.ofPattern("yyyy");
    public static final DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");
    public static final DateTimeFormatter FORMATTER_YYYYMMDDHH = DateTimeFormatter.ofPattern("yyyyMMddHH");
    public static final DateTimeFormatter FORMATTER_SPLIT_SLASH_YYYY_MM_DD = DateTimeFormatter.ofPattern("yyyy/MM/dd");
}
