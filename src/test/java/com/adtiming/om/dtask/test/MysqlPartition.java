package com.adtiming.om.dtask.test;

import org.junit.Test;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class MysqlPartition {

    @Test
    public void create() {
        String table = "stat_adnetwork";
        DateTimeFormatter fmtDays = DateTimeFormatter.ofPattern("yyyy-MM-dd");
        DateTimeFormatter fmtDate = DateTimeFormatter.ofPattern("yyyyMMdd");

        System.out.printf("alter table %s add partition(\n", table);
        LocalDate day = LocalDate.of(2020, 1, 1);
        while (true) {
            String date_str = fmtDate.format(day);
            day = day.plusDays(1);
            String day_str = fmtDays.format(day);
            System.out.printf("  partition p%s values less than (to_days('%s'))", date_str, day_str);
            if (day.getYear() < 2019) {
                System.out.print(",\n");
            } else {
                System.out.print("\n");
                break;
            }
        }
        System.out.print(");\n");
    }

    @Test
    public void drop() {
        String table = "stat_adnetwork";
        DateTimeFormatter fmt_date = DateTimeFormatter.ofPattern("yyyyMM");

        LocalDate day = LocalDate.of(2017, 12, 1);
        while (day.getYear() < 2021) {
            String date_str = fmt_date.format(day);
            System.out.printf("alter table %s drop partition p%s;\n", table, date_str);
            day = day.plusMonths(1);
        }
    }

}
