package com.adtiming.om.dtask.util;

import com.adtiming.om.pb.CrossPromotionPB;
import com.adtiming.om.pb.CrossPromotionPB.Range;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * 解析区间表达式, 支持多个区间和单值的组合, 使用分号分隔
 * <p>
 * 区间示例:
 * {@code (a,b)}   {@code {x | a < x < b}}
 * {@code [a,b]}   {@code {x | a <= x <= b}}
 * {@code (a,b]}   {@code {x | a < x <= b}}
 * {@code [a,b)}   {@code {x | a <= x < b}}
 * {@code (a,+∞)}  {@code {x | x > a}}
 * {@code [a,+∞)}  {@code {x | x >= a}}
 * {@code (-∞,b)}  {@code {x | x < b}}
 * {@code (-∞,b]}  {@code {x | x <= b}}
 * {@code (-∞,+∞)} {@code {x}}
 * <p>
 * 其中无穷 ∞ 字符只是为了方便表示, 真实表达式需要留空
 * <p>
 * 多区间组合示例: {@code (,c); e; [f, h); m; [x,] }
 */
public class RangeExp {

    private final String exp;
    private List<String> items;
    private List<Range> ranges;

    public RangeExp(String exp) {
        this.exp = exp;
        if (StringUtils.isBlank(exp)) {
            return;
        }
        String[] arr = exp.split(";");
        for (String s : arr) {
            s = s.trim();
            if (s.length() == 0) {
                continue;
            }

            if (s.charAt(0) == '[' || s.charAt(0) == '(') {
                if (ranges == null) {
                    ranges = new ArrayList<>();
                }
                Range.Builder r = Range.newBuilder();
                r.setMinType(s.charAt(0) == '('); // OPEN
                r.setMaxType(s.charAt(s.length() - 1) == ')');
                s = s.substring(1, s.length() - 1);
                String[] m = s.split(",");

                String min = m[0].trim();
                if (min.length() > 0) {
                    r.setMin(min);
                }
                if (m.length > 1) {
                    String max = m[1].trim();
                    if (max.length() > 0) {
                        r.setMax(max);
                    }
                }

                ranges.add(r.build());

            } else {
                if (items == null) {
                    items = new ArrayList<>();
                }
                items.add(s);
            }
        }
    }

    public String getExp() {
        return exp;
    }

    public boolean hasItems() {
        return items != null;
    }

    public List<String> getItems() {
        return items;
    }

    public boolean hasRange() {
        return ranges != null;
    }

    public List<Range> getRanges() {
        return ranges;
    }

}
