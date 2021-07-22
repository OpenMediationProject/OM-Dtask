// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.util;

import org.apache.commons.lang3.math.NumberUtils;
import org.springframework.util.StringUtils;

import javax.servlet.http.HttpServletRequest;
import java.math.BigDecimal;
import java.util.Locale;
import java.util.Map;

public class Util {

    /**
     * Convert a name in camelCase to an underscored name in lower case.
     * Any upper case letters are converted to lower case with a preceding underscore.
     *
     * @param name the original name
     * @return the converted name
     * @see #lowerCaseName
     */
    public static String underscoreName(String name) {
        if (!StringUtils.hasLength(name)) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        result.append(lowerCaseName(name.substring(0, 1)));
        for (int i = 1; i < name.length(); i++) {
            String s = name.substring(i, i + 1);
            String slc = lowerCaseName(s);
            if (!s.equals(slc)) {
                result.append("_").append(slc);
            } else {
                result.append(s);
            }
        }
        return result.toString();
    }

    /**
     * Convert the given name to lower case.
     * By default, conversions will happen within the US locale.
     *
     * @param name the original name
     * @return the converted name
     */
    public static String lowerCaseName(String name) {
        return name.toLowerCase(Locale.US);
    }

    public static String getClientIP(HttpServletRequest req) {
        String clientIP = req.getHeader("X-Real-IP");
        if (clientIP == null) {
            clientIP = req.getRemoteAddr();
        }
        return clientIP;
    }

    public static String getString(Map<String, Object> map, String key) {
        if (map == null || map.isEmpty()) return null;
        Object obj = map.get(key);
        if (obj != null) {
            return obj.toString();
        }
        return null;
    }

    public static int getInt(Map<String, Object> map, String key) {
        if (map == null || map.isEmpty()) return 0;
        Object obj = map.get(key);
        if (obj != null) {
            return NumberUtils.toInt(obj.toString());
        }
        return 0;
    }

    public static BigDecimal getBigDecimal(Map<String, Object> map, String key) {
        Object o = map.get(key);
        if (o == null) return null;
        if (o instanceof BigDecimal) return (BigDecimal) o;
        return new BigDecimal(o.toString());
    }

}