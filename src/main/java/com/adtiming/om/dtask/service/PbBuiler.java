// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.service;

import com.google.protobuf.MessageLite;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

public abstract class PbBuiler {

    private static final Logger log = LogManager.getLogger();
    public static final BigDecimal d1000 = new BigDecimal("1000");

    @FunctionalInterface
    public interface PBWriter {
        void writeDelimited(MessageLite ml);
    }

    public List<String> str2list(String str) {
        return str2list(str, String::toString);
    }

    public List<String> str2list(String str, Function<String, String> mapper) {
        if (StringUtils.isEmpty(str))
            return Collections.emptyList();
        return Stream.of(str.split("[,\r\n]"))
                .map(String::trim)
                .filter(StringUtils::isNotBlank)
                .map(mapper)
                .collect(Collectors.toList());
    }

    public void build(String name, File dir, Consumer<PBWriter> fn) {
        File src = new File(dir, name + ".gz.tmp");
        File dst = new File(dir, name + ".gz");
        log.info("start build {}", name);
        long start = System.currentTimeMillis();

        OutputStream out = null;
        try {
            out = new GZIPOutputStream(new FileOutputStream(src));
            OutputStream _out = out;
            fn.accept(ml -> {
                try {
                    ml.writeDelimitedTo(_out);
                } catch (IOException e) {
                    log.error("write {} pb error", name, e);
                }
            });
            out.close();
            out = null;
            if (!src.renameTo(dst)) {
                log.error("mv {} to {} error", src, dst);
            }
        } catch (Exception e) {
            log.error("build {} error", name, e);
        } finally {
            IOUtils.closeQuietly(out);
        }
        log.debug("build {} finished, cost {} ms", name, System.currentTimeMillis() - start);
    }
}
