// Copyright 2020 ADTIMING TECHNOLOGY COMPANY LIMITED
// Licensed under the GNU Lesser General Public License Version 3

package com.adtiming.om.dtask.cloud;

import org.springframework.web.context.support.GenericWebApplicationContext;

public interface CloudClient {

    default void registerBeans(GenericWebApplicationContext context) {
    }

    CloudClient CLIENT0 = new CloudClient() {
    };
}
