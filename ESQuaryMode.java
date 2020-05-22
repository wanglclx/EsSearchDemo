package com.cmos.ngmc.service.impl.es;

import java.io.Serializable;

/**
 * @Author: wlc
 * @Date: 2018/4/17 19:32
 * @Description:定义ES查询模式
 */
public enum ESQuaryMode implements Serializable{
		MUST("com.cmos.ngmc.service.impl.es.ESMust"),
        MUST_NOT("com.cmos.ngmc.service.impl.es.ESMustNot"),
        SHOULD("com.cmos.ngmc.service.impl.es.ESShould"),
        FILTER("com.cmos.ngmc.service.impl.es.ESFilter");

    private String mode;

    ESQuaryMode(String mode) {
        this.mode = mode;
    }

    public String mode() {
        return mode;
    }
}
