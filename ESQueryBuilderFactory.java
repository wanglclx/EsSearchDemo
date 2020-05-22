package com.cmos.ngmc.service.impl.es;

/**
 * @Author: wlc
 * @Date: 2018/4/17 19:33
 * @Description:
 **/
public class ESQueryBuilderFactory {
    public static IESQueryBuilders creatESQueryBuilder(ESQuaryMode mode) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        Class<?> clazz = Class.forName(mode.mode());
        return (IESQueryBuilders) clazz.newInstance();
    }
}
