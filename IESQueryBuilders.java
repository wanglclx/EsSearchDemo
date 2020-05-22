package com.cmos.ngmc.service.impl.es;


/**
 * @Author: wlc
 * @Date: 2018/4/17 16:08
 * @Description:
 **/
public interface IESQueryBuilders {
    IESQueryBuilders termQuery(String name, String valve);
    IESQueryBuilders wildcardQuery(String name, String valve);
}
