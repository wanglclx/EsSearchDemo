package com.cmos.ngmc.service.impl.es;

import com.alibaba.dubbo.config.annotation.Service;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * @Author: wlc
 * @Date: 2018/4/17 16:10
 * @Description:
 **/
@Service(group = "ngmc")
public class ESShould implements IESQueryBuilders {
    private transient BoolQueryBuilder boolQueryBuilder;

    public BoolQueryBuilder getBoolQueryBuilder() {
        return boolQueryBuilder;
    }

    @Override
    public ESShould termQuery(String name, String valve) {
        return this;
    }

    @Override
    public ESShould wildcardQuery(String name, String valve) {
        boolQueryBuilder.should();
        return this;
    }
}
