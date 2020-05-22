package com.cmos.ngmc.service.impl.es;

import com.alibaba.dubbo.config.annotation.Service;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * @Author: wlc
 * @Date: 2018/4/17 16:08
 * @Description:
 **/
@Service(group = "ngmc")
public class ESMustNot implements IESQueryBuilders{

    private transient BoolQueryBuilder boolQueryBuilder;

    public BoolQueryBuilder getBoolQueryBuilder() {
        return boolQueryBuilder;
    }

    @Override
    public ESMustNot termQuery(String name, String valve) {
        boolQueryBuilder.mustNot(QueryBuilders.termQuery(name, valve));
        return this;
    }

    @Override
    public ESMustNot wildcardQuery(String name, String valve) {
        boolQueryBuilder.mustNot(QueryBuilders.wildcardQuery(name, valve));
        return this;
    }
}
