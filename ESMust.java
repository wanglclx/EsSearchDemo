package com.cmos.ngmc.service.impl.es;

import com.alibaba.dubbo.config.annotation.Service;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * @Author: wlc
 * @Date: 2018/4/17 16:07
 * @Description:
 **/
@Service(group = "ngmc")
public class ESMust implements IESQueryBuilders {

    private transient BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
    public BoolQueryBuilder getBoolQueryBuilder() {
        return boolQueryBuilder;
    }
    @Override
    public ESMust termQuery(String name, String valve) {
        boolQueryBuilder.must(QueryBuilders.termQuery(name, valve));
        return this;
    }

    @Override
    public ESMust wildcardQuery(String name, String valve) {
        boolQueryBuilder.must(QueryBuilders.wildcardQuery(name, "*" + valve + "*"));
        return this;
    }
}
