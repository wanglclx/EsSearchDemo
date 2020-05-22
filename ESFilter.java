package com.cmos.ngmc.service.impl.es;

import com.alibaba.dubbo.config.annotation.Service;
import org.elasticsearch.index.query.BoolQueryBuilder;

/**
 * @Author: wlc
 * @Date: 2018/4/17 21:10
 * @Description:
 **/
@Service(group = "ngmc")
public class ESFilter implements IESQueryBuilders {
    private transient BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
    public BoolQueryBuilder getBoolQueryBuilder() {
        return boolQueryBuilder;
    }

    @Override
    public ESFilter termQuery(String name, String valve) {
        return this;
    }

    @Override
    public ESFilter wildcardQuery(String name, String valve) {
        return this;
    }
}
