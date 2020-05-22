package com.cmos.ngmc.service.impl.es;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;

public class ProdDetailMapping extends BaseMapping {
    /**
     * 存储商品信息基本信息
     * @param type
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder createMapping(String type) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startObject(type).field("dynamic","strict")
                .startObject("properties")
                .startObject("mcdsId").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("mcdsNm").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("baseCatgUnitId").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("baseProdCatgNm").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("mcdsUnitSubclsTypeCd").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("prodCatgId").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("prodCatgNm").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("crtAppSysId").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("mcdsUnitDesc").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("crtTime").field("type", "date").field("index", "not_analyzed")
                .field("format", "yyy-MM-dd HH:mm:ss").endObject()
                .startObject("bgnEffDate").field("type", "date").field("index", "not_analyzed")
                .field("format", "yyy-MM-dd HH:mm:ss").endObject()
                .startObject("mcdsPicUrl").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("validStsCdNm").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("ptyNm").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("argeSeqno").field("type", "integer").field("index", "not_analyzed").endObject()
                .startObject("slsupptFlag").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("valIdStsCd").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("brandId").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("brandNm").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("lowPrice").field("type", "double").field("index", "not_analyzed").endObject()
                .startObject("topPrice").field("type", "double").field("index", "not_analyzed").endObject()
                .startObject("lowRmnrtn").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("topRmnrtn").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("mrctId").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("mrctNm").field("type", "String").field("index", "not_analyzed").endObject()
    			.startObject("chnlId").field("type", "String").field("index", "not_analyzed").endObject()
    			.startObject("skuQty").field("type", "integer").field("index", "not_analyzed").endObject()
    			.startObject("cnspQty").field("type", "integer").field("index", "not_analyzed").endObject()
    			.startObject("rngIds").field("type", "String").field("index", "not_analyzed").endObject()
    			.startObject("suitBizId").field("type", "String").field("index", "not_analyzed").endObject()
    			.startObject("cronSignNm").field("type", "String").field("index", "not_analyzed").endObject()
    			.startObject("cronSignShwstyCd").field("type", "String").field("index", "not_analyzed").endObject()
    			.startObject("cronSignColCode").field("type", "String").field("index", "not_analyzed").endObject()
                .startObject("shopBckgrdPicAddr").field("type", "String").field("index", "not_analyzed").endObject()
                .endObject().endObject().endObject();
        return builder;
    }

}
