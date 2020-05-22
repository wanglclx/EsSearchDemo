package com.cmos.ngmc.service.impl.es;

import com.cmos.ngmc.util.EsUtil;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

public abstract class BaseMapping {

    /**
     * 创建index入口
     * @param
     * @param type
     * @param settingMap
     * @throws IOException
     * @throws Exception
     */
    public void buildIndexMapping(String indexName, String type, Map<String, String> settingMap)
        throws  IOException {
        // 查询索引是否存在，若存在，则只创建类型
        if (EsUtil.isExistsIndex(indexName)) {
            //类型是否存在
            if(EsUtil.isExistsType(indexName, type)){
                //throw new EcpCoreException("指定的索引类型已经存在，无法创建或更新！");
            }
            // 仅创建mapping
            PutMappingRequest mappingRequest = Requests.putMappingRequest(indexName).type(type)
                    .source(createMapping(type));
            EsUtil.getClient().admin().indices().putMapping(mappingRequest).actionGet();
        } else {
            // 在本例中主要得注意,ttl及timestamp如何用java ,这些字段的具体含义,请去到es官网查看
            CreateIndexRequestBuilder cib = EsUtil.getClient().admin().indices().prepareCreate(indexName);
            // 设置该index的Mapping，可暂时不设置，后面建完index之后再设置也可
            cib.addMapping(type, createMapping(type));
            // 备注：创建index时，完全可不用设置setting内容，系统会默认自动创建
            cib.setSettings(createSetting(settingMap));
            // 执行创建index请求
            cib.execute().actionGet();
        }

    }

    /**
     *  设置index的Settings
     *  number_of_shards 分片数量
     *  number_of_replicas 副本数量
     *  version.created 创建该index用到的Elasticsearch版本
     *  version.created_string
     *  creating_data 索引创建日期
     *
     * @return
     */
    public Settings createSetting(Map<String, String> settingMap) {
        Settings settings = Settings.builder().put(settingMap).build();
        return settings;
    }

    /**
     * 设置index的Mappings，具体内容在后面解释
     * @param type
     * @return
     * @throws IOException
     */
    public abstract XContentBuilder createMapping(String type) throws IOException;

}
