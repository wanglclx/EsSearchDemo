package com.cmos.ngmc.service.impl.es;

import org.elasticsearch.search.sort.SortOrder;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * elasticsearch 查询辅助构造工具
 * @author GavinCook
 * @date 2016-11-28
 * @since 1.0.0
 */
public class ESQuery implements Serializable{

    /**
     * 查询条件
     */
    private List<ESCondition> esConditions = new ArrayList<>();

    /**
     * 要查询出的字段
     */
    private List<String> includeFields = new ArrayList<>();

    /**
     * 需要过滤掉的字段
     */
    private List<String> excludeFields = new ArrayList<>();

    /**
     * 需要高亮的字段
     */
    private List<String> highlightFields = new ArrayList<>();

    /**
     * 需要查询的type，默认查询index下所有的type
     */
    private String[] types;

    /**
     * 需要排序
     */
    private Map<String, SortOrder> orderMap; // 排序标识


    public Map<String, SortOrder> getOrderMap() {
        return orderMap;
    }


    public void addOrderMap(String field, SortOrder order){
        if (getOrderMap() == null) {
            orderMap = new HashMap<String, SortOrder>();
        }
        orderMap.put(field, order);
    }

    public static ESQuery get(){
        return new ESQuery();
    }

    /**
     * 结果需要查询出的字段，默认查询所有字段
     * @param fields
     * @return
     */
    public ESQuery fields(String...fields){
		if (fields == null) {
            return this;
        }
        for(String field : fields){
            includeFields.add(field);
        }
        return this;
    }

    /**
     * 需要过滤掉的字段
     * @param fields
     * @return
     */
    public ESQuery excludeFields(String...fields){
		if (fields == null) {
            return this;
        }
        for(String field : fields){
            excludeFields.add(field);
        }
        return this;
    }

    /**
     * 增加需要高亮的字段
     * @param fields
     * @return
     */
    public ESQuery highlight(String...fields){
		if (fields == null) {
            return this;
        }
        for(String field : fields){
            highlightFields.add(field);
        }
        return this;
    }

    public ESQuery condition(ESCondition esCondition){
        esConditions.add(esCondition);
        return this;
    }

    public String[] getTypes() {
        return types;
    }

    public ESQuery setTypes(String... types) {
        this.types = types;
        return this;
    }

    public String[] getIncludeFields(){
        return includeFields.toArray(new String[includeFields.size()]);
    }

    public String[] getExcludeFields(){
        return excludeFields.toArray(new String[excludeFields.size()]);
    }

    public String[] getHighlightFields(){
        return highlightFields.toArray(new String[highlightFields.size()]);
    }

    public String toQueryString() {
        StringBuilder queryBuilder = new StringBuilder();

        boolean hasCondition = false;
        for(ESCondition esCondition : esConditions){
            if(hasCondition){
                queryBuilder.append(",");
            }
            queryBuilder.append(esCondition.toString());
            hasCondition = true;
        }

        queryBuilder.insert(0, "{\"bool\":{\"must\":[");
        queryBuilder.append("] } }  ");

        return queryBuilder.toString();
    }
}
