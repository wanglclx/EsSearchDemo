package com.cmos.ngmc.service.impl.es;

import com.cmos.common.exception.GeneralException;
import com.cmos.core.logger.Logger;
import com.cmos.core.logger.LoggerFactory;
import com.cmos.ngmc.iservice.es.IEsService;
import com.cmos.ngmc.util.EsUtil;
import com.cmos.ngmc.util.JsonUtil;
import com.cmos.ngmc.util.StringUtil;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsRequest;
import org.elasticsearch.action.admin.indices.exists.types.TypesExistsResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortOrder;

import java.util.*;
import java.util.Map.Entry;

/**
 * 获取es公共方法
 * 1、提供增删改查基本常用调用方法；
 * 2、ES最重要的搜索有两种：根据filter进行搜索；根据query进行搜索；
 * 3、常用类：IndexResponse、UpdateResponse、SearchResponse、BulkRequestBuilder、BulkResponse、QueryBuilder、QueryBuilders
 * @author Administrator
 *
 */
public class EsServiceImpl implements IEsService {
    private static final Logger logger = LoggerFactory.getServiceLog(EsServiceImpl.class);

    /**
     * 区分模糊查询、精确查询、分词查询
     * @author Administrator
     *
     */
    private enum Type {
        jq, // 精确查询
        mh, // 模糊查询
        fc, // 分词查询
        mn, // not in
        sh,	// in
		sw// should+模糊查询
    }

	private static String DESC = "desc";
    /**
     * 插入一个空索引
     * @param indexName
     * @return
     */
    @Override
    public boolean insertEmptyIndex(String indexName) throws GeneralException{
        if(isExistsIndex(indexName)){
            throw new GeneralException("索引已经存在，请核实！");
        }
        CreateIndexResponse response = EsUtil.getClient().admin().indices().prepareCreate(indexName).get();
        return response.isAcknowledged();
    }

    /**
     * 插入数据，主键不允许主动生成
     * @param indexName 索引名
     * @param type 类型
     * @param id 主键id
     * @param source 数据json传
     * @return
     */
    @Override
    public boolean insert(String indexName, String type, String id, Object source){
        if (StringUtil.isEmpty(id)) {
            //throw new EcpCoreException("不允许主键自动生成！");
        }
        IndexResponse response = EsUtil.getClient().prepareIndex(indexName, type, id).setSource(source).get();
        return isSuccess(response);
    }

    /**
     * 插入数据，主键不允许主动生成
     * @param indexName 索引名
     * @param type 类型
     * @param id 主键id
     * @return
     */
    @Override
    public boolean insert(String indexName, String type, String id, Map<String, String> map){
        if (StringUtil.isEmpty(id)) {
            //throw new EcpCoreException("不允许主键自动生成！");
        }
        // 转换map为json
        String source = JsonUtil.convertObject2Json(map);
        // 判断主键是否已经存在
        if (!isExistsId(indexName, type, id)) {
            IndexResponse response = EsUtil.getClient().prepareIndex(indexName, type, id).setSource(source).get();
            return isSuccess(response);
        }
        return false;
    }

    /**
     * 插入数据，主键不做控制
     * @param indexName 索引名
     * @param type 类型
     * @return
     */
    @Override
    public boolean insert(String indexName, String type, Map<String, String> map) {
        // 转换map为json
        String source = JsonUtil.convertObject2Json(map);
        // 判断主键是否已经存在
        IndexResponse response = EsUtil.getClient().prepareIndex(indexName, type).setSource(map).get();
        return isSuccess(response);
    }

    /**
     * 判断指定的索引名是否存在
     * @param indexName 索引名
     * @return  存在：true; 不存在：false;
     */
    @Override
    public boolean isExistsIndex(String indexName) {
        IndicesExistsResponse response = EsUtil.getClient().admin().indices()
                .exists(new IndicesExistsRequest().indices(new String[]{indexName})).actionGet();
        return response.isExists();
    }

    /**
     * 判断指定的索引的类型是否存在
     * @param indexName 索引名
     * @param indexType 索引类型
     * @return  存在：true; 不存在：false;
     */
    @Override
    public boolean isExistsType(String indexName, String indexType) {
        TypesExistsResponse response = EsUtil.getClient().admin().indices()
                .typesExists(new TypesExistsRequest(new String[]{indexName}, indexType)).actionGet();
        return response.isExists();
    }

    /**
     * 根据ID获取内容
     * @param indexName 索引名称
     * @param type 索引类型
     * @param id 索引id
     * @return
     */
    @Override
    public Map<String, String> get(String indexName, String type, String id) {
        GetResponse response = EsUtil.getClient().prepareGet(indexName, type, id).setOperationThreaded(false) // 线程安全
                .get();
        if (!response.isExists()) {
            return new HashMap<String, String>();
        }
        return converMapType(response.getSource());
    }

    /**
     * 判断id是否存在
     * @param indexName 索引名称
     * @param type 索引类型
     * @param id 索引id
     * @return
     */
    @Override
    public boolean isExistsId(String indexName, String type, String id) {
        GetResponse response = EsUtil.getClient().prepareGet(indexName, type, id).setOperationThreaded(false) // 线程安全
                .get();
        return response.isExists();
    }

    /**
     * 删除一条数据
     * @param indexName 索引名称
     * @param type 索引类型
     * @param id 索引id
     */
    @Override
    public boolean delete(String indexName, String type, String id) {
        DeleteResponse response = EsUtil.getClient().prepareDelete(indexName, type, id).execute().actionGet();
        return isSuccess(response);
    }

    /**
     * 删除索引
     * @param indexName
     */
    @Override
    public boolean deleteIndex(String indexName){
        // 判断索引是否存在
        if (!isExistsIndex(indexName)) {
            //throw new EcpCoreException("索引不存在！");
        }
        DeleteIndexResponse dResponse = EsUtil.getClient().admin().indices().prepareDelete(indexName).execute()
                .actionGet();
        return dResponse.isAcknowledged();
    }

    /**
     * 更新一条数据，使用doc更新
     * @param indexName 索引名称
     * @param type 索引类型
     * @param id 索引id
     * @param source 资源，可以传入任意类型，包括：json、map、byte[]、indexRequest、<String,Object>等
     * @return
     */
    @Override
    public boolean update(String indexName, String type, String id, Map<String, String> source) {
        if (StringUtil.isEmpty(id)) {
            //throw new EcpCoreException("传入的数据核实不正确，缺少指定主键！");
        }
        UpdateResponse response = EsUtil.getClient().prepareUpdate(indexName, type, id).setDoc(source).get();
        return isSuccess(response);
    }

    /**
     * 更新一条数据
     * @param indexName 索引名称
     * @param type 索引类型
     * @param request 请求入参
     * @return
     */
    @Override
    public boolean update(String indexName, String type, UpdateRequest request){
        UpdateResponse response = null;
        try {
            response = EsUtil.getClient().update(request).get();
        } catch (Exception e) {
            logger.error("UPDATE ERROR", e.getMessage(), e);
            //throw new EcpCoreException(e.getMessage());
        }
        return isSuccess(response);
    }

    /**
     * 批量插入数据，每条数据的主键必须存在，且内定为id
     * @param indexName 索引名称
     * @param type 索引类型
     * @param list 入参
     * @param isAutoKey 是否指定主键标识。ture:自动生成主键；false：需要指定传入的主键，指定的主键属性为：_id
     */
    @Override
    public boolean insertBatch(String indexName, String type, List<Map<String, String>> list, boolean isAutoKey,
        String _id) {
        BulkRequestBuilder bulkRequest = EsUtil.getClient().prepareBulk();
        for (Map<String, String> map : list) {
            if (!isAutoKey) {
                String id = map.get(_id); // 主键
                if (StringUtil.isEmpty(id)) {
                    //throw new EcpCoreException("传入的数据核实不正确，缺少指定主键！");
                }
               //  bulkRequest.add(EsUtil.getClient().prepareIndex(indexName, type, id).setSource(map)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            } else {
               // bulkRequest.add(EsUtil.getClient().prepareIndex(indexName, type).setSource(map)).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            }
        }
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
        if (bulkResponse.hasFailures()) {
            logger.error("ERROR insertBatch", "插入索引失败");
            return false;
        }
        logger.info("SUCCESS insertBatch", "批量插入索引成功："+list.size());
        return true;
    }

    /**
     * 批量删除数据
     * @param indexName 索引名称
     * @param type 索引类型
     */
    @Override
    public boolean deleteBatch(String indexName, String type, SearchResponse response) {
        BulkRequestBuilder bulkRequest = EsUtil.getClient().prepareBulk();
        SearchHit[] hits = response.getHits().getHits();
        for (int i=0;i<hits.length;i++) {
            SearchHit hit = hits[i];
            String id = hit.getId();
           // bulkRequest.add(EsUtil.getClient().prepareDelete(indexName, type, id).request()).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        }
        BulkResponse bulkResponse = bulkRequest.get();
        if (bulkResponse.hasFailures()) {
            for (BulkItemResponse item : bulkResponse.getItems()) {
                logger.error("ERROR deleteBatch", "删除索引失败,索引id为：", item.getId());
            }
            return false;
        }
        logger.error("SUCCESS deleteBatch", "批量删除索引数据成功共删除"+hits.length+"条数据");
        return true;
    }

    /**
     * 自定义搜索列表，封装到list中
     * @param queryBuilder 搜索列表
     * @param indexname 索引名称
     * @param type 索引类型
     * @return
     */
    @Override
    public List<Map<String, String>> searcherForList(String indexname, String type, QueryBuilder queryBuilder) {
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        SearchResponse searchResponse = EsUtil.getClient().prepareSearch(indexname).setTypes(type)
                .setQuery(queryBuilder).execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        logger.error("SUCCESS searcher", "查询到记录数=" + hits.getTotalHits());
        if (hits.getTotalHits() > 0) {
            for (SearchHit hit : hits) {
                list.add(converMapType(hit.sourceAsMap()));
            }
        }
        return list;
    }

    /**
     * 自定义查询数据，返回SearchResponse
     * @param queryBuilder 搜索列表
     * @param indexname 索引名称
     * @param type 索引类型
     * @return
     */
    @Override
    public SearchResponse searcherForResp(String indexname, String type, QueryBuilder queryBuilder){
		SearchResponse searchResponse = EsUtil.getClient().prepareSearch(indexname).setTypes(type)
                .setQuery(queryBuilder).execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        logger.error("SUCCESS searcher", "查询到记录数=" + hits.getTotalHits());
        return searchResponse;
    }
    
    public SearchResponse scrollSearcherForResp(String indexname, String type, QueryBuilder queryBuilder){
        SearchResponse searchResponse = EsUtil.getClient().prepareSearch(indexname).setTypes(type).setScroll(new TimeValue(30000))
                .setSearchType(SearchType.QUERY_THEN_FETCH).setQuery(queryBuilder).execute().actionGet(); // wlcnote 此处有个模式需了解
        String scrollId = searchResponse.getScrollId();
        SearchResponse response = EsUtil.getClient().prepareSearchScroll(scrollId).execute().actionGet();
        SearchHit[] hits = response.getHits().getHits();
        logger.error("SUCCESS searcher", "查询到记录数=" + hits.length);
        return response;
    }
    
    /**
     * 多条件搜索列表，不支持分页
     * @param indexname
     * @param type
     * @return
     */
    @Override
    public List<Map<String, String>> searcher(String indexname, String type, Map<String, String> termMap,
        Map<String, String> wildMap){
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 精确查询参数
        if (!termMap.isEmpty()) {
            boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, termMap, Type.jq);
        }
        // 模糊查询参数
        if (!wildMap.isEmpty()) {
            boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, wildMap, Type.mh);
        }
        logger.info("EsServiceImpl_searcher:boolQueryBuilder", boolQueryBuilder.toString());
        // 执行查询
        SearchResponse searchResponse = EsUtil.getClient().prepareSearch(indexname).setTypes(type)
                .setQuery(boolQueryBuilder).execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        logger.error("SUCCESS searcher", "查询到记录数=" + hits.getTotalHits());
        if (hits.getTotalHits() > 0) {
            for (SearchHit hit : hits) {
                Map<String, String> returnMap = converMapType(hit.sourceAsMap());
                returnMap.put("_id", hit.getId());
                list.add(returnMap);
            }
        }
        return list;
    }

    /**
     * 列表，支持多条件搜索，支持分页
     * @param indexname 索引名
     * @param type 类型
     * @param start 开始行
     * @param limit 限制行
     * @param asc 默认升序，false:倒序
     * @param property 排序参数
     * @return
     */
	@Override
	public List<Map<String, String>> searcher(String indexname, String type, Map<String, String> termMap,
		Map<String, String> wildMap, Map<String, String> mnMap, List<Map<String, String>> shList,
		List<Map<String, String>> swList,
		String property, boolean asc, int start, int limit) {
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 精确查询参数
        if (!termMap.isEmpty()) {
            boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, termMap, Type.jq);
        }
        // 模糊查询参数
        if (!wildMap.isEmpty()) {
            boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, wildMap, Type.mh);
        }
		// notIn查询参数
		if (!mnMap.isEmpty()) {
			boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, mnMap, Type.mn);
		}
		// should查询参数
		if (!shList.isEmpty()) {
			boolQueryBuilder = setshList2Boolqb(boolQueryBuilder, shList, Type.sh);
		}
		// should+模糊查询参数
		if (!swList.isEmpty()) {
			boolQueryBuilder = setswList2Boolqb(boolQueryBuilder, swList, Type.sw);
		}
        // 查询
        SortOrder so = SortOrder.ASC;
        if (!asc) {
            so = SortOrder.DESC;
        }
        // 查询
        SearchResponse searchResponse = EsUtil.getClient().prepareSearch(indexname).setTypes(type)
                .setQuery(boolQueryBuilder).addSort(property, so).setFrom(start).setSize(limit).execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        logger.error("SUCCESS searcher", "查询到记录数=" + hits.getTotalHits());
        if (hits.getTotalHits() > 0) {
            for (SearchHit hit : hits) {
                Map<String, String> returnMap = converMapType(hit.sourceAsMap());
                returnMap.put("_id", hit.getId());
                list.add(returnMap);
            }
        }
        return list;
    }
    
	/**
	 * 列表，支持多条件搜索，支持分页
	 * @param indexname 索引名
	 * @param type 类型
	 * @param start 开始行
	 * @param limit 限制行
	 * @return
	 */
	@Override
	public List<Map<String, String>> searcher(String indexname, String type, Map<String, String> termMap,
		Map<String, String> wildMap, Map<String, String> mnMap, List<Map<String, String>> shList,
		List<Map<String, String>> swList,
		int start, int limit){
		List<Map<String, String>> list = new ArrayList<Map<String, String>>();
		BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		// 精确查询参数
		if (!termMap.isEmpty()) {
			boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, termMap, Type.jq);
		}
		// 模糊查询参数
		if (!wildMap.isEmpty()) {
			boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, wildMap, Type.mh);
		}
		// notIn查询参数
		if (!mnMap.isEmpty()) {
			boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, mnMap, Type.mn);
		}
		// should查询参数
		if (!shList.isEmpty()) {
			boolQueryBuilder = setshList2Boolqb(boolQueryBuilder, shList, Type.sh);
		}
		// should+模糊查询参数
		if (!swList.isEmpty()) {
			boolQueryBuilder = setswList2Boolqb(boolQueryBuilder, swList, Type.sw);
		}
		// 查询
		SearchResponse searchResponse = EsUtil.getClient().prepareSearch(indexname).setTypes(type)
			.setQuery(boolQueryBuilder).setFrom(start).setSize(limit).execute().actionGet();
		SearchHits hits = searchResponse.getHits();
		logger.error("SUCCESS searcher", "查询到记录数=" + hits.getTotalHits());
		if (hits.getTotalHits() > 0) {
			for (SearchHit hit : hits) {
				Map<String, String> returnMap = converMapType(hit.sourceAsMap());
				returnMap.put("_id", hit.getId());
				list.add(returnMap);
			}
		}
		return list;
	}

    /**
     * 统计总数
     * @param indexname 文档索引
     * @param type 文档类型
     * @param termMap 精确查询的参数
     * @param wildMap 模糊查询的参数
     * @return
     */
	@Override
    public String searcherCount(String indexname, String type, Map<String, String> termMap,
		Map<String, String> wildMap, Map<String, String> mnMap, List<Map<String, String>> shList,
		List<Map<String, String>> swList) {
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 多条件组装入参
        if (!termMap.isEmpty()) {
            boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, termMap, Type.jq);
        }
        // 多条件组装入参
        if (!wildMap.isEmpty()) {
            boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, wildMap, Type.mh);
        }
		// notIn查询参数
		if (!mnMap.isEmpty()) {
			boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, mnMap, Type.mn);
		}
		// should查询参数
		if (!shList.isEmpty()) {
			boolQueryBuilder = setshList2Boolqb(boolQueryBuilder, shList, Type.sh);
		}
		// should+模糊查询参数
		if (!swList.isEmpty()) {
			boolQueryBuilder = setswList2Boolqb(boolQueryBuilder, swList, Type.sw);
		}
        // 查询count总数
        SearchResponse countResponse = EsUtil.getClient().prepareSearch(indexname).setTypes(type)
                .setQuery(boolQueryBuilder).execute().actionGet();
        long count = countResponse.getHits().totalHits();
        return String.valueOf(count);
    }

    /**
	 * 列表，支持多条件搜索，支持分页
	 * @param indexname 索引名
	 * @param type 类型
	 * 
	 * @param start 开始行
	 * @param limit 限制行
	 * @return
	 */
    @Override
    public List<Map<String, String>> searcher(String indexname, String type, Map<String, String> termMap,
        Map<String, String> wildMap, int start, int limit){
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
        // 精确查询参数
        if (!termMap.isEmpty()) {
            boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, termMap, Type.jq);
        }
        // 模糊查询参数
        if (!wildMap.isEmpty()) {
            boolQueryBuilder = setMap2Boolqb(boolQueryBuilder, wildMap, Type.mh);
        }
        // 查询
        SearchResponse searchResponse = EsUtil.getClient().prepareSearch(indexname).setTypes(type)
                .setQuery(boolQueryBuilder).setFrom(start).setSize(limit).execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        logger.error("SUCCESS searcher", "查询到记录数=" + hits.getTotalHits());
        if (hits.getTotalHits() > 0) {
            for (SearchHit hit : hits) {
                Map<String, String> returnMap = converMapType(hit.sourceAsMap());
                returnMap.put("_id", hit.getId());
                list.add(returnMap);
            }
        }
        return list;
    }

    /**
     * 拼装结果集进行搜索
     * @param indexname 索引名
     * @param type 类型
     * @param start 开始行
     * @param limit 限制行
     * @return
     */
    @Override
    public List<Map<String, String>> searcher(String indexname, String type, String[] includes, String[] excludes,
		List<Map<String, String>> sortList, String queryString, int start, int limit) {
        // 组装查询条件
        SearchRequestBuilder searchRequestBuilder = EsUtil.getClient().prepareSearch(indexname);
		searchRequestBuilder.setTypes(type);
		// 组装多个排序字段
		SortOrder so = SortOrder.ASC;
		for (Map<String, String> sortMap : sortList) {
			for (Entry<String, String> map : sortMap.entrySet()) {
				if (DESC.equalsIgnoreCase(map.getValue())) {
					so = SortOrder.DESC;
				}
				searchRequestBuilder.addSort(map.getKey(), so);
			}
		}
        BoolQueryBuilder boolQueryBuilder = QueryBuilders.boolQuery();
		boolQueryBuilder.must();
        SearchResponse searchResponse = searchRequestBuilder.setFetchSource(includes, excludes) // 设置结果集包含或排除的字段
                .setQuery(boolQueryBuilder) // 设置DSL查询语句
                .setFrom(start).setSize(limit).get();
        // 结果封装
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        SearchHits hits = searchResponse.getHits();
        logger.error("SUCCESS searcher", "查询到记录数=" + hits.getTotalHits());
        if (hits.getTotalHits() > 0) {
            for (SearchHit hit : hits) {
                Map<String, String> returnMap = converMapType(hit.getSourceAsMap());
                returnMap.put("_id", hit.getId());
                list.add(returnMap);
            }
        }
        return list;
    }
    
    /**
     * 拼装结果集进行搜索
     * @param indexname 索引名
     * @param type 类型
     * @param start 开始行
     * @param limit 限制行
     * @return
     */
    @Override
    public List<Map<String, String>> searcher(String indexname, String type, String[] includes, String[] excludes,
        String queryString, int start, int limit) {
        // 组装查询条件
        SearchRequestBuilder searchRequestBuilder = EsUtil.getClient().prepareSearch(indexname);
        searchRequestBuilder.setTypes(type);
        
        SearchResponse searchResponse = searchRequestBuilder.setFetchSource(includes, excludes) // 设置结果集包含或排除的字段
                .setQuery(QueryBuilders.queryStringQuery(queryString)) // 设置DSL查询语句
                .setFrom(start).setSize(limit).get();
        // 结果封装
        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        SearchHits hits = searchResponse.getHits();
        logger.info("SUCCESS searcher,查询到记录数=" + hits.getTotalHits());
        if (hits.getTotalHits() > 0) {
            for (SearchHit hit : hits) {
                Map<String, String> returnMap = converMapType(hit.getSourceAsMap());
                returnMap.put("_id", hit.getId());
                list.add(returnMap);
            }
        }
        return list;
    }

    /**
     * 统计总数
     * @param indexname 索引名
     * @param type 类型
     * @return
     */
    @Override
    public String searcherCount(String indexname, String type, String queryString){
        // 组装查询条件
        SearchRequestBuilder searchRequestBuilder = EsUtil.getClient().prepareSearch(indexname);
		searchRequestBuilder.setTypes(type);
        SearchResponse countResponse = searchRequestBuilder // 设置结果集包含或排除的字段
               // .setQuery(queryString) // 设置DSL查询语句
                .get();
        long count = countResponse.getHits().totalHits;
        return String.valueOf(count);
    }

    /**
     * 获取索引的mapping信息
     * @param indexname 文档索引
     * @param type 文档类型
     * @return
     */
    @Override
    public String getMapping(String indexname, String type){
        ImmutableOpenMap<String, MappingMetaData> mappings = EsUtil.getClient().admin().cluster().prepareState()
                .execute().actionGet().getState().getMetaData().getIndices().get(indexname).getMappings();
        String mapping = mappings.get(type).source().toString();
        return mapping;
    }

    /**
     * 将map转换为boolQueryBuilder类
     * @param boolQueryBuilder
     * @param params 入参集合
     * @return
     */
    private static BoolQueryBuilder setMap2Boolqb(BoolQueryBuilder boolQueryBuilder, Map<String, String> params,
        Type type) {
        // 多条件组装入参
        Iterator<Entry<String, String>> entryKeyIterator = params.entrySet().iterator();
        while (entryKeyIterator.hasNext()) {
            Entry<String, String> e = entryKeyIterator.next();
            String key = e.getKey();
            String value = e.getValue();
                if (type.equals(Type.jq)) {
                boolQueryBuilder.must(QueryBuilders.termQuery(key, value));
            } else if (type.equals(Type.mh)) {
				boolQueryBuilder.must(QueryBuilders.wildcardQuery(key, "*" + value + "*"));
            } else if (type.equals(Type.fc)) {
                boolQueryBuilder.must(QueryBuilders.matchQuery(key, value));
			} else if (type.equals(Type.mn)) {
				boolQueryBuilder.mustNot(QueryBuilders.matchQuery(key, value));
			} else if (type.equals(Type.sh)) {
				boolQueryBuilder.should(QueryBuilders.matchQuery(key, value));
			} else if (type.equals(Type.sw)) {
				boolQueryBuilder.should(QueryBuilders.wildcardQuery(key, value));
            }

        }
        return boolQueryBuilder;
    }

	/**
	 * 将list转换为boolQueryBuilder类
	 * @param boolQueryBuilder
	 * @param params 入参集合
	 * @return
	 */
	private static BoolQueryBuilder setswList2Boolqb(BoolQueryBuilder boolQueryBuilder,
		List<Map<String, String>> params, Type type) {
		// 多条件组装入参
		for (Map<String, String> map : params) {
			Iterator<Entry<String, String>> entryKeyIterator = map.entrySet().iterator();
			while (entryKeyIterator.hasNext()) {
				Entry<String, String> e = entryKeyIterator.next();
				String key = e.getKey();
				String value = e.getValue();
				if (type.equals(Type.sw)) {
					boolQueryBuilder.should(QueryBuilders.wildcardQuery(key, value));
				}
			}
		}
		return boolQueryBuilder;
	}

	/**
	 * 将list转换为boolQueryBuilder类
	 * @param boolQueryBuilder
	 * @param params 入参集合
	 * @return
	 */
	private static BoolQueryBuilder setshList2Boolqb(BoolQueryBuilder boolQueryBuilder,
		List<Map<String, String>> params, Type type){
		// 多条件组装入参
		for (Map<String, String> map : params) {
			Iterator<Entry<String, String>> entryKeyIterator = map.entrySet().iterator();
			while (entryKeyIterator.hasNext()) {
				Entry<String, String> e = entryKeyIterator.next();
				String key = e.getKey();
				String value = e.getValue();
				if (type.equals(Type.sh)) {
					boolQueryBuilder.should(QueryBuilders.matchQuery(key, value));
				}
			}
		}
		return boolQueryBuilder;
	}

    private static boolean isSuccess(Object response) {
        if (response instanceof IndexResponse) {
            //返回状态有两种情况-第一:该文档id不存在,进行创建写入RestStatus.CREATED ; 第二,文档id已经存在,进行更新操作返回RestStatus.OK
            return RestStatus.CREATED.equals(((IndexResponse) response).status()) || RestStatus.OK.equals(((IndexResponse) response).status());
        } else if (response instanceof DeleteResponse) {
            return RestStatus.OK.equals(((DeleteResponse) response).status());
        }
        return false;
    }

    /**
     * map类型强制转换
     * @param map
     * @return
     */
    private static Map<String, String> converMapType(Map<String, Object> map) {
        return ( Map<String, String>)JsonUtil.convertJson2Object(JsonUtil.convertObject2Json(map), Map.class);
    }


    /**
     *  常用的查询：简单查询和聚合查询
     *  常用的一些查询工具方法，可以参考
     */
    /*private static void testExample() {
        // 搜索所有
        QueryBuilder qb = QueryBuilders.matchAllQuery();

         //以下为全文级别查询，高级全文查询通常用于在全文字段（如电子邮件正文）上运行全文查询。 
        // 匹配多个字段的查询(在title字段和content字段中搜索含有git的记录)
        QueryBuilder qb1 = QueryBuilders.multiMatchQuery("git", "title", "content");

        // 执行全文查询的标准查询，包括模糊匹配和短语或接近查询(在title字段中搜索含有git的记录)
        QueryBuilder qb2 = QueryBuilders.matchQuery("title", "git");

        // 针对不常见单词的查询
        QueryBuilder qb4 = QueryBuilders.commonTermsQuery("title", "git");

        // 支持紧凑的Lucene查询字符串语法，允许您在单个查询字符串中指定AND | OR | NOT条件和多字段搜索，如下表示：title中包括git，content中不包含Shell和SVN的记录
        QueryBuilder qb5 = QueryBuilders.queryStringQuery("+title:git -content:(Shell SVN)");

        // 更适合直接暴露给用户的更简单更健壮的查询字符串语法。
        QueryBuilder qb6 = QueryBuilders.simpleQueryStringQuery("+git");

         //以下为术语级别查询，这些查询通常用于结构化数据，如数字，日期和枚举，而不是全文本字段。 
        // 查找包含在指定字段中指定的确切术语的文档。如下表示：title字段中包含hibernate的记录
        QueryBuilder qb3 = QueryBuilders.termQuery("title", "hibernate");

        // 查找包含指定字段中指定的任何确切术语的记录。如下表示：title字段中包含git或者hibernate或者Java的记录
        QueryBuilder qb7 = QueryBuilders.termsQuery("title", "git", "hibernate", "java");

        // 查找指定字段包含指定范围内的值（日期，数字或字符串）的文档。如下表示：查询字段ID>=3且ID<5的记录
        QueryBuilder qb8 = QueryBuilders.rangeQuery("id").from(3).to(5).includeLower(true).includeUpper(false);
        // 查找指定字段包含指定范围内的值（日期，数字或字符串）的文档。如下表示：查询字段ID>=3且ID<5的记录
        QueryBuilder qb9 = QueryBuilders.rangeQuery("id").gte(3).lt(5);

        // 查找指定的字段包含任何非空值的文档。如下表示：查询ID字段值不为空的记录
        QueryBuilder qb10 = QueryBuilders.existsQuery("id");

        // 查找指定的字段包含以指定的精确前缀开头的术语的文档。如下表示：查询字段title中以m开头的记录
        QueryBuilder qb11 = QueryBuilders.prefixQuery("title", "m");

        // 查找指定字段包含与指定模式匹配的术语的文档，其中该模式支持单字符通配符（？）和多字符通配符（*）。
        QueryBuilder qb12 = QueryBuilders.wildcardQuery("title", "m?s*");

        // 查找指定的字段包含与指定的正则表达式匹配的术语的文档。
        QueryBuilder qb13 = QueryBuilders.regexpQuery("title", "m.*");

        // 查找指定字段包含与指定术语模糊相似的术语的文档。如下表示：查询title字段中包含和Hobernate类似的记录。
        QueryBuilder qb14 = QueryBuilders.fuzzyQuery("title", "Hobernate");

        // 查找指定类型的文档。如下表示：查询索引类型为article的记录。
        QueryBuilder qb15 = QueryBuilders.typeQuery("article");

        // 查找具有指定类型和ID的文档。
        QueryBuilder qb16 = QueryBuilders.idsQuery("article").addIds("1", "5");

         //以下为复合查询 
        // 一个查询包装另一个查询，但在过滤器上下文中执行,且所有匹配的文档都赋予相同的“常量”，即_score。
        QueryBuilder qb17 = QueryBuilders.constantScoreQuery(qb3).boost(2.0f);

        // 用于组合多个子查询或复合查询子句，且以must，must_not,filter和should子句作为查询约束， must和should具有更好更匹配的条约， 而must_not和filter子句在过滤器上下文中执行。
        QueryBuilder qb18 = QueryBuilders.boolQuery().must(qb).mustNot(qb3).filter(qb13);

        // 接受多个查询的查询，并返回与任何查询子句匹配的任何文档。 bool查询组合来自所有匹配查询的分数，而dis_max查询使用单个最佳匹配查询子句的分数。
        QueryBuilder qb19 = QueryBuilders.disMaxQuery().add(qb3).add(qb4);

        // 对指定的索引执行一个查询，为另一个索引执行另一个查询。如下表示：为索引article和index2执行qb3查询，对其他索引执行qb4查询
        QueryBuilder qb20 = QueryBuilders.indicesQuery(qb3, "article", "index2").noMatchQuery(qb4);

        // 对指定的索引执行一个查询，为另一个索引执行另一个查询。如下表示：为索引article和index2执行qb3查询，对其他索引执行（如果是all,则匹配所有，如果是none,则匹配空）
        QueryBuilder qb21 = QueryBuilders.indicesQuery(qb3, "article", "index2").noMatchQuery("all");

        // 简单查询
        // DFS_QUERY_THEN_FETCH 精确查询；
        // 查询前20条记录
        SearchResponse response = EsUtil.getClient().prepareSearch("megacorp").setTypes("employee")
                .setSearchType(SearchType.DFS_QUERY_THEN_FETCH).setQuery(QueryBuilders.termQuery("first_name", "xiaoming"))
                .setFrom(0).setSize(20).setExplain(true).execute().actionGet();

        // 聚合算法
        // 1、按照team进行分组；
        // 2、按照salary字段进行desc降序操作；
        // 3、求salary的总和total_salary；
        // 4、仅返回15条数据
        SearchRequestBuilder sbuilder = EsUtil.getClient().prepareSearch("player").setTypes("player");
        TermsBuilder teamAgg = AggregationBuilders.terms("team").order(Order.aggregation("salary", false)).size(15);
        SumBuilder salaryAgg = AggregationBuilders.sum("total_salary").field("salary");
        sbuilder.addAggregation(teamAgg.subAggregation(salaryAgg));
        SearchResponse rs = sbuilder.execute().actionGet();
        AggregationBuilders.count("");
        // 结果输出
        Map<String, Aggregation> aggMap = rs.getAggregations().asMap();
        StringTerms teamAgg1 = (StringTerms)aggMap.get("keywordAgg");
        Iterator<Bucket> teamBucketIt = teamAgg1.getBuckets().iterator();
        while (teamBucketIt.hasNext()) {
            Bucket buck = teamBucketIt.next();
            // 球队名
            String team = (String)buck.getKey();
            // 记录数
            long count = buck.getDocCount();
            // 得到所有子聚合
            Map subaggmap = buck.getAggregations().asMap();
            // avg值获取方法
            double avg_age = ((InternalAvg)subaggmap.get("avg_age")).getValue();
            // sum值获取方法
            double total_salary = ((InternalSum)subaggmap.get("total_salary")).getValue();
        }

    }*/
    


}
