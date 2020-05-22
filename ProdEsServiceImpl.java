package com.cmos.ngmc.service.impl.es;


import com.ai.ecpcore.exception.EcpCoreException;
import com.ai.ecpcore.service.es.IProdEsService;
import com.ai.ecpcore.service.impl.BaseServiceImpl;
import com.ai.ecpcore.util.Constants;
import com.ai.frame.bean.InputObject;
import com.ai.frame.bean.OutputObject;
import com.ai.frame.util.JsonUtil;
import com.alibaba.dubbo.common.utils.StringUtils;
import com.cmos.core.logger.Logger;
import com.cmos.core.logger.LoggerFactory;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 商品搜索引擎开发类
 * @author Administrator
 *
 */
public class ProdEsServiceImpl extends BaseServiceImpl implements IProdEsService {

	private static final Logger logger = LoggerFactory.getServiceLog(ProdEsServiceImpl.class);
	private static String indexName = "prodindex";// 索引名称只能全小写，可以包含数字
	private String  prodEsQuerySwitch;

	// 初始化索引及类型
	@Override
	public void prodIndexInit() throws EcpCoreException, IOException {
		// 判断商品索引库是否存在
		if (getEsService().isExistsIndex(indexName)) {
			//删除索引
			getEsService().deleteIndex(indexName);
		}
		// 初始化详情
		ProdDetailMapping pdm = new ProdDetailMapping();
		pdm.buildIndexMapping(indexName, "prodDetail", new HashMap<String, String>());
	}

	/**@desc 刷新搜索引擎数据
	 * @time 17/6/29
	 * @param inputObject
	 * @param outputObject
	 * @throws EcpCoreException,IOException
	 */
	@Override
	public void refreshEsDataFromDB(InputObject inputObject, OutputObject outputObject)
		throws EcpCoreException, IOException {
		// 初始化索引
		try {
			prodIndexInit();
		} catch (IOException e1) {
			logger.info("ES初始化索引失败:prodIndexInit.ERROR", e1.getMessage(), e1);
			throw new EcpCoreException("刷新数据时，初始化索引失败！");
		}
		// 查询商品详情加入搜索引擎
		loadProdDetailToEs(inputObject, outputObject);
		outputObject.setReturnCode("0");
		outputObject.setReturnMessage("恭喜你！刷新成功！");
	}

	/**@desc 加载商品详情入搜索引擎
	 * @time 17/6/30
	 * @param inputObject
	 * @param outputObject
	 * @throws EcpCoreException
	 */
	@Override
	public void loadProdDetailToEs(InputObject inputObject, OutputObject outputObject) throws EcpCoreException {
		try {
		    logger.info("bgn:loadProdDetailToEs", JsonUtil.convertObject2Json(inputObject));
			inputObject.setService("mcdsService");
			inputObject.setMethod("queryMcdsAndMrctDataForEs");
			super.getProdRemoteSV().execute(inputObject, outputObject);
			List<Map<String, String>> list = outputObject.getBeans();
			if(!list.isEmpty()){
			    getEsService().insertBatch(indexName, "prodDetail", list, true, "");
			}
			logger.info("end:loadProdDetailToEs", JsonUtil.convertObject2Json(outputObject));
		} catch (EcpCoreException e) {
			logger.info("查询商品详情加入搜索引擎失败:queryMcdsAndMrctDataForEs.error", e.getMessage(), e);
			throw new EcpCoreException("刷新数据时，查询商品详情失败！");
		}

	}
	/**
	 *  删除对应商品编码的Es搜索引擎数据(批量数据删除)
	 */
	public void deleteBatchProdDetail(String idxName,String Type,SearchResponse response){
		getEsService().deleteBatch(idxName, Type, response);
	}
	public SearchResponse queryProdDetailByMcdsId(QueryBuilder queryBuilder)throws EcpCoreException{
		return getEsService().scrollSearcherForResp(indexName, Constants.PROD_INDEX_TYPE.PROD_DETAIL, queryBuilder);
	}
	public void init() throws Exception {
		if(StringUtils.isNotEmpty(prodEsQuerySwitch)){
			//设置放入缓存
			getCacheService().put2Cache(Constants.ECPCORE_ES_QUERY_CACHE_KEY.QUERY_PROD, prodEsQuerySwitch);
		}
	}

	//判断 ES prod 查询是否开启
	public  boolean isEnableProdEsQuery() throws EcpCoreException {
		//从缓存拿
		if( Constants.ES_PROD_SWITCH.ES_PROD_VALID.equals(getCacheService().getFromCache(Constants.ECPCORE_ES_QUERY_CACHE_KEY.QUERY_PROD))){
			return true;
		}
		return false;
	}

	// 启用/停用ES PROD 查询，将开关值放入缓存，refreshProdEsFlag，1为开 ；0 为关
	public void refreshProdEsSwitchCache(InputObject inputObject, OutputObject outputObject) throws EcpCoreException {
		String refreshProdEsFlag = inputObject.getParams().get("refreshProdEsFlag");
		if (Constants.ES_PROD_SWITCH.ES_PROD_VALID.equals(refreshProdEsFlag)) {
			getCacheService().put2Cache(Constants.ECPCORE_ES_QUERY_CACHE_KEY.QUERY_PROD, Constants.ES_PROD_SWITCH.ES_PROD_VALID);
		} else {
			getCacheService().put2Cache(Constants.ECPCORE_ES_QUERY_CACHE_KEY.QUERY_PROD, Constants.ES_PROD_SWITCH.ES_PROD_INVALID);
		}
		outputObject.setReturnCode("0");
		outputObject.setReturnMessage(
			"搜索引擎开关的值为：" + ("1".equals(getCacheService().getFromCache(Constants.ECPCORE_ES_QUERY_CACHE_KEY.QUERY_PROD))
				? "开" : "关"));
	}

	public String getProdEsQuerySwitch() {
		return prodEsQuerySwitch;
	}

	public void setProdEsQuerySwitch(String prodEsQuerySwitch) {
		this.prodEsQuerySwitch = prodEsQuerySwitch;
	}
}
