package com.cmos.ngmc.service.impl.es;

import com.cmos.core.logger.Logger;
import com.cmos.core.logger.LoggerFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.common.Strings;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * @author GavinCook
 * @date 2017-02-21
 * @since 1.0.0
 */
public class ESCondition implements Serializable{

    private static final Logger logger = LoggerFactory.getServiceLog(ESCondition.class);
    /**
     * 
     */
    private static final long serialVersionUID = 1L;

    /**
     * 匹配
     */
    private transient Map<String, Object> matches = new HashMap<>();

    /**
     * 过滤，精确
     */
    private transient Map<String, Object> terms = new HashMap<>();

	/**
	 * 过滤，精确
	 */
	private Map<String, List<String>> termList = new HashMap<String, List<String>>();
	/**
	 * 过滤，精确
	 */
	private Map<String, List<String>> shouldWildList = new HashMap<String, List<String>>();
    /**
     * 模糊匹配
     */
    private transient Map<String, Object> wildcards = new HashMap<>();

    /**
     * 范围查询
     */
	private List<Range> ranges = new ArrayList<>();
	/**
	 * 正则查询regexp
	 */
	private transient Map<String, Object> regexp = new HashMap<>();

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private ESConditionMode mode;

    private ESCondition(ESConditionMode mode) {
        this.mode = mode;
    }

    public static ESCondition getESCondition(ESConditionMode mode) {
        return new ESCondition(mode);
    }

    /**
     * 全匹配（会查询相关度），类似sql中的等于（=）
     *
     * @param key
     * @param value
     * @return
     */
    public ESCondition match(String key, Object value) {
		if (Strings.isNullOrEmpty(key) || value == null) {
            return this;

        }
        matches.put(key, value);
        return this;
    }

	/**
	 * 正则查询
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public ESCondition regexp(String key, Object value) {
		if (Strings.isNullOrEmpty(key) || value == null) {
			return this;
		}
		regexp.put(key, value);
		System.err.println(regexp.toString());
		return this;
	}

    /**
     * 过滤（不会查询相关度）
     *
     * @param key
     * @param value
     * @return
     */
    public ESCondition term(String key, Object value) {
		if (Strings.isNullOrEmpty(key) || value == null) {
            return this;
        }
        terms.put(key, value);
        return this;
    }
    
    /**
     * 过滤（不会查询相关度）
     *
     * @param key
     * @param value
     * @return
     */
    public ESCondition termList(String key, List<String> value) {
    	if (Strings.isNullOrEmpty(key) || value == null) {
    		return this;
    	}
    	termList.put(key, value);
    	return this;
    }

	/**
	 * 多条件模糊匹配
	 *
	 * @param key
	 * @param value
	 * @return
	 */
	public ESCondition shouldWildList(String key, List<String> value) {
		if (Strings.isNullOrEmpty(key) || value == null) {
			return this;
		}
		shouldWildList.put(key, value);
		return this;
	}

    /**
     * 通配符查询
     *
     * @param key
     * @param value
     * @return
     */
    public ESCondition wildcard(String key, Object value) {
		if (Strings.isNullOrEmpty(key) || value == null) {
            return this;
        }
        wildcards.put(key, value);
        return this;
    }

    /**
     * 范围查询
     *
     * @param range
     * @return
     */
    public ESCondition range(Range range) {
        ranges.add(range);
        return this;
    }

    @Override
    public String toString() {
        StringBuilder queryBuilder = new StringBuilder();
        queryBuilder.append("{\"bool\":{\"").append(mode.mode()).append("\":[");
        boolean hasCondition = false;
        try {
            if (matches.size() > 0) {
                for (Entry<String, Object> entry : matches.entrySet()) {
                    addSpeIfNecessary(queryBuilder, hasCondition);
                    hasCondition = true;
                    queryBuilder.append(appendWithBracket("match", write(entry.getKey(), entry.getValue())));
                }
            }

            if (terms.size() > 0) {
                for (Entry<String, Object> entry : terms.entrySet()) {
                    addSpeIfNecessary(queryBuilder, hasCondition);
                    hasCondition = true;
                    queryBuilder.append(appendWithBracket("term", write(entry.getKey(), entry.getValue())));
                }
            }

			if (termList.size() > 0) {
				for (Entry<String, List<String>> entry : termList.entrySet()) {
					String key = entry.getKey();
					for (String str : entry.getValue()) {
						addSpeIfNecessary(queryBuilder, hasCondition);
						hasCondition = true;
						queryBuilder.append(appendWithBracket("term", write(key, str)));
					}
				}
			}
			if (!shouldWildList.isEmpty()) {
				for (Entry<String, List<String>> entry : shouldWildList.entrySet()) {
					String key = entry.getKey();
					for (String str : entry.getValue()) {
						addSpeIfNecessary(queryBuilder, hasCondition);
						hasCondition = true;
						queryBuilder.append(appendWithBracket("wildcard", write(key, str)));
					}
				}
			}
            if (!ranges.isEmpty()) {
                for (Range range : ranges) {
                    addSpeIfNecessary(queryBuilder, hasCondition);
                    hasCondition = true;
                    queryBuilder.append(appendWithBracket("range", range.toString()));
                }
            }

            if (wildcards.size() > 0) {
                for (Entry<String, Object> entry : wildcards.entrySet()) {
                    //                    addSpeIfNecessary(queryBuilder, hasCondition);
                    handleMultiCondition(queryBuilder, hasCondition, entry);
                    hasCondition = true;
                    //                    queryBuilder.append(appendWithBracket("wildcard", write(entry.getKey(), entry.getValue())));
                }
            }
			// regexp
			if (regexp.size() > 0) {
				for (Entry<String, Object> entry : regexp.entrySet()) {
					addSpeIfNecessary(queryBuilder, hasCondition);
					hasCondition = true;
					queryBuilder.append(appendWithBracket("regexp", write(entry.getKey(), entry.getValue())));
				}
			}
        } catch (JsonProcessingException e) {
            logger.error("", e.getMessage(), e);
        }
        queryBuilder.append("] } }");
        return queryBuilder.toString();
    }

    private void handleMultiCondition(StringBuilder queryBuilder, boolean hasCondition, Entry<String, Object> entry) throws JsonProcessingException{
        if (entry.getValue() instanceof List) {
            @SuppressWarnings("unchecked")
            List<String> multiConditions = (List<String>)entry.getValue();
            boolean flag = hasCondition;
            for (String condition : multiConditions) {
                addSpeIfNecessary(queryBuilder, flag);
                flag = true;
                queryBuilder.append(appendWithBracket("wildcard", write(entry.getKey(), condition)));
            }
        } else {
            addSpeIfNecessary(queryBuilder, hasCondition);
            queryBuilder.append(appendWithBracket("wildcard", write(entry.getKey(), entry.getValue())));
        }
    }
    private void addSpeIfNecessary(StringBuilder builder, boolean needAdd){
        if(needAdd){
            builder.append(",");
        }
    }
    private static final String colon = ":";

    private String write(String key, Object value) throws JsonProcessingException {
        return "\"" + key +"\"" + colon + objectMapper.writeValueAsString(value);
    }


    private String appendWithBracket(String key, Object value) throws JsonProcessingException {
        return "{\"" + key +"\"" + colon +"{" + value +"}}";
    }

    public static class Range implements Serializable {
        private String name;
        private transient Object from;
        private transient Object to;
        private boolean includeLower;
        private boolean includeUpper;

        public Range(String name) {
            this.name = name;
        }

        public Range gt(Object from) {
            this.from = from;
            includeLower = false;
            return this;
        }

        public Range gte(Object from) {
            this.from = from;
            includeLower = true;
            return this;
        }

        public Range lt(Object to) {
            this.to = to;
            includeUpper = false;
            return this;
        }

        public Range lte(Object to) {
            this.to = to;
            includeUpper = true;
            return this;
        }

        @Override
        public String toString() {
            try {
                StringBuilder sb = new StringBuilder();
                sb.append("\"").append(name).append("\":{");
                if (from != null) {
                    if (includeLower) {
                        sb.append(objectMapper.writeValueAsString("gte")).append(":").append(objectMapper.writeValueAsString(from));
                    } else {
                        sb.append(objectMapper.writeValueAsString("gt")).append(":").append(objectMapper.writeValueAsString(from));
                    }
                }
                if (to != null) {
                    if(from != null){
                        sb.append(",");
                    }
                    if (includeUpper) {
                        sb.append(objectMapper.writeValueAsString("lte")).append(":").append(objectMapper.writeValueAsString(to));
                    } else {
                        sb.append(objectMapper.writeValueAsString("lt")).append(":").append(objectMapper.writeValueAsString(to));
                    }
                }
                sb.append("}");
                return sb.toString();
            } catch (JsonProcessingException e) {
                logger.error("", e.getMessage(), e);
            }
            return "";
        }

        public static Range get(String name) {
            return new Range(name);
        }
    }
}
