package org.isaac.hive.hook.utils;

import java.util.*;

import com.google.gson.*;
import com.google.gson.reflect.TypeToken;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/7 17:19
 * @since 1.0.0
 */
public class JsonMapper {

    private static JsonMapper defaultJsonMapper = null;
    private static JsonMapper nonEmptyJsonMapper = null;
    private static JsonMapper nonDefaultJsonMapper = null;
    private static JsonMapper defaultUnwrapRootJsonMapper = null;

    private final Gson gson;
    private final JsonParser parser;

    public JsonMapper() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        // mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        // mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        // mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
        // mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        // mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
        // mapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        // mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        //
        // if (unwrapRoot) {
        //     mapper.configure(DeserializationFeature.UNWRAP_ROOT_VALUE, true);
        // }
        this.gson = gsonBuilder.disableHtmlEscaping().create();
        this.parser = new JsonParser();
    }

    /**
     * 创建只输出非Null且非Empty(如List.isEmpty)的属性到Json字符串的Mapper,建议在外部接口中使用.
     */
    public static synchronized JsonMapper nonEmptyMapper() {
        if (nonEmptyJsonMapper == null) {
            nonEmptyJsonMapper = new JsonMapper();
        }
        return nonEmptyJsonMapper;
    }

    /**
     * 创建只输出初始值被改变的属性到Json字符串的Mapper, 最节约的存储方式，建议在内部接口中使用。
     */
    public static synchronized JsonMapper nonDefaultMapper() {
        if (nonDefaultJsonMapper == null) {
            nonDefaultJsonMapper = new JsonMapper();
        }
        return nonDefaultJsonMapper;
    }

    public static synchronized JsonMapper defaultUnwrapRootMapper() {
        if (defaultUnwrapRootJsonMapper == null) {
            defaultUnwrapRootJsonMapper = new JsonMapper();
        }
        return defaultUnwrapRootJsonMapper;
    }

    /**
     * 创建默认Mapper
     */
    public static synchronized JsonMapper defaultMapper() {
        if (defaultJsonMapper == null) {
            defaultJsonMapper = new JsonMapper();
        }
        return defaultJsonMapper;
    }

    /**
     * 对象转换成JSON字符串
     *
     * @param object Object
     * @return String
     */
    public String toJson(Object object) {
        return gson.toJson(object);
    }

    /**
     * JSON转换成Java对象
     *
     * @param json  String
     * @param clazz Class<T>
     * @param <T>   T
     * @return T
     */
    public <T> T fromJson(String json, Class<T> clazz) {
        if (json == null || json.trim().length() == 0) {
            return null;
        }
        return gson.fromJson(json, clazz);
    }

    /**
     * JSON转换成Java对象
     *
     * @param json String
     * @return Map
     */
    public Map<String, Object> json2Map(String json) {
        java.lang.reflect.Type type =
                new TypeToken<HashMap<String, Object>>() {
                }.getType();
        return gson.fromJson(json, type);
    }

    /**
     * 如果jsons 是数组格式，则挨个转换成clazz对象返回list，否则直接尝试转换成clazz对象返回list
     *
     * @param jsons jsons
     * @param clazz Class<T>
     * @param <T>   T
     * @return T
     */
    public <T> List<T> fromJsons(String jsons, Class<T> clazz) {
        if (jsons == null || jsons.trim().length() == 0) {
            return Collections.emptyList();
        }
        List<T> list = new ArrayList<>();
        JsonElement jsonNode = parser.parse(jsons);
        if (jsonNode.isJsonArray()) {
            JsonArray array = jsonNode.getAsJsonArray();
            for (int i = 0; i < array.size(); i++) {
                list.add(gson.fromJson(array.get(i), clazz));
            }
        } else {
            list.add(fromJson(jsons, clazz));
        }
        return list;
    }

    public Gson getMapper() {
        return gson;
    }

}
