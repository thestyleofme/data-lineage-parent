package org.isaac.lineage.neo4j.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.CollectionType;
import org.isaac.lineage.neo4j.exceptions.LineageException;
import org.springframework.context.ApplicationContext;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 15:45
 * @since 1.0.0
 */
public class JsonUtil {

    private JsonUtil() {
        throw new IllegalStateException("util class");
    }

    private static final ObjectMapper OBJECT_MAPPER;

    static {
        ApplicationContext context = ApplicationContextHelper.getContext();
        if (Objects.nonNull(context)) {
            OBJECT_MAPPER = context.getBean(ObjectMapper.class);
        } else {
            // 可以用于单元测试
            OBJECT_MAPPER = new ObjectMapper();
        }
    }

    public static <T> T toObj(String json, Class<T> clazz) {
        try {
            return OBJECT_MAPPER.readValue(json, clazz);
        } catch (IOException e) {
            throw new LineageException(e);
        }
    }

    public static <T> T toObj(String json, TypeReference<T> typeReference) {
        try {
            return OBJECT_MAPPER.readValue(json, typeReference);
        } catch (IOException e) {
            throw new LineageException(e);
        }
    }

    public static <T> List<T> toArray(String json, Class<T> clazz) {
        try {
            CollectionType type = OBJECT_MAPPER.getTypeFactory().constructCollectionType(ArrayList.class, clazz);
            return OBJECT_MAPPER.readValue(json, type);
        } catch (IOException e) {
            throw new LineageException(e);
        }
    }

    public static <T> String toJson(T obj) {
        try {
            return OBJECT_MAPPER.writeValueAsString(obj);
        } catch (IOException e) {
            throw new LineageException(e);
        }
    }
}
