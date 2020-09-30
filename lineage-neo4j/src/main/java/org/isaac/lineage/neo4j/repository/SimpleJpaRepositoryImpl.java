package org.isaac.lineage.neo4j.repository;

import java.beans.PropertyDescriptor;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.isaac.lineage.neo4j.domain.node.BaseNodeEntity;
import org.isaac.lineage.neo4j.exceptions.LineageException;
import org.neo4j.ogm.session.Session;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;
import org.springframework.data.neo4j.repository.support.SimpleNeo4jRepository;
import org.springframework.util.StringUtils;

/**
 * <p>
 * 扩展SimpleNeo4jRepository类，在save前做一些处理
 * </p>
 *
 * @author JupiterMouse 2020/09/28
 * @since 1.0
 */
public class SimpleJpaRepositoryImpl<T, ID extends Serializable> extends SimpleNeo4jRepository<T, ID> {

    /**
     * Creates a new {@link SimpleNeo4jRepository} to manage objects of the given domain type.
     *
     * @param domainClass must not be {@literal null}.
     * @param session     must not be {@literal null}.
     */
    public SimpleJpaRepositoryImpl(Class<T> domainClass, Session session) {
        super(domainClass, session);
    }

    @Override
    public <S extends T> S save(S entity) {
        if (entity instanceof BaseNodeEntity) {
            this.beforeSave(entity);
        }
        return super.save(entity);
    }

    @Override
    public <S extends T> Iterable<S> save(Iterable<S> ses, int depth) {
        if (!ses.iterator().hasNext()) {
            return super.save(ses, depth);
        }
        final S next = ses.iterator().next();
        if (next instanceof BaseNodeEntity) {
            ses.forEach(this::beforeSave);
        }
        return super.save(ses, depth);
    }

    /**
     * 目的：更新node时，只更新非空的属性
     */
    @SuppressWarnings("unchecked")
    private <S extends T> void beforeSave(S entity) {
        final BaseNodeEntity baseNodeEntity = (BaseNodeEntity) entity;
        ID entityId = (ID) baseNodeEntity.getPk();
        if (StringUtils.isEmpty(entityId)) {
            // 异常抛出，pk不能为空
            throw new LineageException("entity id can not be null");
        }
        // 若ID非空 则查询最新数据
        Optional<T> optionalT = findById(entityId);
        // 若根据ID查询结果为空,则新增
        if (!optionalT.isPresent()) {
            return;
        }
        // 获取最新对象
        T target = optionalT.get();
        // 拷贝查询对象属性中保存对象为空的属性 到 保存对象
        BeanUtils.copyProperties(target, entity, getNonNullProperties(entity));
    }

    /**
     * 获取对象的空属性
     */
    private static String[] getNonNullProperties(Object src) {
        // 获取Bean
        BeanWrapper srcBean = new BeanWrapperImpl(src);
        // 获取Bean的属性描述
        PropertyDescriptor[] pds = srcBean.getPropertyDescriptors();
        // 获取Bean的空属性
        Set<String> properties = new HashSet<>();
        for (PropertyDescriptor propertyDescriptor : pds) {
            String propertyName = propertyDescriptor.getName();
            Object propertyValue = srcBean.getPropertyValue(propertyName);
            if (Objects.nonNull(propertyValue)) {
                properties.add(propertyName);
            }
        }
        return properties.toArray(new String[0]);
    }
}
