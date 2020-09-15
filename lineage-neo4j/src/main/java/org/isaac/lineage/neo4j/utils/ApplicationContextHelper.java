package org.isaac.lineage.neo4j.utils;

import java.lang.reflect.Method;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractRefreshableApplicationContext;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

/**
 * <p>
 * description
 * </p>
 *
 * @author isaac 2020/9/15 15:47
 * @since 1.0.0
 */
@Component("pluginApplicationContextHelper")
public class ApplicationContextHelper implements ApplicationContextAware {

    private static final Logger LOGGER = LoggerFactory.getLogger(ApplicationContextHelper.class);

    private static DefaultListableBeanFactory springFactory;

    private static ApplicationContext context;

    @Override
    public void setApplicationContext(@NonNull ApplicationContext applicationContext) {
        ApplicationContextHelper.setContext(applicationContext);
        if (applicationContext instanceof AbstractRefreshableApplicationContext) {
            AbstractRefreshableApplicationContext springContext =
                    (AbstractRefreshableApplicationContext) applicationContext;
            ApplicationContextHelper.setFactory((DefaultListableBeanFactory) springContext.getBeanFactory());
        } else if (applicationContext instanceof GenericApplicationContext) {
            GenericApplicationContext springContext = (GenericApplicationContext) applicationContext;
            ApplicationContextHelper.setFactory(springContext.getDefaultListableBeanFactory());
        }
    }

    private static void setContext(ApplicationContext applicationContext) {
        ApplicationContextHelper.context = applicationContext;
    }

    private static void setFactory(DefaultListableBeanFactory springFactory) {
        ApplicationContextHelper.springFactory = springFactory;
    }

    public static DefaultListableBeanFactory getSpringFactory() {
        return springFactory;
    }

    public static ApplicationContext getContext() {
        return context;
    }

    /**
     * 异步从 ApplicationContextHelper 获取 bean 对象并设置到目标对象中，在某些启动期间需要初始化的bean可采用此方法。
     * 适用于实例方法注入。
     *
     * @param type         bean type
     * @param target       目标类对象
     * @param setterMethod setter 方法，target 中需包含此方法名，且类型与 type 一致
     * @param <T>          type
     */
    public static <T> void asyncInstanceSetter(Class<T> type, Object target, String setterMethod) {
        if (ApplicationContextHelper.getContext() != null) {
            try {
                Method method = target.getClass().getDeclaredMethod(setterMethod, type);
                method.setAccessible(true);
                method.invoke(target, ApplicationContextHelper.getContext().getBean(type));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            LOGGER.info("setter {} to {} success.", type.getName(), target.getClass().getName());
            return;
        }
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "sync-setter"));
        executorService.scheduleAtFixedRate(() -> {
            if (ApplicationContextHelper.getContext() != null) {
                try {
                    Method method = target.getClass().getDeclaredMethod(setterMethod, type);
                    method.setAccessible(true);
                    method.invoke(target, ApplicationContextHelper.getContext().getBean(type));
                    LOGGER.info("setter {} to {} success.", type.getName(), target.getClass().getName());
                } catch (Exception e) {
                    LOGGER.error("setter {} to {} failure.", type.getName(), target.getClass().getName(), e);
                }
                executorService.shutdown();
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

    /**
     * 异步从 ApplicationContextHelper 获取 bean 对象并设置到目标对象中，在某些启动期间需要初始化的bean可采用此方法。
     * 适用于静态方法注入。
     *
     * @param type         bean type
     * @param target       目标类对象
     * @param setterMethod setter 方法，target 中需包含此方法名，且类型与 type 一致
     */
    public static void asyncStaticSetter(Class<?> type, Class<?> target, String setterMethod) {
        if (ApplicationContextHelper.getContext() != null) {
            try {
                Method method = target.getDeclaredMethod(setterMethod, type);
                method.setAccessible(true);
                method.invoke(null, ApplicationContextHelper.getContext().getBean(type));
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
            LOGGER.info("setter {} to {} success.", type.getName(), target.getName());
            return;
        }
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(1, r -> new Thread(r, "sync-setter"));
        executorService.scheduleAtFixedRate(() -> {
            if (ApplicationContextHelper.getContext() != null) {
                try {
                    Method method = target.getDeclaredMethod(setterMethod, type);
                    method.setAccessible(true);
                    method.invoke(target, ApplicationContextHelper.getContext().getBean(type));
                    LOGGER.info("setter {} to {} success.", type.getName(), target.getName());
                } catch (Exception e) {
                    LOGGER.error("setter {} to {} failure.", type.getName(), target.getName(), e);
                }
                executorService.shutdown();
            }
        }, 0, 1, TimeUnit.SECONDS);
    }

}
