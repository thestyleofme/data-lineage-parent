package org.isaac.lineage.hook.hive;

import java.io.File;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.FileBasedConfiguration;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.isaac.lineage.hook.hive.exceptions.HiveHookException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Application properties
 * </p>
 *
 * @author isaac 2020/9/7 16:02
 * @since 1.0.0
 */
public final class ApplicationProperties extends PropertiesConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationProperties.class);

    public static final String APPLICATION_PROPERTIES = "kafka-application.properties";

    private static volatile Configuration instance = null;

    private ApplicationProperties() {

    }

    public static void forceReload() {
        if (instance != null) {
            synchronized (ApplicationProperties.class) {
                if (instance != null) {
                    instance = null;
                }
            }
        }
    }

    public static Configuration get() {
        if (instance == null) {
            synchronized (ApplicationProperties.class) {
                if (instance == null) {
                    instance = get(APPLICATION_PROPERTIES);
                }
            }
        }
        return instance;
    }

    public static Configuration get(String fileName) {
        // get from jar path
        String confLocation = getProjectPath();
        try {
            URL url;
            File file = new File(confLocation, fileName);
            if (confLocation == null || !file.exists()) {
                LOG.info("Looking for {} in classpath", fileName);
                url = ApplicationProperties.class.getClassLoader().getResource(fileName);
                if (url == null) {
                    LOG.info("Looking for /{} in classpath", fileName);
                    url = ApplicationProperties.class.getClassLoader().getResource("/" + fileName);
                }
            } else {
                url = file.toURI().toURL();
            }
            LOG.info("Loading {} from {}", fileName, url);
            Parameters params = new Parameters();
            FileBasedConfigurationBuilder<FileBasedConfiguration> builder =
                    new FileBasedConfigurationBuilder<FileBasedConfiguration>(PropertiesConfiguration.class)
                            .configure(params.properties()
                                    .setURL(url));
            FileBasedConfiguration configuration = builder.getConfiguration();
            logConfiguration(configuration);
            return configuration;
        } catch (Exception e) {
            throw new HiveHookException("Failed to load application properties", e);
        }
    }

    private static void logConfiguration(Configuration configuration) {
        if (LOG.isDebugEnabled()) {
            Iterator<String> keys = configuration.getKeys();
            LOG.debug("Configuration loaded:");
            while (keys.hasNext()) {
                String key = keys.next();
                LOG.debug("{} = {}", key, configuration.getProperty(key));
            }
        }
    }

    private static String getProjectPath() {
        String filePath = null;
        try {
            URL url = ApplicationProperties.class.getProtectionDomain().getCodeSource().getLocation();
            if (url != null) {
                filePath = URLDecoder.decode(url.getPath(), StandardCharsets.UTF_8.name());
                filePath = filePath.replace("\\", "/");
                if (filePath.endsWith(".jar")) {
                    filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
                }
                File file = new File(filePath);
                filePath = file.getAbsolutePath();
            }
        } catch (Exception e) {
            // ignore
            filePath = null;
        }
        return filePath;
    }
}
