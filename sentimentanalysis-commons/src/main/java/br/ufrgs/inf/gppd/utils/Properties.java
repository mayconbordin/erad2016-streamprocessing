package br.ufrgs.inf.gppd.utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Properties {
    private static final Logger LOGGER = LoggerFactory.getLogger(Properties.class);
    private static Properties singleton;

    private Configuration config;

    private Properties() {
        try {
            config = new PropertiesConfiguration(getClass().getResource("/sa.properties"));
        } catch (Exception ex) {
            LOGGER.error("Could not load configuration", ex);
            LOGGER.trace(null, ex);
        }
    }

    private static Properties get() {
        if (singleton == null)
            singleton = new Properties();
        return singleton;
    }

    public static String getString(String key) {
        return get().config.getString(key);
    }

    public static Integer getInt(String key) {
        return get().config.getInt(key);
    }
}
