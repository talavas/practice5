package shpp.level3.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Config {
    private final Properties properties;
    private final String fileName;

    private static final Logger logger = LoggerFactory.getLogger(Config.class);
    public Config(String fileName) {
        this.fileName = fileName;
        properties = new Properties();

        loadPropertiesFromClasspath();

        if(properties.isEmpty()) {
            setDefaultProperties();
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }
    private void loadPropertiesFromClasspath() {
        try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(fileName)
        ) {
            if (inputStream != null) {
                logger.debug("Load properties from '{}' file.", fileName);
                properties.load(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
            }else{
                throw new IOException(fileName);
            }
        } catch (IOException e) {
            logger.warn("Can't get properties from file {}.", fileName, e);
        }
    }
    private void setDefaultProperties() {
        properties.setProperty("batch.size", "1000");
        properties.setProperty("threads", "1");

        logger.warn("Set default properties.");
    }
}
