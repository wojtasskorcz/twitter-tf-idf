package to.us.bachor.iosr;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Properties;

import org.apache.log4j.Logger;

public class Settings implements Serializable {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(Settings.class);

	private final static String SYSTEM_PROPERTY_SETTINGS_FILE_NAME = "tf.idf.propertiesfile";

	public static enum Key {
		REDIS_HOST("REDIS_HOST", "localhost"), REDIS_PORT("REDIS_PORT", "6379");

		public String propertyName;
		public String defaultValue;

		Key(String key, String defaultValue) {
			this.propertyName = key;
			this.defaultValue = defaultValue;
		}

	}

	private static Settings _instance;

	private Properties properties;

	private Settings(String settingsFileName) {
		InputStream settingsStream = this.getClass().getClassLoader().getResourceAsStream(settingsFileName);
		properties = new Properties();
		try {
			properties.load(settingsStream);
		} catch (IOException e) {
			logger.error("Cannot load properties from classpath file: " + settingsFileName
					+ " - defaults will be used.", e);
		}
	}

	public String getProperty(Key key) {
		return properties.getProperty(key.propertyName, key.defaultValue);
	}

	public int getIntegerProperty(Key key) {
		return Integer.valueOf(getProperty(key));
	}

	public static Settings getSettings() {
		return _instance;
	}

	static {
		String settingsFileName = System.getProperty(SYSTEM_PROPERTY_SETTINGS_FILE_NAME, "tf-idf.properties");
		_instance = new Settings(settingsFileName);
	}

}
