package com.bigsonata.commons;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class RegistryBase {
    protected static Props props;

    public static void initialize() {
        try {
            props = Props.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    public static class Entry {
        private final String key;

        public Entry(String key) {
            this.key = key;
        }

        public static Entry create(String key) {
            return new Entry(key);
        }

        public String valueString() throws Exception {
            return props.getProperty(key);
        }

        public String valueString(String defaultValue) {
            return props.getProperty(key, defaultValue);
        }

        public int valueInt() throws Exception {
            return props.getPropertyInt(key);
        }

        public int valueInt(int defaultValue) {
            return props.getPropertyInt(key, defaultValue);
        }

        public boolean valueBoolean() throws Exception {
            return props.getPropertyBoolean(key);
        }

        public boolean valueBoolean(boolean defaultValue) {
            return props.getPropertyBoolean(key, defaultValue);
        }
    }

    public static class Props {
        private static Logger logger = LoggerFactory.getLogger(Props.class.getCanonicalName());
        private Properties props = new Properties();

        /**
         * Construct a new Props from a configuration (main) file
         *
         * @param filePath Path to the file
         * @throws IOException If there's no such file, throw IOException
         */
        public Props(String filePath) throws IOException {
            props.load(new FileInputStream(filePath));
        }

        private static int parseInt(String input, int defaultValue) {
            try {
                return Integer.parseInt(input);
            } catch (Exception e) {
                return defaultValue;
            }
        }

        private static int parseInt(String input) throws NumberFormatException {
            return Integer.parseInt(input);
        }

        private static long parseLong(String input) throws NumberFormatException {
            return Long.parseLong(input);
        }

        /**
         * Construct a new instance of Props by reading the configuration file passed via `-Dconf=`
         *
         * @return An instance. If there are no such files, return null
         */
        public static Props newInstance() throws Exception {
            String filePath = System.getProperty("appRegistry");
            if (filePath == null || filePath.length() == 0) {
                filePath = "registry.properties";
            }

            return newInstance(filePath);
        }

        public static Props newInstance(String filePath) throws Exception {
            try {
                return new Props(filePath);
            } catch (Exception e) {
                logger.warn("Can NOT read `{}`. Reason: {}", filePath, e.getMessage());
                throw e;
            }
        }

        /**
         * Get an numeric value associated with a key
         *
         * @param key The key
         * @return The value
         * @throws Exception If there's no such key, throw Exceptions
         */
        public int getPropertyInt(String key) throws Exception {
            try {
                String value = getProperty(key);
                if (value == null) {
                    throw new Exception("No such key " + key);
                }
                return parseInt(value);
            } catch (NumberFormatException e) {
                logger.error("Invalid numeric value for key {}", key);
                throw e;
            } catch (Exception e) {
                logger.error("No such key {}", key);
                throw e;
            }
        }

        public long getPropertyLong(String key) throws Exception {
            try {
                String value = getProperty(key);
                if (value == null) {
                    throw new Exception("No such key " + key);
                }
                return parseLong(value);
            } catch (NumberFormatException e) {
                logger.error("Invalid numeric value for key {}", key);
                throw e;
            } catch (Exception e) {
                logger.error("No such key {}", key);
                throw e;
            }
        }

        /**
         * Get an numeric value associated with a key
         *
         * @param key The key
         * @param defaultValue Default value to return
         * @return The value. If there's no such key, the default value will be returned.
         */
        public int getPropertyInt(String key, int defaultValue) {
            try {
                return getPropertyInt(key);
            } catch (Exception e) {
                return defaultValue;
            }
        }

        public long getPropertyLong(String key, long defaultValue) {
            try {
                return getPropertyLong(key);
            } catch (Exception e) {
                return defaultValue;
            }
        }

        /**
         * Get a boolean value associated with a key
         *
         * @param key The key
         * @param defaultValue Default value to return
         * @return The value. If there's no such key, the default value will be returned.
         */
        public boolean getPropertyBoolean(String key, boolean defaultValue) {
            try {
                return getProperty(key, "false").toLowerCase().equals("true");
            } catch (Exception e) {
                return defaultValue;
            }
        }

        /**
         * Get a boolean value associated with a key
         *
         * @param key The key
         * @return The value. If there's no such key, the default value will be returned.
         */
        boolean getPropertyBoolean(String key) throws Exception {
            String value = getProperty(key);
            return value.toLowerCase().equals("true");
        }

        /**
         * Get a value associated with a key
         *
         * @param key The key
         * @return The value
         * @throws Exception If there's no such key, throw Exceptions
         */
        public String getProperty(String key) throws Exception {
            try {
                String res = props.getProperty(key);

                if (res != null) {
                    return res;
                }
                throw new Exception("No such key " + key);
            } catch (Exception e) {
                logger.error("No such key {}", key);
                throw e;
            }
        }

        /**
         * Get a string value associated with a key
         *
         * @param key The key
         * @param defaultValue Default value to return
         * @return The value. If there's no such key, the default value will be returned.
         */
        public String getProperty(String key, String defaultValue) {
            try {
                return getProperty(key);
            } catch (Exception e) {
                return defaultValue;
            }
        }
    }
}
