//
// Treasure Data Logger for Java.
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.logger;

import java.util.Map;
import java.util.Properties;
import java.util.WeakHashMap;
import java.util.logging.Logger;

import org.fluentd.logger.FluentLogger;
import org.fluentd.logger.sender.RawSocketSender;
import org.fluentd.logger.sender.Sender;

import com.treasure_data.logger.sender.HttpSender;

public class TreasureDataLogger extends FluentLogger {

    private static Logger LOG = Logger.getLogger(TreasureDataLogger.class.getName());

    private static Map<String, TreasureDataLogger> loggers =
        new WeakHashMap<String, TreasureDataLogger>();

    public static TreasureDataLogger getLogger(String database) {
        Properties props = System.getProperties();
        return getLogger(database, props);
    }

    /**
     * Define order for API key lookup.
     * 1. lookup props's 'td.logger.api.key'
     */
    private static String lookupApiKey(Properties props) {
        String apiKey = null;
        // lookup from system property td.logger.api.key. If it exists, writes/updates td.api.key.
        if (props.containsKey(Config.TD_LOGGER_API_KEY)) {
            apiKey = props.getProperty(Config.TD_LOGGER_API_KEY);
            props.setProperty(Config.TD_API_KEY, apiKey);
        }
        return apiKey;
    }

    /**
     * Define order for hostname lookup.
     * 1. lookup props's td.logger.api.server.host
     */
    private static String lookupHost(Properties props) {
        String host = null;

        // 1. lookup system property 'td.logger.api.server.host'
        if (props.containsKey(Config.TD_LOGGER_API_SERVER_HOST)) {
            host = props.getProperty(Config.TD_LOGGER_API_SERVER_HOST);
        }

        // if not exists, set default value
        if (host == null) {
            host = Config.TD_LOGGER_API_SERVER_HOST_DEFAULT;
        }

        // write/overwrite it to 'td.api.server.host'
        props.setProperty(Config.TD_API_SERVER_HOST, host);
        return host;
    }

    /**
     * Define order for port num lookup.
     * 1. lookup props's td.logger.api.server.port
     */
    private static String lookupPort(Properties props) {
        String port = null;

        // 1. lookup system property 'td.logger.api.server.port'
        if (props.containsKey(Config.TD_LOGGER_API_SERVER_PORT)) {
            port = props.getProperty(Config.TD_LOGGER_API_SERVER_PORT);
        }

        // if not exists, set defualt value
        if (port == null) {
            port = Config.TD_LOGGER_API_SERVER_PORT_DEFAULT;
        }

        // write/overwrite it to 'td.api.server.port'
        props.setProperty(Config.TD_API_SERVER_PORT, port);
        return port;
    }

    /**
     * Define order for scheme lookup.
     * 1. lookup props's td.logger.api.server.scheme
     */
    private static String lookupScheme(Properties props) {
        String scheme = null;

        // 1. lookup system property 'td.logger.api.server.scheme'
        if (props.containsKey(Config.TD_LOGGER_API_SERVER_SCHEME)) {
            scheme = props.getProperty(Config.TD_LOGGER_API_SERVER_SCHEME);
        }

        // if not exists, set default value
        if (scheme == null) {
            scheme = Config.TD_LOGGER_API_SERVER_SCHEME_DEFAULT;
        }

        // write/overwrite it to 'td.api.server.port'
        props.setProperty(Config.TD_CK_API_SERVER_SCHEME, scheme);
        return scheme;
    }

    public static synchronized TreasureDataLogger getLogger(
            String database, Properties props) {
        if (database == null || database.equals("")) {
            throw new NullPointerException(
                    "Cannot specify null or null charactor as value of database");
        }

        String key, host, apiKey = null, agentTag = null;
        int port, timeout = 0, bufferCapacity = 0;

        boolean agentMode = Boolean.parseBoolean(props.getProperty(
                Config.TD_LOGGER_AGENTMODE,
                Config.TD_LOGGER_AGENTMODE_DEFAULT));

        if (!agentMode) {
            apiKey = lookupApiKey(props);
            host = lookupHost(props);
            port = Integer.parseInt(lookupPort(props));
            lookupScheme(props);

            key = String.format("%s_%s_%s_%d", database, apiKey, host, port);
        } else {
            host = lookupHost(props);
            port = Integer.parseInt(lookupPort(props));

            agentTag = props.getProperty(
                    Config.TD_LOGGER_AGENT_TAG,
                    Config.TD_LOGGER_AGENT_TAG_DEFAULT);
            timeout = Integer.parseInt(props.getProperty(
                    Config.TD_LOGGER_AGENT_TIMEOUT,
                    Config.TD_LOGGER_AGENT_TIMEOUT_DEFAULT));
            bufferCapacity = Integer.parseInt(props.getProperty(
                    Config.TD_LOGGER_AGENT_BUFCAPACITY,
                    Config.TD_LOGGER_AGENT_BUFCAPACITY_DEFAULT));
            key = String.format("%s_%s_%d_%s_%d_%d", database, host, port, agentTag, timeout, bufferCapacity);
        }

        if (loggers.containsKey(key)) {
            return loggers.get(key);
        }

        // create logger object
        LOG.info(String.format("Creates logger(%s)", key));
        TreasureDataLogger logger;
        if (!agentMode) {
            // connected to TD platform directly
            HttpSender sender = Config.createHttpSender(props, host, port, apiKey);
            LOG.info("created sender class: " + sender.getClass().getName());
            sender.startBackgroundProcess();
            logger = new TreasureDataLogger(database, sender);
        } else {
            // agent mode is connected to specified fluentd
            String tagPrefix;
            if(agentTag.isEmpty()) {
                tagPrefix = database;
            } else {
                tagPrefix = agentTag + "." + database;
            }
            logger = new TreasureDataLogger(tagPrefix,
                    new RawSocketSender(host, port, timeout, bufferCapacity));
        }

        loggers.put(key, logger);
        return logger;
    }

    public static synchronized void closeAll() {
        for (Map.Entry<String, TreasureDataLogger> e : loggers.entrySet()) {
            LOG.info(String.format("Close logger(%s)", e.getKey()));
            e.getValue().close();
        }
    }

    public static synchronized void flushAll() {
        for (Map.Entry<String, TreasureDataLogger> e : loggers.entrySet()) {
            LOG.info(String.format("Flush logger(%s)", e.getKey()));
            e.getValue().flush();
        }
    }

    protected TreasureDataLogger(String database, Sender sender) {
        super(database, sender);
    }

    @Override
    public boolean log(String label, String key, Object value) {
        return super.log(label, key, value);
    }

    @Override
    public boolean log(String label, String key, Object value, long timestamp) {
        return super.log(label, key, value, timestamp);
    }

    @Override
    public boolean log(String label, Map<String, Object> data) {
        return super.log(label, data);
    }

    @Override
    public boolean log(String label, Map<String, Object> data, long timestamp) {
        return super.log(label, data, timestamp);
    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public void flush() {
        super.flush();
    }

    @Override
    public String toString() {
        return super.toString();
    }

    @Override
    public void finalize() {
        super.finalize();
    }
}
