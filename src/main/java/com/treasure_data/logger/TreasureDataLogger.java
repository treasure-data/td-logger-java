//
// Treasure Data Logger for Java.
//
// Copyright (C) 2011 - 2012 Muga Nishizawa
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

import org.fluentd.logger.FluentLogger;
import org.fluentd.logger.sender.RawSocketSender;
import org.fluentd.logger.sender.Sender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.treasure_data.logger.sender.HttpSender;

public class TreasureDataLogger extends FluentLogger {

    private static Logger LOG = LoggerFactory.getLogger(TreasureDataLogger.class);

    private static Map<String, TreasureDataLogger> loggers = new WeakHashMap<String, TreasureDataLogger>();

    public static TreasureDataLogger getLogger(String database) {
        Properties props = System.getProperties();
        return getLogger(database, props);
    }

    public static synchronized TreasureDataLogger getLogger(String database, Properties props) {
        if (database == null || database.equals("")) {
            throw new NullPointerException(
                    "Cannot specify null or null charactor as value of database");
        }

        String key, host, apiKey = null;
        int port, timeout = 0, bufferCapacity = 0;

        boolean agentMode = Boolean.parseBoolean(props.getProperty(
                Config.TD_LOGGER_AGENTMODE, Config.TD_LOGGER_AGENTMODE_DEFAULT));

        apiKey = System.getenv(Config.TD_ENV_API_KEY);
        if (apiKey != null && !apiKey.equals("")) {
            agentMode = false;
        }

        if (!agentMode) {
            if (apiKey == null) {
                apiKey = props.getProperty(Config.TD_LOGGER_API_KEY);
            }
            if (apiKey == null) {
                throw new IllegalArgumentException(
                        String.format("APIKey option is required as java property: %s",
                                new Object[] { Config.TD_LOGGER_API_KEY }));
            }
            host = props.getProperty(
                    Config.TD_LOGGER_API_SERVER_HOST, Config.TD_LOGGER_API_SERVER_HOST_DEFAULT);
            port = Integer.parseInt(props.getProperty(
                    Config.TD_LOGGER_API_SERVER_PORT, Config.TD_LOGGER_API_SERVER_PORT_DEFAULT));
            key = String.format("%s_%s_%s_%d", new Object[] { database, apiKey, host, port });
        } else {
            host = props.getProperty(
                    Config.TD_LOGGER_AGENT_HOST, Config.TD_LOGGER_AGENT_HOST_DEFAULT);
            port = Integer.parseInt(props.getProperty(
                    Config.TD_LOGGER_AGENT_PORT, Config.TD_LOGGER_AGENT_PORT_DEFAULT));
            timeout = Integer.parseInt(props.getProperty(
                    Config.TD_LOGGER_AGENT_TIMEOUT, Config.TD_LOGGER_AGENT_TIMEOUT_DEFAULT));
            bufferCapacity = Integer.parseInt(props.getProperty(
                    Config.TD_LOGGER_AGENT_BUFCAPACITY, Config.TD_LOGGER_AGENT_BUFCAPACITY_DEFAULT));
            key = String.format("%s_%s_%d_%d_%d",
                    new Object[] { database, host, port, timeout, bufferCapacity });
        }

        if (loggers.containsKey(key)) {
            return loggers.get(key);
        } else {
            // create logger object
            LOG.info(String.format("Creates logger(%s)", new Object[] { key }));
            TreasureDataLogger logger;
            if (!agentMode) {
                // connected to TD platform directly
                logger = new TreasureDataLogger(database,
                        new HttpSender(host, port, apiKey));
            } else {
                // agent mode is connected to specified fluentd
                logger = new TreasureDataLogger(database,
                        new RawSocketSender(host, port, timeout, bufferCapacity));
            }
            loggers.put(key, logger);
            return logger;
        }
    }

    public static synchronized void closeAll() {
        for (Map.Entry<String, TreasureDataLogger> e : loggers.entrySet()) {
            LOG.info(String.format("Closes logger(%s)", e.getKey()));
            e.getValue().close();
        }
    }

    public static synchronized void flushAll() {
        for (Map.Entry<String, TreasureDataLogger> e : loggers.entrySet()) {
            LOG.info(String.format("Flushes logger(%s)", e.getKey()));
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
