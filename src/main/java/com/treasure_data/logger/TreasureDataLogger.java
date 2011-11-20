//
// Treasure Data Logger for Java.
//
// Copyright (C) 2011 Muga Nishizawa
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

public class TreasureDataLogger extends FluentLogger {

    private static Map<String, TreasureDataLogger> loggers = new WeakHashMap<String, TreasureDataLogger>();

    public static TreasureDataLogger getLogger(String database) {
        Properties props = System.getProperties();
        return getLogger(database, props);
    }

    public static synchronized TreasureDataLogger getLogger(String database, Properties props) {
        String host;
        int port, timeout, bufferCapacity;
        boolean agentMode = Boolean.parseBoolean(props.getProperty(
                Config.TD_LOGGER_AGENTMODE, Config.TD_LOGGER_AGENTMODE_DEFAULT));

        if (!agentMode) {
            // TODO #MN must implement it soon
            throw new UnsupportedOperationException();// TODO
        } else {
            host = props.getProperty(
                    Config.TD_LOGGER_AGENT_HOST, Config.TD_LOGGER_AGENT_HOST_DEFAULT);
            port = Integer.parseInt(props.getProperty(
                    Config.TD_LOGGER_AGENT_PORT, Config.TD_LOGGER_AGENT_PORT_DEFAULT));
            timeout = Integer.parseInt(props.getProperty(
                    Config.TD_LOGGER_AGENT_TIMEOUT, Config.TD_LOGGER_AGENT_TIMEOUT_DEFAULT));
            bufferCapacity = Integer.parseInt(props.getProperty(
                    Config.TD_LOGGER_AGENT_BUFCAPACITY, Config.TD_LOGGER_AGENT_BUFCAPACITY_DEFAULT));
        }

        String key = String.format("%s_%s_%d_%d_%d",
                new Object[] { database, host, port, timeout, bufferCapacity });
        if (loggers.containsKey(key)) {
            return loggers.get(key);
        } else {
            TreasureDataLogger logger;
            if (!agentMode) { // connected to TD platform directly
                // TODO #MN must implement it soon
                logger = null;
                throw new UnsupportedOperationException();// TODO
            } else { // agent mode is connected to specified fluentd
                logger = new TreasureDataLogger(database, host, port, timeout, bufferCapacity);
            }
            loggers.put(key, logger);
            return logger;
        }
    }

    public static synchronized void close() {
        for (TreasureDataLogger logger : loggers.values()) {
            logger.close0();
        }
    }

    protected TreasureDataLogger(String database, String host, int port, int timeout, int bufferCapacity) {
        super(database, host, port, timeout, bufferCapacity);
    }

    @Override
    protected void close0() {
        super.close0();
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
