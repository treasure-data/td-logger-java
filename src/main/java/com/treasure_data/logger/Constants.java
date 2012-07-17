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

public interface Constants extends com.treasure_data.client.Constants {
    //////
    // For agent mode
    ////

    String TD_LOGGER_AGENTMODE = "td.logger.agentmode";

    String TD_LOGGER_AGENTMODE_DEFAULT = "true";

    String TD_LOGGER_AGENT_HOST = "td.logger.agent.host";

    String TD_LOGGER_AGENT_HOST_DEFAULT = "localhost";

    String TD_LOGGER_AGENT_PORT = "td.logger.agent.port";

    String TD_LOGGER_AGENT_PORT_DEFAULT = "24224";

    String TD_LOGGER_AGENT_TAG = "td.logger.agent.tag";

    String TD_LOGGER_AGENT_TAG_DEFAULT = "td";

    String TD_LOGGER_AGENT_TIMEOUT = "td.logger.agent.timeout";

    String TD_LOGGER_AGENT_TIMEOUT_DEFAULT = "3000";

    String TD_LOGGER_AGENT_BUFCAPACITY = "td.logger.agent.buffercapacity";

    String TD_LOGGER_AGENT_BUFCAPACITY_DEFAULT = "1048576"; // 1 * 1024 * 1024

    //////
    // For API server mode
    ////

    String TD_LOGGER_API_KEY = "td.logger.api.key";

    String TD_LOGGER_API_SERVER_HOST = "td.logger.api.server.host";

    String TD_LOGGER_API_SERVER_HOST_DEFAULT = "api.treasure-data.com";

    String TD_LOGGER_API_SERVER_PORT = "td.logger.api.server.port";

    String TD_LOGGER_API_SERVER_PORT_DEFAULT = "80";

    String TD_LOGGER_AUTO_CREATE_TABLE = "td.logger.create.table.auto";

    String TD_LOGGER_AUTO_CREATE_TABLE_DEFAULT = "false";
}
