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

import java.lang.reflect.Constructor;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.treasure_data.logger.sender.HttpSender;

public class Config extends com.treasure_data.client.Config implements Constants {
    private static Logger LOG = Logger.getLogger(Config.class.getName());

    public static HttpSender createHttpSender(Properties props, String host, int port,
            String apiKey) {
        String senderClassName = props.getProperty(TD_LOGGER_HTTPSENDER_CLASS,
                TD_LOGGER_HTTPSENDER_CLASS_DEFAULT);

        LOG.info("use sender class: " + senderClassName);
        try {
            @SuppressWarnings("unchecked")
            Class<HttpSender> senderClass = (Class<HttpSender>) Class
                    .forName(senderClassName);
            Constructor<HttpSender> cons = senderClass.getConstructor(
                    Properties.class, String.class, int.class, String.class);
            return cons.newInstance(props, host, port, apiKey);
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Cannot create sender object by reflection. fall back", e);
        }
        return new HttpSender(props, host, port, apiKey);
    }

}
