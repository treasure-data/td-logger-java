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
package com.treasure_data.logger.sender;

import java.io.IOException;
import java.util.Map;

import org.fluentd.logger.sender.Event;
import org.fluentd.logger.sender.Sender;
import org.msgpack.MessagePack;
import org.msgpack.template.Templates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpSender implements Sender {

    private static Logger LOG = LoggerFactory.getLogger(HttpSender.class);

    private MessagePack msgpack;

    public HttpSender() {
        msgpack = new MessagePack();
    }

    public void emit(String tag, Map<String, String> data) {
        emit(tag, System.currentTimeMillis(), data);
    }

    public void emit(String tag, long timestamp, Map<String, String> data) {
        if (LOG.isDebugEnabled()) { // for debug
            LOG.debug(String.format("Emit event{tag=%s,ts=%d,data=%s}",
                    new Object[] { tag, timestamp, data.toString() }));
        }

        String[] splited = tag.split(".");
        String database = splited[splited.length - 2];
        String table = splited[splited.length - 1];

        // validation
//        begin
//          TreasureData::API.validate_database_name(db)
//        rescue
//          @logger.error "TreasureDataLogger: Invalid database name #{db.inspect}: #{$!}"
//          raise "Invalid database name #{db.inspect}: #{$!}"
//        end
//        begin
//          TreasureData::API.validate_table_name(table)
//        rescue
//          @logger.error "TreasureDataLogger: Invalid table name #{table.inspect}: #{$!}"
//          raise "Invalid table name #{table.inspect}: #{$!}"
//        end

        byte[] bytes = null;
        try {
            // serialize tag, timestamp and data
            bytes = msgpack.write(data, Templates.tMap(Templates.TString, Templates.TString));
        } catch (IOException e) {
            LOG.error("Cannot serialize data: " + data, e);
        }


    }


    public byte[] getBuffer() {
        // TODO Auto-generated method stub
        return null;
    }

    public void close() {
        // TODO Auto-generated method stub

    }

}
