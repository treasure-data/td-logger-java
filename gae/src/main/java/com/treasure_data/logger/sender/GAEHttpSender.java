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
package com.treasure_data.logger.sender;

import java.io.IOException;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.treasure_data.logger.sender.HttpSender;

public class GAEHttpSender extends HttpSender {
    private Queue gaequeue;

    public GAEHttpSender(String host, int port, String apiKey) {
        super(host, port, apiKey);
    }

    @Override
    public void startBackgroundProcess() {
        // TODO #MN queue name should be specified by users
        gaequeue = QueueFactory.getDefaultQueue();
    }

    @Override
    public int getQueueSize() {
        // TODO #MN should consider the return value more carefully?
        return 0;
    }

    @Override
    public void putQueue(String databaseName, String tableName, byte[] bytes) {
        // TODO #MN need refactoring
        //String urlpart = "api.treasure-data.com:80/v3/table/import/%s/%s/msgpack.gz";
        //String url = String.format(urlpart, databaseName, tableName);
        TaskOptions opts = Builder.withPayload(bytes, "application/octet-stream")
                .url("/mygae")
                .header("Content-Length", "" + bytes.length)
                .param("database", databaseName)
                .param("table", tableName)
                //.payload(bytes)
                .method(Method.PUT);
        gaequeue.add(opts);
    }

    @Override
    public void flush() {
        try {
            flush0(true);
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public void close() {
        try {
            flush0(true);
        } catch (IOException e) {
            // ignore
        }
    }
}
