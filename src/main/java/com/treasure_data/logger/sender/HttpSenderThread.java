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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.GZIPOutputStream;

import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.treasure_data.logger.Config;
import com.treasure_data.model.ClientException;
import com.treasure_data.model.HttpClient;

class HttpSenderThread implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(HttpSenderThread.class);

    private MessagePack msgpack;

    private LinkedBlockingQueue<QueueEvent> queue;

    private HttpClient client;

    private AtomicBoolean flushNow = new AtomicBoolean(false);

    private long flushInterval = 10 * 1000;

    private long maxFlushInterval = 300 * 1000;

    private double retryWait = 1.0;

    private int retryLimit = 12;

    private AtomicBoolean finished = new AtomicBoolean(false);

    private long nextTime = System.currentTimeMillis() + flushInterval;

    private int errorCount = 0;

    HttpSenderThread(LinkedBlockingQueue<QueueEvent> queue, HttpClient client) {
        this.msgpack = new MessagePack();
        this.queue = queue;
        this.client = client;
    }

    @Override
    public void run() {
        upload();
    }

    private void upload() {
        while (!finished.get()) {
            long now = System.currentTimeMillis();

            boolean flushed = false;
            if (nextTime <= now || (flushNow.get() && errorCount == 0)) {
                flushed = tryFlush();
                flushNow.set(false);
            }

            long nextWait;
            if (errorCount == 0) {
                if (flushed && flushInterval < maxFlushInterval) {
                    flushInterval = Math.min(flushInterval + 60 * 1000, maxFlushInterval);
                }
                nextWait = flushInterval;
            } else {
                nextWait = (long) (retryWait * Math.pow(2, errorCount - 1));
            }
            nextTime = nextWait + now;
        }
    }

    private boolean tryFlush() {
        boolean flushed = false;

        while (!queue.isEmpty()) {
            try {
                QueueEvent ev = queue.take();
                upload(ev);
                flushed = true;
            } catch (Exception e) {
                if (errorCount < retryLimit) {
                    LOG.error("Failed to upload event logs to Treasure Data, retrying", e);
                    errorCount += 1;
                } else {
                    LOG.error("Failed to upload event logs to Treasure Data, trashed", e);
                    errorCount = 0;
                    queue.clear();
                }
            }
        }

        return flushed;
    }

    private void upload(QueueEvent ev) throws IOException, ClientException {
        boolean retry = true;
        while (retry) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            GZIPOutputStream gzout = new GZIPOutputStream(out);
            gzout.write(ev.data);
            gzout.flush();
            gzout.close();
            byte[] bytes = out.toByteArray();

            LOG.debug(String.format("Uploading event logs to %s.%s table on Treasure Data (%d bytes)",
                    new Object[] { ev.databaseName, ev.tableName, ev.data.length }));

            try {
                client.importData(ev.databaseName, ev.tableName, "msgpack.gz", bytes);
                retry = false;
            } catch (ClientException e) { // TODO #MN ClientException?
                if (!Boolean.parseBoolean(System.getProperty(
                        Config.TD_LOGGER_AUTO_CREATE_TABLE, Config.TD_LOGGER_AUTO_CREATE_TABLE_DEFAULT))) {
                    throw e;
                }

                LOG.info(String.format("Creating table %s.%s on Treasure Data",
                        new Object[] { ev.databaseName, ev.tableName }));
                try {
                    client.createLogTable(ev.databaseName, ev.tableName);
                } catch (ClientException e0) {
                    client.createDatabase(ev.databaseName);
                    client.createLogTable(ev.databaseName, ev.tableName);
                }
            }
        }
    }

    synchronized void stop() {
        finished.set(true);
        flushNow.set(true);

        tryFlush();
    }
}