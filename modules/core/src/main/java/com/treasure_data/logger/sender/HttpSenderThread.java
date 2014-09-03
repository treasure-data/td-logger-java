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
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.treasure_data.client.ClientException;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.model.Database;
import com.treasure_data.model.Table;
import com.treasure_data.logger.Config;

class HttpSenderThread implements Runnable {
    private static Logger LOG = Logger.getLogger(HttpSenderThread.class.getName());

    private HttpSender sender;

    private TreasureDataClient client;

    private AtomicBoolean flushNow = new AtomicBoolean(true);

    private long flushInterval = 10; // sec

    private long maxFlushInterval = 300; // sec

    private double retryWait = 1.0;

    private int retryLimit = 12;

    private AtomicBoolean finished = new AtomicBoolean(false);

    private long nextTime = (System.currentTimeMillis() / 1000) + flushInterval;

    private int errorCount = 0;

    HttpSenderThread(HttpSender sender, TreasureDataClient client) {
        this.sender = sender;
        this.client = client;
    }

    @Override
    public void run() {
        upload();
    }

    void upload() {
        while (!finished.get()) {
            long now = System.currentTimeMillis() / 1000;

            boolean flushed = false;
            if (nextTime <= now || (flushNow.get() && errorCount == 0)) {
                flushed = tryFlush();
                flushNow.set(false);
            }

            long nextWait;
            if (errorCount == 0) {
                if (flushed && flushInterval < maxFlushInterval) {
                    flushInterval = Math.min(flushInterval + 60, maxFlushInterval);
                }
                nextWait = flushInterval;
            } else {
                nextWait = (long) (retryWait * Math.pow(2, errorCount - 1));
            }

            nextTime = nextWait + now;

            try {
                Thread.sleep(nextWait * 1000);
            } catch (InterruptedException e) {
            }
        }
    }

    boolean tryFlush() {
        // buffer is stored on queue
        if (sender.queue.isEmpty()) {
            try {
                sender.flush0(true);
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "Failed to store event logs on queue, trashed", e);
            }
        }

        boolean flushed = false;

        // data stored on queue is flushed
        while (!sender.queue.isEmpty()) {
            try {
                QueueEvent ev = sender.queue.take();

                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("Toke event from queue (size: %d)", sender.queue.size()));
                }

                uploadEvent(ev);
                flushed = true;
            } catch (Exception e) {
                if (errorCount < retryLimit) {
                    LOG.log(Level.WARNING, "Failed to upload event logs to Treasure Data, retrying", e);
                    errorCount += 1;
                } else {
                    LOG.log(Level.SEVERE, "Failed to upload event logs to Treasure Data, trashed", e);
                    errorCount = 0;
                    sender.queue.clear();
                }
            }
        }

        return flushed;
    }

    void uploadEvent(QueueEvent ev) throws IOException, ClientException {
        boolean retry = true;
        while (retry) {
            try {
                LOG.info(String.format("Uploading event logs to %s.%s on TreasureData (%d bytes, %d records)",
                        ev.databaseName, ev.tableName, ev.data.length, ev.rowSize));

                Table table = new Table(new Database(ev.databaseName), ev.tableName);

                long time = System.currentTimeMillis();
                client.importData(table, ev.data);
                time = System.currentTimeMillis() - time;

                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("Uploaded in %d sec.", time));
                }

                retry = false;
            } catch (ClientException e) {
                if (!Boolean.parseBoolean(System.getProperty(
                        Config.TD_LOGGER_AUTO_CREATE_TABLE, Config.TD_LOGGER_AUTO_CREATE_TABLE_DEFAULT))) {
                    throw e;
                }

                LOG.info(String.format("Creating table %s.%s on Treasure Data",
                        new Object[] { ev.databaseName, ev.tableName }));
                try {
                    client.createTable(ev.databaseName, ev.tableName);
                } catch (ClientException e0) {
                    client.createDatabase(ev.databaseName);
                    client.createTable(ev.databaseName, ev.tableName);
                }
            }
        }
    }

    synchronized void flush() {
        flushNow.set(true);
        tryFlush();
    }

    synchronized void stop() {
        finished.set(true);
        flush();
    }
}
