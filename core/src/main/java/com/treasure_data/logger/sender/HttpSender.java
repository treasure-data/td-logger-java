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
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;

import org.fluentd.logger.sender.Sender;
import org.msgpack.MessagePack;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.TreasureDataClient;

public class HttpSender implements Sender {
    private static Logger LOG = Logger.getLogger(HttpSender.class.getName());

    private static Pattern databasePat = Pattern.compile("^([a-z0-9_]+)$");

    private static Pattern columnPat = Pattern.compile("^([a-z0-9_]+)$");

    public static boolean validateDatabaseName(String name) {
        if (name == null || name.equals("")) {
            LOG.info(String.format("Empty name is not allowed: %s",
                    new Object[] { name }));
            return false;
        }
        int len = name.length();
        if (len < 3 || 32 < len) {
            LOG.info(String.format("Name must be 3 to 32 characters, got %d characters.",
                    new Object[] { len }));
            return false;
        }

        if (!databasePat.matcher(name).matches()) {
            LOG.info("Name must consist only of alphabets, numbers, '_'.");
            return false;
        }

        return true;
    }

    public static boolean validateTableName(String name) {
        return validateDatabaseName(name);
    }

    public static boolean validateColumnName(String name) {
        if (name == null || name.equals("")) {
            LOG.info(String.format("Empty column name is not allowed: %s",
                    new Object[] { name }));
            return false;
        }

        int len = name.length();
        if (32 < len) {
            LOG.info(String.format("Column name must be to 32 characters, got %d characters.",
                    new Object[] { len }));
            return false;
        }

        if (!columnPat.matcher(name).matches()) {
            LOG.info("Column name must consist only of alphabets, numbers, '_'.");
            return false;
        }

        return true;
    }

    private MessagePack msgpack;

    private Map<String, ExtendedPacker> chunks;

    private int chunkLimit = 8 * 1024 * 1024; // 8MB

    protected String host;
    protected int port;
    protected String apiKey;

    LinkedBlockingQueue<QueueEvent> queue;
    private int queueLimit = 50;
    private HttpSenderThread senderThread;

    private String name;

    public HttpSender(final String host, final int port, final String apiKey) {
        if (apiKey == null) {
            throw new IllegalArgumentException("APIKey is required");
        }

        msgpack = new MessagePack();
        chunks = new ConcurrentHashMap<String, ExtendedPacker>();
        this.host = host;
        this.port = port;
        this.apiKey = apiKey;
        name = String.format("%s_%d", host, port);
    }

    public void startBackgroundProcess() {
        queue = new LinkedBlockingQueue<QueueEvent>();
        TreasureDataClient client = new TreasureDataClient(new TreasureDataCredentials(apiKey), System.getProperties());
        senderThread = new HttpSenderThread(this, client);
        new Thread(senderThread).start();
    }

    private synchronized Map<String, ExtendedPacker> recreateChunks() {
        Map<String, ExtendedPacker> tmp = chunks;
        chunks = new ConcurrentHashMap<String, ExtendedPacker>();
        return tmp;
    }

    public boolean emit(String tag, Map<String, Object> record) {
        return emit(tag, System.currentTimeMillis() / 1000, record);
    }

    public boolean emit(String tag, long timestamp, Map<String, Object> record) {
        record.put("time", timestamp);
        return emit0(tag, record);
    }

    private boolean emit0(String tag, Map<String, Object> record) {
        if (LOG.isLoggable(Level.FINE)) {
            LOG.fine(String.format("Emit event{tag=%s,record=%s}",
                    new Object[] { tag, record.toString() }));
        }

        String[] splited = tag.split("\\.");
        String databaseName = splited[splited.length - 2];
        String tableName = splited[splited.length - 1];

        // validation
        if (!validateDatabaseName(databaseName)) {
            String msg = String.format("Invalid database name %s", new Object[] { databaseName });
            LOG.severe(msg);
            throw new IllegalArgumentException(msg);
        }
        if (!validateTableName(tableName)) {
            String msg = String.format("Invalid table name %s", new Object[] { tableName });
            LOG.severe(msg);
            throw new IllegalArgumentException(msg);
        }

        // check queue limit
        if (getQueueSize() > queueLimit) {
            LOG.severe("queue length exceeds limit. cannot add new event log");
            return false;
        }

        String key = databaseName + "." + tableName;
        ExtendedPacker packer = null;
        try {
            packer = getPacker(key);
        } catch (IOException e) {
            LOG.severe("Cannot create packer object");
            LOG.throwing(this.getClass().getName(), "emit0", e);
            return false;
        }

        // write data to chunk
        try {
            packer.write(record);
        } catch (Exception e) {
            LOG.severe(String.format("Cannot serialize data to %s.%s",
                    databaseName, tableName));
            chunks.remove(key);
            return false;
        }

        if (packer.getChunkSize() > chunkLimit) {
            try {
                putQueue(databaseName, tableName, packer.getByteArray());

                if (LOG.isLoggable(Level.FINE)) {
                    LOG.fine(String.format("Put event on queue (size: %d)",
                            new Object[] { getQueueSize() }));
                }
            } catch (IOException e) {
                LOG.throwing(this.getClass().getName(), "emit0", e);
            } finally {
                chunks.remove(key);
            }
        }
        return true;
    }

    protected int getQueueSize() {
        return queue.size();
    }

    protected void putQueue(String databaseName, String tableName, byte[] bytes) {
        try {
            queue.put(new QueueEvent(databaseName, tableName, bytes));
        } catch (InterruptedException e) { // ignore
        }
    }

    private synchronized ExtendedPacker getPacker(String key) throws IOException {
        ExtendedPacker packer = null;
        if (!chunks.containsKey(key)) {
            packer = new ExtendedPacker(msgpack);
            chunks.put(key, packer);
        } else {
            packer = chunks.get(key);
        }
        return packer;
    }

    public byte[] getBuffer() { // TODO #MN need the impl. for testing
        throw new UnsupportedOperationException();
    }

    public void flush() {
        try {
            flush0(false);
        } catch (IOException e) {
            // ignore
        } finally {
            senderThread.flush();
        }
    }

    public void close() {
        try {
            flush0(false);
        } catch (IOException e) {
            // ignore
        } finally {
            senderThread.stop();
        }
    }

    synchronized void flush0(boolean isThread) throws IOException {
        if (!chunks.isEmpty()) {
            Map<String, ExtendedPacker> chunks0 = recreateChunks();
            for (Map.Entry<String, ExtendedPacker> entry : chunks0.entrySet()) {
                String[] splited = entry.getKey().split("\\.");
                String databaseName = splited[0];
                String tableName = splited[1];
                try {
                    byte[] bytes = entry.getValue().getByteArray();
                    putQueue(databaseName, tableName, bytes);
                } catch (IOException e) {
                    LOG.throwing(this.getClass().getName(), "flushChunks", e);
                }
            }
        }
        if (!isThread) {
            senderThread.flush();
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }

}
