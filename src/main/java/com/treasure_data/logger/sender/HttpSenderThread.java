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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.treasure_data.model.HttpClient;

class HttpSenderThread implements Runnable {
    private static Logger LOG = LoggerFactory.getLogger(HttpSenderThread.class);

    private MessagePack msgpack;

    private LinkedBlockingQueue<QueueEvent> queue;

    private HttpClient client;

    private AtomicBoolean finished = new AtomicBoolean(false);

    private Map<String, BufferPacker> map = new ConcurrentHashMap<String, BufferPacker>();

    private int chunkLimit = 8 * 1024 * 1024; // 8MB

    HttpSenderThread(LinkedBlockingQueue<QueueEvent> queue, HttpClient client) {
        this.msgpack = new MessagePack();
        this.queue = queue;
        this.client = client;
    }

    @Override
    public void run() {
        while (!finished.get()) {
            try {
                QueueEvent ev = queue.take();
                String key = ev.databaseName + "." + ev.tableName;
                BufferPacker packer;
                if (!map.containsKey(key)) {
                    packer = msgpack.createBufferPacker(4096);
                    map.put(key, packer);
                } else {
                    packer = map.get(key);
                }

                byte[] bytes = null;
                try {
                    packer.write(ev);
                    // TODO toByteArray -> getSize
                    if (packer.toByteArray().length > chunkLimit) {
                        
                    }
                } catch (IOException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } catch (InterruptedException e) {
            }
        }

        // TODO #MN write code for pre stop process
        if (queue.size() != 0) {
            // TODO
        }

        if (!map.isEmpty()) {
            // TODO
        }
    }

    synchronized void stop() {
        finished.set(true);
    }
}
