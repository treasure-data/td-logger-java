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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.template.Templates;

class ExtendedPacker {
    private static Logger LOG = Logger.getLogger(ExtendedPacker.class.getName());

    private static final int KEY_SOFT_LIMIT = 256;
    private static final int KEY_HARD_LIMIT = 512;

    private final MessagePack msgpack;

    private Packer packer;
    private ByteArrayOutputStream out;
    private GZIPOutputStream gzout;
    private List<String> keys;
    private long rowCount;

    ExtendedPacker(MessagePack msgpack) throws IOException {
        this.msgpack = msgpack;
        refresh();
    }

    private void refresh() throws IOException {
        out = new ByteArrayOutputStream();
        gzout = new GZIPOutputStream(out);
        packer = msgpack.createPacker(gzout);
        keys = new ArrayList<String>(KEY_SOFT_LIMIT);
        rowCount = 0;
    }

    synchronized ExtendedPacker write(Map<String, Object> v) throws IOException {
        packer.writeMapBegin(v.size());
        {
            for (Map.Entry<String, Object> entry : v.entrySet()) {
                String key = entry.getKey();
                if (!keys.contains(key)) {
                    keys.add(key);
                    if (keys.size() == KEY_SOFT_LIMIT) {
                        LOG.warning("Went over soft limit of record keys");
                    } else if (keys.size() == KEY_HARD_LIMIT) {
                        String msg = "Went over hard limit of record keys";
                        LOG.severe(msg);
                        throw new IllegalStateException(msg);
                    }
                }
                Templates.TString.write(packer, key);
                packer.write(entry.getValue());
            }
        }
        packer.writeMapEnd();
        rowCount++;
        return this;
    }

    int getChunkSize() {
        return out.size();
    }

    synchronized byte[] getByteArray() throws IOException {
        try {
            gzout.close();
            return out.toByteArray();

        } finally {
            refresh();
        }
    }

    long getRowCount() {
        return rowCount;
    }
}