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
package com.treasure_data.logger.sender;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.template.Templates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ExtendedPacker {
    private static Logger LOG = LoggerFactory.getLogger(ExtendedPacker.class);

    private static int keySoftLimit = 256;

    private static int keyHardLimit = 512;

    private Packer packer;

    private ByteArrayOutputStream out;

    private GZIPOutputStream gzout;

    private List<String> keys = new ArrayList<String>(keySoftLimit);

    ExtendedPacker(MessagePack msgpack) throws IOException {
        out = new ByteArrayOutputStream();
        gzout = new GZIPOutputStream(out);
        packer = msgpack.createPacker(gzout);
    }

    ExtendedPacker write(Map<String, Object> v) throws IOException {
        packer.writeMapBegin(v.size());
        {
            for (Map.Entry<String, Object> entry : v.entrySet()) {
                String key = entry.getKey();
                if (!keys.contains(key)) {
                    keys.add(key);
                    if (keys.size() == keySoftLimit) {
                        LOG.warn("Went over soft limit of record keys");
                    } else if (keys.size() == keyHardLimit) {
                        String msg = "Went over hard limit of record keys";
                        LOG.error(msg);
                        throw new IllegalStateException(msg);
                    }
                }
                Templates.TString.write(packer, key);
                packer.write(entry.getValue());
            }
        }
        packer.writeMapEnd();
        return this;
    }

    int getChunkSize() {
        return out.size();
    }

    byte[] getByteArray() throws IOException {
        gzout.finish();
        return out.toByteArray();
    }
}