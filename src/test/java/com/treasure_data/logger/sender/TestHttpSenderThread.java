package com.treasure_data.logger.sender;

import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;
import org.msgpack.unpacker.Unpacker;
import org.msgpack.unpacker.UnpackerIterator;

import com.treasure_data.auth.TreasureDataCredentials;
import com.treasure_data.client.TreasureDataClient;
import com.treasure_data.logger.Config;

public class TestHttpSenderThread {

    @Before
    public void setUp() throws Exception {
    }

    @Test @Ignore
    public void testUploadEvent() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
        MessagePack msgpack = new MessagePack();

        String apiKey = props.getProperty(Config.TD_LOGGER_API_KEY);
        TreasureDataClient client = new TreasureDataClient(
                new TreasureDataCredentials(apiKey), props);
        HttpSenderThread thread = new HttpSenderThread(null, client);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzout = new GZIPOutputStream(out);
        Packer packer = msgpack.createPacker(gzout);
        for (int i = 0; i < 20; ++i) {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("k", i);
            data.put("time", System.currentTimeMillis() / 1000);
            packer.write(data);
        }
        gzout.finish();
        byte[] bytes = out.toByteArray();
        System.out.println(bytes.length);

        QueueEvent ev = new QueueEvent("mugadb", "table01", bytes);
        thread.uploadEvent(ev);
    }
}
