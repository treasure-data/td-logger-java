package com.treasure_data.logger.sender;

import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.BufferPacker;

import com.treasure_data.logger.Config;
import com.treasure_data.model.HttpClient;
import com.treasure_data.model.TestHttpClient;

public class TestHttpSenderThread {

    private MessagePack msgpack = new MessagePack();

    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(TestHttpClient.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Ignore @Test
    public void testUploadEvent() throws Exception {
        Properties props = System.getProperties();
        String apiKey = props.getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        HttpSenderThread thread = new HttpSenderThread(null, client);

        BufferPacker packer = msgpack.createBufferPacker();
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("k1", "v1");
        data.put("time", System.currentTimeMillis());
        packer.write(data);
        byte[] bytes = packer.toByteArray();
        QueueEvent ev = new QueueEvent("mugatest", "table1", bytes);
        thread.uploadEvent(ev);
    }
}
