package com.treasure_data.logger.sender;

import static org.junit.Assert.assertTrue;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.zip.GZIPOutputStream;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

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

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Packer packer = msgpack.createPacker(out);
        for (int i = 0; i < 1000; ++i) {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("k:" + i, "v:" + i);
            data.put("time", System.currentTimeMillis() / 1000);
            packer.write(data);
        }
        byte[] bytes = out.toByteArray();
        System.out.println(bytes.length);

        ByteArrayOutputStream out2 = new ByteArrayOutputStream();
        //FileOutputStream out2 = new FileOutputStream("msgpack.gz");
        GZIPOutputStream gzout = new GZIPOutputStream(out2);
        gzout.write(bytes);
        gzout.finish();
        byte[] bytes2 = out2.toByteArray();
        System.out.println(bytes2.length);
        QueueEvent ev = new QueueEvent("mugatest", "table1", bytes2);
        thread.uploadEvent(ev);

//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        //FileOutputStream out = new FileOutputStream("msgpack.gz");
//        GZIPOutputStream gzout = new GZIPOutputStream(out);
//        Packer packer = msgpack.createPacker(gzout);
//      Map<String, Object> data = new HashMap<String, Object>();
//      data.put("k1", "v1");
//      data.put("time", System.currentTimeMillis() / 1000);
//      packer.write(data);
//        //gzout.finish();
//        gzout.flush();
//        //out.flush();
//
//        byte[] bytes = out.toByteArray();
//        gzout.close();
//        QueueEvent ev = new QueueEvent("mugatest", "table1", bytes);
//        thread.uploadEvent(ev);
    }
}
