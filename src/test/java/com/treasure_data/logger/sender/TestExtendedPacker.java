package com.treasure_data.logger.sender;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

public class TestExtendedPacker {

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testNormal() throws Exception {
        MessagePack msgpack = new MessagePack();

        // expect
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Packer packer = msgpack.createPacker(out);
        for (int i = 0; i < 100; i++) {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("ki:" + i, "vi:" + i);
            data.put("ki:" + i, i);
            packer.write(data);
        }
        byte[] expected = out.toByteArray();

        // actual
        ExtendedPacker extpacker = new ExtendedPacker(msgpack);
        for (int i = 0; i < 100; i++) {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("ki:" + i, "vi:" + i);
            data.put("ki:" + i, i);
            extpacker.write(data);
        }
        out = new ByteArrayOutputStream();
        GZIPInputStream gzin = new GZIPInputStream(new ByteArrayInputStream(extpacker.getByteArray()));
        while (true) {
            int len = gzin.read();
            if (len < 0) {
                break;
            }
            out.write(len);
        }
        out.flush();
        out.close();
        byte[] actual = out.toByteArray();

        assertArrayEquals(expected, actual);
    }

    @Test
    public void testThrowIllegalStateException() throws Exception {
        MessagePack msgpack = new MessagePack();

        // OK
        {
            ExtendedPacker extpacker = new ExtendedPacker(msgpack);
            for (int i = 0; i < 511; i++) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("ki:" + i, "vi:" + i);
                data.put("ki:" + i, i);
                extpacker.write(data);
            }
        }

        // NG
        {
            ExtendedPacker extpacker = new ExtendedPacker(msgpack);
            for (int i = 0; i < 511; i++) {
                Map<String, Object> data = new HashMap<String, Object>();
                data.put("ki:" + i, "vi:" + i);
                data.put("ki:" + i, i);
                extpacker.write(data);
            }
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("ki:" + 511, "vi:" + 511);
            data.put("ki:" + 511, 511);
            try {
                extpacker.write(data);
                fail();
            } catch (Throwable t) {
                assertTrue(t instanceof IllegalStateException);
            }
        }
    }
}
