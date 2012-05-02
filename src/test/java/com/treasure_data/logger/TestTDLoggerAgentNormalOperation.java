package com.treasure_data.logger;

import static org.junit.Assert.assertEquals;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.fluentd.logger.sender.Event;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.msgpack.MessagePack;
import org.msgpack.unpacker.Unpacker;

public class TestTDLoggerAgentNormalOperation {

    private static List<Event> no01 = new ArrayList<Event>();

    @Before
    public void setUp() {
        no01.clear();
        Properties props = System.getProperties();
        props.setProperty(Config.TD_LOGGER_AGENT_HOST, "localhost");
        props.setProperty(Config.TD_LOGGER_AGENT_PORT, "25225");
        props.setProperty(Config.TD_LOGGER_AGENTMODE, "true");
    }

    @Test @Ignore
    public void testNormal01() throws Exception {
        Properties props = System.getProperties();
        int port = Integer.parseInt(props.getProperty(
                Config.TD_LOGGER_AGENT_PORT, Config.TD_LOGGER_AGENT_PORT_DEFAULT));

        // start mock server
        MockFluentd server = new MockFluentd(port, new MockFluentd.MockProcess() {
            public void process(MessagePack msgpack, Socket socket) throws IOException {
                BufferedInputStream in = new BufferedInputStream(socket.getInputStream());
                Unpacker unpacker = msgpack.createUnpacker(in);
                no01.add(unpacker.read(Event.class));
                no01.add(unpacker.read(Event.class));
                socket.close();
            }
        });
        server.start();

        Map<String, Object> data;

        // create logger objects
        TreasureDataLogger logger1 = TreasureDataLogger.getLogger("tag1");

        data = new HashMap<String, Object>();
        data.put("t1k1", "t1v1");
        data.put("t1k2", "t1v2");
        logger1.log("label1", data);

        data = new HashMap<String, Object>();
        data.put("t1k1", "t1v1");
        data.put("t1k2", "t1v2");
        logger1.log("label2", data);

        // close mock server sockets
        server.close();

        // sleep a little bit
        Thread.sleep(100);

        // check data
        assertEquals(2, no01.size());
        {
            Event e = no01.get(0);
            assertEquals("tag1.label1", e.tag);
            assertEquals("t1v1", e.data.get("t1k1"));
            assertEquals("t1v2", e.data.get("t1k2"));
        }
        {
            Event e = no01.get(1);
            assertEquals("tag1.label2", e.tag);
            assertEquals("t1v1", e.data.get("t1k1"));
            assertEquals("t1v2", e.data.get("t1k2"));
        }
    }

    /**
    @Test
    public void testSimpleBench() throws Exception {
        // create logger object
        TreasureDataLogger logger = TreasureDataLogger.getLogger("tag");
        long time = System.currentTimeMillis();
        for (int i = 0; i < 10000; ++i) {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("k" + i, "v" + i);
            logger.log("label", data);
        }
        time = System.currentTimeMillis() - time;
        TreasureDataLogger.close();
        System.out.println(time);
    }
     */
}
