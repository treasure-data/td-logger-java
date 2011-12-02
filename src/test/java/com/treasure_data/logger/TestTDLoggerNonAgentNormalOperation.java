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

import com.treasure_data.model.TestHttpClient;

public class TestTDLoggerNonAgentNormalOperation {

    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(TestHttpClient.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Ignore @Test
    public void testNormalOperation01() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(Config.TD_LOGGER_AGENTMODE, "false");
        TreasureDataLogger logger = TreasureDataLogger.getLogger("mugatest");

        int count = 100;
        for (int i = 0; i < 1000 * 1000 * 1000; ++i) {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("kk:" + ":" + i, "vv:" + ":" + i);
            logger.log("table1", data);
            if (count == 100) {
                Thread.sleep(1);
                count = 0;
            }
            count++;
        }

        TreasureDataLogger.close();
    }
}
