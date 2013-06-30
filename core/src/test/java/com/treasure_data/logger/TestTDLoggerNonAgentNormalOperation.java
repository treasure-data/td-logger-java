package com.treasure_data.logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class TestTDLoggerNonAgentNormalOperation {

    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test
    public void testNormal01() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(Config.TD_LOGGER_AGENTMODE, "false");
        TreasureDataLogger logger = TreasureDataLogger.getLogger("mugadb");

        //long baseTime = System.currentTimeMillis() / 1000 / 3600 * 3600;
        long baseTime = 1363939200;
        for (int i = 0; i < 100; ++i) {
            Map<String, Object> data = new HashMap<String, Object>();
            data.put("time", baseTime + (i * 60));
            data.put("k:" + i, "v:" + i);
            logger.log("testtbl", data);
        }

        logger.close();
    }

}
