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

    @Ignore @Test
    public void testNormal01() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(Config.TD_LOGGER_AGENTMODE, "false");
        TreasureDataLogger logger = TreasureDataLogger.getLogger("mugatest");

        int count = 0;
        for (int i = 0; i < 1000 * 1000 * 1000; ++i) {
            Map<String, Object> data = new HashMap<String, Object>();
            for (int j = 0; j < 128; ++j) {
                data.put("kk:" + ":" + j, "vv:" + ":" + j);
                if (count == 1000) {
                    Thread.sleep(1);
                    count = 0;
                }
                count++;
            }
            logger.log("table1", data);
        }

        TreasureDataLogger.close();
    }
}
