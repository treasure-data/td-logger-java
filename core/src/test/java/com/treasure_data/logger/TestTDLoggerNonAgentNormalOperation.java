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

    @Test @Ignore
    public void testNormal01() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(Config.TD_LOGGER_AGENTMODE, "false");
        TreasureDataLogger logger = TreasureDataLogger.getLogger("mugadb");

        for (int i = 0; i < 10; ++i) {
            Map<String, Object> data = new HashMap<String, Object>();
            for (int j = 0; j < 16; ++j) {
                data.put("kk:" + ":" + j, "vv:" + ":" + j);
            }
            logger.log("mugatbl", data);
        }

        //logger.flush();
        //TreasureDataLogger.closeAll();
        
        Thread.sleep(100 * 1000);
    }

    @Test @Ignore
    public void testNormal02() throws Exception {
        Properties props = System.getProperties();
        props.setProperty(Config.TD_LOGGER_AGENTMODE, "false");
        TreasureDataLogger logger = TreasureDataLogger.getLogger("mugadb");

        for (int i = 0; i < 1000; ++i) {
            Map<String, Object> data = new HashMap<String, Object>();
            for (int j = 0; j < 16; ++j) {
                data.put("kk:" + ":" + j, "vv:" + ":" + j);
            }
            logger.log("newscore01", data);
        }

        //logger.flush();
        //TreasureDataLogger.closeAll();
        
        Thread.sleep(100 * 1000);
    }
}
