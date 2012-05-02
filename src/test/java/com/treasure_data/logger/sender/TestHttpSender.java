package com.treasure_data.logger.sender;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.logger.Config;

public class TestHttpSender {

    @Before
    public void setUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test
    public void testEmit() throws Exception {
        Properties props = System.getProperties();
        String host = props.getProperty(
                Config.TD_LOGGER_API_SERVER_HOST, Config.TD_LOGGER_API_SERVER_HOST_DEFAULT);
        int port = Integer.parseInt(props.getProperty(
                Config.TD_LOGGER_API_SERVER_PORT, Config.TD_LOGGER_API_SERVER_PORT_DEFAULT));
        String apiKey = props.getProperty(Config.TD_LOGGER_API_KEY);
        HttpSender sender = new HttpSender(host, port, apiKey);
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("k1", "v1");
        sender.emit("mugadb.loggertable", data);
        System.out.println("finished emit method");
        Thread.sleep(10 * 1000);
        sender.close();
        System.out.println("finished close method");
    }
}
