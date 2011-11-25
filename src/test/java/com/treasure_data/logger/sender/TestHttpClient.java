package com.treasure_data.logger.sender;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.logger.Config;
import com.treasure_data.model.Table;

public class TestHttpClient {

    @BeforeClass
    public static void setUp() throws IOException {
        Properties props = System.getProperties();
        props.load(TestHttpClient.class.getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Ignore @Test
    public void testGetDatabaseNames() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        List<String> databaseNames = client.getDatabaseNames();
        System.out.println(databaseNames);
    }

    @Ignore @Test
    public void testDeleteDatabase() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        boolean deleted = client.deleteDatabase("mugatest");
    }

    @Ignore @Test
    public void testCreateDatabase() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        boolean deleted = client.createDatabase("mugatest");
    }

    @Ignore @Test
    public void testGetTables() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        Map<String, Table> tables = client.getTables("mugatest");
        System.out.println(tables);
    }

    @Ignore @Test
    public void testCreateTable() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        client.createLogTable("mugatest", "table4");
        client.createItemTable("mugatest", "table5");
    }

    @Ignore @Test
    public void testDeleteTable() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        Table.Type type = client.deleteTable("mugatest", "table4");
        System.out.println(type);
    }

    @Test
    public void testGetServerStatus() throws Exception {
        HttpClient c = new HttpClient(null);
        assertEquals("ok", c.getServerStatus());
    }
}
