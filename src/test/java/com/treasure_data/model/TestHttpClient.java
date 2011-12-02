package com.treasure_data.model;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.logger.Config;
import com.treasure_data.model.Database;
import com.treasure_data.model.HttpClient;
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
        List<Database> databases = client.getDatabases();
        System.out.println(databases);
    }

    @Ignore @Test
    public void testDeleteDatabase() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        boolean deleted = client.deleteDatabase("mugatest");
        System.out.println(deleted);
    }

    @Ignore @Test
    public void testCreateDatabase() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        boolean deleted = client.createDatabase("mugatest");
        System.out.println(deleted);
    }

    @Ignore @Test
    public void testGetTables() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        List<Table> tables = client.getTables("sf");
        System.out.println(tables);
    }

    @Ignore @Test
    public void testUpdateSchema() throws Exception {
        String apiKey = System.getProperties().getProperty(Config.TD_LOGGER_API_KEY);
        HttpClient client = new HttpClient(apiKey);
        List<List<String>> schema = new ArrayList<List<String>>();
        List<String> s1 = new ArrayList<String>();
        s1.add("arg1");
        s1.add("int");
        schema.add(s1);
        List<String> s2 = new ArrayList<String>();
        s2.add("arg2");
        s2.add("string");
        schema.add(s2);
        boolean updated = client.updateSchema("mugatest", "table1", schema);
        System.out.println(updated);
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
    public void testAuthenticate() throws Exception {
        Properties props = System.getProperties();
        HttpClient c = HttpClient.getClient(
                props.getProperty("td.logger.api.user"),
                props.getProperty("td.logger.api.password"));
        System.out.println(c.getAPIKey());
    }

    @Ignore @Test
    public void testGetServerStatus() throws Exception {
        HttpClient c = new HttpClient(null);
        assertEquals("ok", c.getServerStatus());
    }
}
