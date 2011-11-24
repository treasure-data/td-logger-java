//
// Treasure Data Logger for Java.
//
// Copyright (C) 2011 Muga Nishizawa
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
//
package com.treasure_data.logger.sender;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.treasure_data.logger.Config;
import com.treasure_data.model.APIException;
import com.treasure_data.model.AbstractClient;
import com.treasure_data.model.CannotCreateException;
import com.treasure_data.model.Database;
import com.treasure_data.model.Table;
import com.treasure_data.model.NotFoundException;

public class HttpClient extends AbstractClient {

    private static Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private static final SimpleDateFormat RFC2822FORMAT =
        new SimpleDateFormat( "E, dd MMM yyyy HH:mm:ss Z", Locale.ENGLISH );

    public static HttpClient authenticate(String user, String password) {
        throw new UnsupportedOperationException(); // TODO #MN
    }

    public HttpClient(final String apiKey) {
        super(apiKey);
    }

    public List<String> getDatabaseNames() throws IOException, APIException {
        HttpURLConnection conn = doGetRequest("/v3/database/list", null, null);
        int code = getResponseCode(conn);
        if (code != HttpURLConnection.HTTP_OK) { // not 200
            String msg = String.format("List databases failed (%d: %s)",
                    new Object[] { code, getResponseMessage(conn) });
            LOG.error(msg);
            disconnect(conn);
            throw new APIException(msg);
        }

        String jsonData = getResponseBody(conn); // TODO #MN format check
        disconnect(conn);
        Map map = (Map) JSONValue.parse(jsonData);
        Iterator<Map> dbNameMapIter = ((List) map.get("databases")).iterator();
        List<String> dbNames = new ArrayList<String>();
        while (dbNameMapIter.hasNext()) {
            Map dbNameMap = dbNameMapIter.next();
            String dbName = (String) dbNameMap.get("name");
            if (!dbNames.contains(dbName)) {
                dbNames.add(dbName);
            }
        }
        return dbNames;
    }

    public boolean deleteDatabase(String databaseName) throws IOException, APIException {
        String path = String.format("/v3/database/delete/%s", new Object[] { databaseName });
        HttpURLConnection conn = doPostRequest(path, null, null);
        int code = getResponseCode(conn);
        if (code != HttpURLConnection.HTTP_OK) { // not 200
            String msg = String.format("Delete database failed (%d: %s)",
                    new Object[] { code, getResponseMessage(conn) });
            LOG.error(msg);
            disconnect(conn);
            throw new APIException(msg);
        }
        return true;
    }

    public boolean createDatabase(String databaseName) throws IOException, APIException {
        String path = String.format("/v3/database/create/%s", new Object[] { databaseName });
        HttpURLConnection conn = doPostRequest(path, null, null);
        int code = getResponseCode(conn);
        if (code != HttpURLConnection.HTTP_OK) { // not 200
            String msg = String.format("Create database failed (%d: %s)",
                    new Object[] { code, getResponseMessage(conn) });
            LOG.error(msg);
            disconnect(conn);
            throw new APIException(msg);
        }
        return true;
    }

    public Map<String, Table> getTables(String databaseName) throws IOException, APIException {
        String path = String.format("/v3/table/list/%s", new Object[] { databaseName });
        HttpURLConnection conn = doGetRequest(path, null, null);
        int code = getResponseCode(conn);
        if (code != HttpURLConnection.HTTP_OK) { // not 200
            String msg = String.format("List tables failed (%d: %s)",
                    new Object[] { code, getResponseMessage(conn) });
            LOG.error(msg);
            disconnect(conn);
            throw new APIException(msg);
        }

        String jsonData = getResponseBody(conn); // TODO #MN format check
        System.out.println(jsonData);
        Map map = (Map) JSONValue.parse(jsonData);
        Iterator<Map> tableIter = ((List) map.get("tables")).iterator();
        Map<String, Table> tables = new HashMap<String, Table>();
        while (tableIter.hasNext()) {
            Map tableMap = tableIter.next();
            String tableName = (String) tableMap.get("name");
            String typeName = (String) tableMap.get("type");
            long count = (Long) tableMap.get("count");
            String schema = (String) tableMap.get("schema");
            Table table = new Table(tableName, Table.toType(typeName), schema, count);
            tables.put(tableName, table);
        }
        return tables;
    }

    public boolean createLogTable(String databaseName, String name) throws IOException, APIException {
        return createTable(databaseName, name, Table.Type.LOG);
    }

    public boolean createItemTable(String databaseName, String name) throws IOException, APIException {
        return createTable(databaseName, name, Table.Type.ITEM);
    }

    private boolean createTable(String databaseName, String name, Table.Type type)
            throws IOException, APIException {
        String path = String.format("/v3/table/create/%s/%s/%s",
                new Object[] { databaseName, name, Table.toName(type) });
        HttpURLConnection conn = doPostRequest(path, null, null);
        int code = getResponseCode(conn);
        if (code != HttpURLConnection.HTTP_OK) { // not 200
            String msg = String.format("Create table failed (%d: %s)",
                    new Object[] { code, getResponseMessage(conn) });
            LOG.error(msg);
            disconnect(conn);
            throw new APIException(msg);
        }
        return true;
    }

    public boolean updateSchema() {
        throw new UnsupportedOperationException();
    }

    public Table.Type deleteTable(String databaseName, String name) throws IOException, APIException {
        String path = String.format("/v3/table/delete/%s/%s",
                new Object[] { databaseName, name });
        HttpURLConnection conn = doPostRequest(path, null, null);
        int code = getResponseCode(conn);
        if (code != HttpURLConnection.HTTP_OK) { // not 200
            String msg = String.format("Drop table failed (%d: %s)",
                    new Object[] { code, getResponseMessage(conn) });
            LOG.error(msg);
            disconnect(conn);
            throw new APIException(msg);
        }

        String jsonData = getResponseBody(conn); // TODO #MN check format
        Map map = (Map) JSONValue.parse(jsonData);
        return Table.toType((String) map.get("type"));
    }

    public String getServerStatus() throws IOException {
        HttpURLConnection conn = doGetRequest("/v3/system/server_status", null, null);
        int code = getResponseCode(conn);
        if (code != HttpURLConnection.HTTP_OK) { // not 200
            String msg = String.format("Server is down (%d: %s)",
                    new Object[] { code, getResponseMessage(conn) }); 
            LOG.error(msg);
            disconnect(conn);
            return msg;
        }
        String jsonData = getResponseBody(conn);
        disconnect(conn);
        Map map = (Map) JSONValue.parse(jsonData); // TODO #MN format check
        return (String) map.get("status");
    }

    private HttpURLConnection doGetRequest(String path,
            Map<String, String> header, Map<String, String> params) throws IOException {
        Properties props = System.getProperties();
        String host = props.getProperty(
                Config.TD_LOGGER_API_SERVER_HOST, Config.TD_LOGGER_API_SERVER_HOST_DEFAULT);
        int port = Integer.parseInt(props.getProperty(
                Config.TD_LOGGER_API_SERVER_PORT, Config.TD_LOGGER_API_SERVER_PORT_DEFAULT));

        StringBuilder sbuf = new StringBuilder();
        sbuf.append("http://").append(host).append(":").append(port).append(path);

        // parameters
        if (params != null && !params.isEmpty()) {
            sbuf.append("?");
            int paramSize = params.size();
            Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
            for (int i = 0; i < paramSize; ++i) {
                Map.Entry<String, String> e = iter.next();
                sbuf.append(e.getKey()).append("=").append(e.getValue());
                if (i + 1 != paramSize) {
                    sbuf.append("&");
                }
            }
        }

        // create connection object with url
        URL url = new URL(sbuf.toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();

        // header
        conn.setRequestMethod("GET");
        if (getAPIKey() != null) {
            conn.setRequestProperty("Authorization", "TD1 " + getAPIKey());
        }
        conn.setRequestProperty("Date", toRFC2822Format(new Date()));
        if (header != null && !header.isEmpty()) {
            for (Map.Entry<String, String> e : header.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }
        }

        // do connection to server
        conn.connect();
        return conn;
    }

    private HttpURLConnection doPostRequest(String path,
            Map<String, String> header, Map<String, String> params) throws IOException {
        Properties props = System.getProperties();
        String host = props.getProperty(
                Config.TD_LOGGER_API_SERVER_HOST, Config.TD_LOGGER_API_SERVER_HOST_DEFAULT);
        int port = Integer.parseInt(props.getProperty(
                Config.TD_LOGGER_API_SERVER_PORT, Config.TD_LOGGER_API_SERVER_PORT_DEFAULT));

        HttpURLConnection conn;
        StringBuilder sbuf = new StringBuilder();
        sbuf.append("http://").append(host).append(":").append(port).append(path);

        if (params != null && !params.isEmpty()) { // parameters
            sbuf.append("?");
            int paramSize = params.size();
            Iterator<Map.Entry<String, String>> iter = params.entrySet().iterator();
            for (int i = 0; i < paramSize; ++i) {
                Map.Entry<String, String> e = iter.next();
                sbuf.append(e.getKey()).append("=").append(e.getValue());
                if (i + 1 != paramSize) {
                    sbuf.append("&");
                }
            }
            URL url = new URL(sbuf.toString());
            conn = (HttpURLConnection) url.openConnection();
        } else {
            System.out.println(sbuf.toString());
            URL url = new URL(sbuf.toString());
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestProperty("Content-Length", "0");
        }

        // header
        conn.setRequestMethod("POST");
        if (getAPIKey() != null) {
            conn.setRequestProperty("Authorization", "TD1 " + getAPIKey());
        }
        conn.setRequestProperty("Date", toRFC2822Format(new Date()));
        if (header != null && !header.isEmpty()) {
            for (Map.Entry<String, String> e : header.entrySet()) {
                conn.setRequestProperty(e.getKey(), e.getValue());
            }
        }
        conn.connect();
        return conn;
    }

    private HttpURLConnection doPutRequest(String path, Map<String, String> params) throws IOException {
        throw new UnsupportedOperationException(); // TODO #MN must implement it soon
    }

    private int getResponseCode(HttpURLConnection conn) throws IOException {
        return conn.getResponseCode();
    }

    private String getResponseMessage(HttpURLConnection conn) throws IOException {
        return conn.getResponseMessage();
    }
    private String getResponseBody(HttpURLConnection conn) throws IOException {
        StringBuilder sbuf = new StringBuilder();
        BufferedReader reader = new BufferedReader( 
                new InputStreamReader(conn.getInputStream()));
        while (true){
            String line = reader.readLine();
            if ( line == null ){
                break;
            }
            sbuf.append(line);
        }
        reader.close();
        return sbuf.toString();
    }

    private void disconnect(HttpURLConnection conn) { // TODO #MN connections are disconnected every time?
        conn.disconnect();
    }

    private static String toRFC2822Format(Date from) {
        return RFC2822FORMAT.format(from);
    }
}
