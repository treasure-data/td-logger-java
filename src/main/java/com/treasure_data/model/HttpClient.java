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
package com.treasure_data.model;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
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

import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.treasure_data.logger.Config;

public class HttpClient extends AbstractClient {

    private static Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private static final SimpleDateFormat RFC2822FORMAT =
        new SimpleDateFormat( "E, dd MMM yyyy HH:mm:ss Z", Locale.ENGLISH );

    public static HttpClient getClient(String user, String password) throws ClientException {
        HttpClient client = new HttpClient(null);
        String apiKey = client.authenticate(user, password);
        client.setAPIKey(apiKey);
        return client;
    }

    public static String getServerStatus() throws ClientException {
        HttpClient client = new HttpClient(null);
        return client.getServerStatus0();
    }

    public HttpClient(final String apiKey) {
        super(apiKey);
    }

    public List<Database> getDatabases() throws ClientException {
        List<String> dbNames = getDatabaseNames();
        List<Database> databases = new ArrayList<Database>(dbNames.size());
        for (String dbName : dbNames) {
            databases.add(new Database(this, dbName, null));
        }
        return databases;
    }

    public Database getDatabase(String name) throws ClientException {
        List<String> dbNames = getDatabaseNames();
        for (String dbName : dbNames) {
            if (dbName.equals(name)) {
                return new Database(this, dbName, null);
            }
        }
        throw new NotFoundException(
                String.format("Database %s does not exist", new Object[] { name }));
    }

    private List<String> getDatabaseNames() throws ClientException {
        HttpURLConnection conn = null;
        String jsonData;
        try {
            conn = doGetRequest("/v3/database/list", null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("List databases failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new ClientException(msg);
            }
            jsonData = getResponseBody(conn); // TODO #MN format check
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }

        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        @SuppressWarnings("unchecked")
        Iterator<Map<String, String>> dbNameMapIter =
            ((List<Map<String, String>>) map.get("databases")).iterator();
        List<String> dbNames = new ArrayList<String>();
        while (dbNameMapIter.hasNext()) {
            Map<String, String> dbNameMap = dbNameMapIter.next();
            String name = dbNameMap.get("name");
            dbNames.add(name);
        }
        return dbNames;
    }

    public boolean deleteDatabase(String name) throws ClientException {
        HttpURLConnection conn = null;
        try {
            String path = String.format("/v3/database/delete/%s", new Object[] { name });
            conn = doPostRequest(path, null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Delete database failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new ClientException(msg);
            }
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }
        return true;
    }

    public boolean createDatabase(String databaseName) throws ClientException {
        HttpURLConnection conn = null;
        try {
            String path = String.format("/v3/database/create/%s", new Object[] { databaseName });
            conn = doPostRequest(path, null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Create database failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new ClientException(msg);
            }
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }
        return true;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public List<Table> getTables(String databaseName) throws ClientException {
        Map map = getTableInfos(databaseName);

        Iterator<Map> tableIter = ((List) map.get("tables")).iterator();
        List<Table> tables = new ArrayList<Table>();
        while (tableIter.hasNext()) {
            Map tableMap = tableIter.next();
            String tableName = (String) tableMap.get("name");
            String typeName = (String) tableMap.get("type");
            long count = (Long) tableMap.get("count");
            List<Map<String, String>> schema = (List<Map<String, String>>)
                    JSONValue.parse((String) tableMap.get("schema"));
            Table table = new Table(this, databaseName, tableName, Table.toType(typeName), count, schema);
            tables.add(table);
        }
        return tables;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    public Table getTable(String databaseName, String tableName) throws ClientException {
        Map map = getTableInfos(databaseName);

        Iterator<Map> tableIter = ((List) map.get("tables")).iterator();
        while (tableIter.hasNext()) {
            Map tableMap = tableIter.next();
            String tblName = (String) tableMap.get("name");
            if (!tblName.equals(tableName)) {
                continue;
            }
            String typeName = (String) tableMap.get("type");
            long count = (Long) tableMap.get("count");
            List<Map<String, String>> schema = (List<Map<String, String>>)
                    JSONValue.parse((String) tableMap.get("schema"));
            return new Table(this, databaseName, tableName, Table.toType(typeName), count, schema);
        }
        throw new NotFoundException(String.format("Table '%s.%s' does not exist",
                new Object[] { databaseName, tableName }));
    }

    @SuppressWarnings("rawtypes")
    private Map getTableInfos(String databaseName) throws ClientException {
        HttpURLConnection conn = null;
        String jsonData;
        try {
            String path = String.format("/v3/table/list/%s", new Object[] { databaseName });
            conn = doGetRequest(path, null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("List tables failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new ClientException(msg);
            }
            jsonData = getResponseBody(conn); // TODO #MN format check
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }

        return (Map) JSONValue.parse(jsonData);
    }

    public boolean createLogTable(String databaseName, String name) throws ClientException {
        return createTable(databaseName, name, Table.Type.LOG);
    }

    public boolean createItemTable(String databaseName, String name) throws ClientException {
        return createTable(databaseName, name, Table.Type.ITEM);
    }

    private boolean createTable(String databaseName, String name, Table.Type type)
            throws ClientException {
        HttpURLConnection conn = null;
        try {
            String path = String.format("/v3/table/create/%s/%s/%s",
                    new Object[] { databaseName, name, Table.toName(type) });
            conn = doPostRequest(path, null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Create table failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new ClientException(msg);
            }
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }
        return true;
    }

    public boolean updateSchema(String databaseName, String tableName, List<List<String>> schema)
            throws ClientException {
        HttpURLConnection conn = null;
        try {
            String path = String.format("/v3/table/update-schema/%s/%s",
                    new Object[] { databaseName, tableName });
            Map<String, String> params = new HashMap<String, String>();
            params.put("schema", JSONValue.toJSONString(schema));
            conn = doPostRequest(path, null, params);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Create schema table failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new ClientException(msg);
            }
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }
        return true;
    }

    public Table.Type deleteTable(String databaseName, String name) throws ClientException {
        HttpURLConnection conn = null;
        String jsonData;
        try {
            String path = String.format("/v3/table/delete/%s/%s", new Object[] { databaseName, name });
            conn = doPostRequest(path, null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Drop table failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new ClientException(msg);
            }
            jsonData = getResponseBody(conn); // TODO #MN check format
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }

        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        return Table.toType((String) map.get("type"));
    }

    public boolean tail() throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void getJobs() throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void showJob() throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void getJobResult() throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void getJobResultFormat() throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void killJob() throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void doHiveQuery() throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void createSchedule(String scheduleName) throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void deleteSchedule(String scheduleName) throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    @SuppressWarnings("rawtypes")
    public List getSchedules() throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void history() throws ClientException {
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public double importData(String databaseName, String tableName, String format, byte[] bytes)
            throws ClientException {
        // TODO #MN under construction, a little bit..
        HttpURLConnection conn = null;
        String jsonData;
        try {
            String path = String.format("/v3/table/import/%s/%s/%s",
                    new Object[] { databaseName, tableName, format });
            conn = doPutRequest(path, bytes);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Import failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                if (code == HttpURLConnection.HTTP_BAD_REQUEST) {
                    throw new AuthenticationException(msg);
                } else {
                    throw new ClientException(msg);
                }
            }
            jsonData = getResponseBody(conn); // TODO #MN check format
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }

        System.out.println(jsonData);
        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        return (Double) map.get("elapsed_time"); // TODO #MN here is 'time'??
    }

    private String authenticate(String user, String password) throws ClientException {
        HttpURLConnection conn = null;
        String jsonData;
        try {
            String path = "/v3/user/authenticate";
            Map<String, String> params = new HashMap<String, String>();
            params.put("user", user);
            params.put("password", password);
            conn = doPostRequest(path, null, params);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Authentication failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                if (code == HttpURLConnection.HTTP_BAD_REQUEST) {
                    throw new AuthenticationException(msg);
                } else {
                    throw new ClientException(msg);
                }
            }
            jsonData = getResponseBody(conn); // TODO #MN check format
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }

        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        return (String) map.get("apikey");
    }

    private String getServerStatus0() throws ClientException {
        HttpURLConnection conn = null;
        String jsonData;
        try {
            conn = doGetRequest("/v3/system/server_status", null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Server is down (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                return msg;
            }
            jsonData = getResponseBody(conn);
        } catch (IOException e) {
            throw new ClientException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }

        @SuppressWarnings("rawtypes")
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

    private HttpURLConnection doPutRequest(String path, byte[] bytes) throws IOException {
        Properties props = System.getProperties();
        String host = props.getProperty(
                Config.TD_LOGGER_API_SERVER_HOST, Config.TD_LOGGER_API_SERVER_HOST_DEFAULT);
        int port = Integer.parseInt(props.getProperty(
                Config.TD_LOGGER_API_SERVER_PORT, Config.TD_LOGGER_API_SERVER_PORT_DEFAULT));

        StringBuilder sbuf = new StringBuilder();
        sbuf.append("http://").append(host).append(":").append(port).append(path);

        URL url = new URL(sbuf.toString());
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setReadTimeout(600 * 1000);
        conn.setRequestMethod("PUT");
        //conn.setRequestProperty("Content-Type", "application/octet-stream");
        conn.setRequestProperty("Content-Length", "" + bytes.length);
        if (getAPIKey() != null) {
            conn.setRequestProperty("Authorization", "TD1 " + getAPIKey());
        }
        conn.setRequestProperty("Date", toRFC2822Format(new Date()));
        conn.setDoOutput(true);
        conn.setUseCaches (false);
        //conn.connect();

        // body
        BufferedOutputStream out = new BufferedOutputStream(conn.getOutputStream());
        out.write(bytes);
        out.flush();
        //out.close();

        return conn;
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

    private void disconnect(HttpURLConnection conn) {
        conn.disconnect();
    }

    private static String toRFC2822Format(Date from) {
        return RFC2822FORMAT.format(from);
    }
}
