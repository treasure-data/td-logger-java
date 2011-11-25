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

import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.treasure_data.logger.Config;
import com.treasure_data.model.APIException;
import com.treasure_data.model.AbstractClient;
import com.treasure_data.model.Table;

public class HttpClient extends AbstractClient {

    private static Logger LOG = LoggerFactory.getLogger(HttpClient.class);

    private static final SimpleDateFormat RFC2822FORMAT =
        new SimpleDateFormat( "E, dd MMM yyyy HH:mm:ss Z", Locale.ENGLISH );

    public HttpClient(final String apiKey) {
        super(apiKey);
    }

    public List<String> getDatabaseNames() throws APIException {
        HttpURLConnection conn = null;
        String jsonData;
        try {
            conn = doGetRequest("/v3/database/list", null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("List databases failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new APIException(msg);
            }
            jsonData = getResponseBody(conn); // TODO #MN format check
        } catch (IOException e) {
            throw new APIException(e);
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
            String dbName = dbNameMap.get("name");
            if (!dbNames.contains(dbName)) {
                dbNames.add(dbName);
            }
        }
        return dbNames;
    }

    public boolean deleteDatabase(String name) throws APIException {
        HttpURLConnection conn = null;
        try {
            String path = String.format("/v3/database/delete/%s", new Object[] { name });
            conn = doPostRequest(path, null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Delete database failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new APIException(msg);
            }
        } catch (IOException e) {
            throw new APIException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }
        return true;
    }

    public boolean createDatabase(String name) throws APIException {
        HttpURLConnection conn = null;
        try {
            String path = String.format("/v3/database/create/%s", new Object[] { name });
            conn = doPostRequest(path, null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("Create database failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new APIException(msg);
            }
        } catch (IOException e) {
            throw new APIException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }
        return true;
    }

    public Map<String, Table> getTables(String name) throws APIException {
        HttpURLConnection conn = null;
        String jsonData;
        try {
            String path = String.format("/v3/table/list/%s", new Object[] { name });
            conn = doGetRequest(path, null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) {
                String msg = String.format("List tables failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new APIException(msg);
            }
            jsonData = getResponseBody(conn); // TODO #MN format check
        } catch (IOException e) {
            throw new APIException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }

        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        @SuppressWarnings({ "rawtypes", "unchecked" })
        Iterator<Map> tableIter = ((List) map.get("tables")).iterator();
        Map<String, Table> tables = new HashMap<String, Table>();
        while (tableIter.hasNext()) {
            @SuppressWarnings("rawtypes")
            Map tableMap = tableIter.next();
            String tableName = (String) tableMap.get("name");
            String typeName = (String) tableMap.get("type");
            String schema = (String) tableMap.get("schema");
            long count = (Long) tableMap.get("count");
            Table table = new Table(tableName, Table.toType(typeName), schema, count);
            tables.put(tableName, table);
        }
        return tables;
    }

    public boolean createLogTable(String databaseName, String name) throws APIException {
        return createTable(databaseName, name, Table.Type.LOG);
    }

    public boolean createItemTable(String databaseName, String name) throws APIException {
        return createTable(databaseName, name, Table.Type.ITEM);
    }

    private boolean createTable(String databaseName, String name, Table.Type type)
            throws APIException {
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
                throw new APIException(msg);
            }
        } catch (IOException e) {
            throw new APIException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }
        return true;
    }

    public boolean updateSchema() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public Table.Type deleteTable(String databaseName, String name) throws APIException {
        HttpURLConnection conn = null;
        String jsonData;
        try {
            String path = String.format("/v3/table/delete/%s/%s",
                    new Object[] { databaseName, name });
            conn = doPostRequest(path, null, null);
            int code = getResponseCode(conn);
            if (code != HttpURLConnection.HTTP_OK) { // not 200
                String msg = String.format("Drop table failed (%s (%d): %s)",
                        new Object[] { getResponseMessage(conn), code, getResponseBody(conn) });
                LOG.error(msg);
                throw new APIException(msg);
            }
            jsonData = getResponseBody(conn); // TODO #MN check format
        } catch (IOException e) {
            throw new APIException(e);
        } finally {
            if (conn != null) {
                disconnect(conn);
            }
        }

        @SuppressWarnings("rawtypes")
        Map map = (Map) JSONValue.parse(jsonData);
        return Table.toType((String) map.get("type"));
    }

    public boolean tail() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void getJobs() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void showJob() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void getJobResult() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void getJobResultFormat() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void killJob() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void doHiveQuery() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void createSchedule(String scheduleName) throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void deleteSchedule(String scheduleName) throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public List getSchedules() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void history() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public void importData() throws APIException { // TODO #MN
        throw new UnsupportedOperationException("Not implement yet.");
    }

    public String authenticate(String user, String password) // TODO #MN
            throws APIException {
        throw new UnsupportedOperationException("Not implement yet.");
    }
    public String getServerStatus() throws APIException {
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
            throw new APIException(e);
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
