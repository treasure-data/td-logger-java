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

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class Database extends Model {

    private String name;

    private List<Table> tables;

    public Database(Client client, String name, List<Table> tables) {
        super(client);
        this.name = name;
        this.tables = tables;
    }

    public boolean create() throws ClientException {
        return client.createDatabase(name);
    }

    public boolean delete() throws ClientException {
        return client.deleteDatabase(name);
    }

    public boolean exists() throws ClientException {
        return client.deleteDatabase(name);
    }

    public List<Table> getTables() throws IOException, ClientException {
        if (tables == null) {
            updateTables();
        }
        return tables;
    }

//    public Table getTable(String tableName) throws IOException, APIException {
//        return getClient().getTable(name, tableName);
//    }

    private void updateTables() throws IOException, ClientException {
        tables = client.getTables(name);
    }

    public boolean createLogTable(String tableName) throws IOException, ClientException {
        return client.createLogTable(name, tableName);
    }

    public boolean createItemTable(String tableName) throws IOException, ClientException {
        return client.createItemTable(name, tableName);
    }

    @Override
    public String toString() {
        return String.format("Database{name=%s,tables=%s}",
                new Object[] { name, tables == null ? "{}" : tables.toString() });
    }
}
