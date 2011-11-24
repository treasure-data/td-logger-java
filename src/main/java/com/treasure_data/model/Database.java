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
import java.util.Map;


public class Database extends Model {

    private String name;

    private Map<String, Table> tables;

    public Database(Client client, String name, Map<String, Table> tables) {
        super(client);
        this.name = name;
        this.tables = tables;
    }

    public String getName() {
        return name;
    }

    public void delete() throws IOException, APIException {
        getClient().deleteDatabase(name);
    }

    public Map<String, Table> getTables() throws IOException, APIException {
        if (tables == null) {
            updateTables();
        }
        return tables;
    }

    public void createLogTable(String tableName) throws IOException, APIException {
        getClient().createLogTable(name, tableName);
    }

    public void createItemTable(String tableName) throws IOException, APIException {
        getClient().createItemTable(name, tableName);
    }

//    public Table getTable(String tableName) throws IOException, APIException {
//        return getClient().getTable(name, tableName);
//    }

    private void updateTables() throws IOException, APIException {
        tables = getClient().getTables(name);
    }
}
