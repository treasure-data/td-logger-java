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

import java.util.List;


public class Database extends Model {

    private String name;

    private List<Table> tables;

    public Database(Client client, String name, List<Table> tables) {
        super(client);
        this.name = name;
        this.tables = tables;
    }

    public String getName() {
        return name;
    }

    public void delete() {
        getClient().deleteDatabase(name);
    }

    public List<Table> getTables() {
        if (tables == null) {
            updateTables();
        }
        return tables;
    }

    public void createLogTable(String tableName) {
        getClient().createLogTable(name, tableName);
    }

    public void createItemTable(String tableName) {
        getClient().createItemTable(name, tableName);
    }

    public Table getTable(String tableName) throws NotFoundException {
        return getClient().getTable(name, tableName);
    }

    private void updateTables() {
        tables = getClient().getTables(name);
    }
}
