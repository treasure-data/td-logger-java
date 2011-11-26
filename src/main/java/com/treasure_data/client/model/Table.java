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
package com.treasure_data.client.model;

import java.io.IOException;
import java.util.List;
import java.util.Map;


public class Table extends Model {

    public static enum Type {
        LOG, ITEM, UNDEFINED
    }

    public static Table.Type toType(String typeName) {
        if (typeName.equals("log")) {
            return Table.Type.LOG;
        } else if (typeName.equals("item")) {
            return Table.Type.ITEM;
        } else if (typeName.equals("?")) {
            return Table.Type.UNDEFINED;
        } else {
            return Table.Type.UNDEFINED; // TODO #MN
        }
    }

    public static String toName(Table.Type type) {
        switch (type) {
        case LOG:
            return "log";
        case ITEM:
            return "item";
        default:
            return "?";
        }
    }

    private String databaseName;

    private String name;

    private Type type;

    private List<Map<String, String>> schema;

    private long count;

    public Table(Client client, String databaseName, String name, Type type,
            long count, List<Map<String, String>> schema) {
        super(client);
        this.databaseName = databaseName;
        this.name = name;
        this.type = type;
        this.schema = schema;
        this.count = count;
    }

    public String getDatabaseName() throws NotFoundException {
        return databaseName;
    }

    public String ID() {
        return databaseName + "." + name;
    }

    public void delete() throws IOException, ClientException {
        getClient().deleteTable(databaseName, name);
    }

    public void tail() {
        throw new UnsupportedOperationException(); // TODO #MN
    }

    public void importData() {
        throw new UnsupportedOperationException(); // TODO
    }
}
