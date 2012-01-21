//
// Treasure Data Logger for Java.
//
// Copyright (C) 2011 - 2012 Muga Nishizawa
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

public class TableCollection extends ModelCollection<Table> {

    private String databaseName;

    public TableCollection(Client client, String databaseName) {
        super(client);
        this.databaseName = databaseName;
    }

    @Override
    public Table get(String name) throws ClientException {
        Table table;
        if (models.containsKey(name)) {
            table = models.get(name);
            table.create();
        } else {
            table = new Table(client, databaseName, name);
            table.create();
            models.put(name, table);
        }
        return table;
    }

    @Override
    public boolean delete(String name) throws ClientException {
        if (models.containsKey(name)) {
            Table table = models.remove(name);
            table.delete();
            return true;
        } else {
            throw new NotFoundException(""); // TODO #MN
        }
    }

    @Override
    public boolean exist(String name) throws ClientException {
        return models.containsKey(name);
    }

}
