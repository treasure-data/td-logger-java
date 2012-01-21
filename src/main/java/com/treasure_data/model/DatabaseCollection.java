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

public class DatabaseCollection extends ModelCollection<Database> {

    public DatabaseCollection(Client client) {
        super(client);
    }

    @Override
    Database get(String name) throws ClientException {
        Database database;
        if (models.containsKey(name)) {
            database = models.get(name);
            database.create();
        } else {
            database = new Database(client, name, null);
            database.create();
            models.put(name, database);
        }
        return database;
    }

    @Override
    boolean delete(String name) throws ClientException {
        if (models.containsKey(name)) {
            Database database = models.remove(name);
            database.delete();
            return true;
        } else {
            throw new NotFoundException(""); // TODO #MN
        }
    }

    @Override
    boolean exist(String name) throws ClientException {
        return models.containsKey(name);
    }

}
