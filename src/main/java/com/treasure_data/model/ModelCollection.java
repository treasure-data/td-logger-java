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

import java.util.Map;

abstract class ModelCollection<T extends Model> extends Model {

    protected Map<String, T> models;

    public ModelCollection(Client client) {
        super(client);
    }

    @Override
    public boolean create() throws ClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean delete() throws ClientException {
        throw new UnsupportedOperationException();
    }

    abstract boolean create(String name) throws ClientException;

    abstract boolean delete(String name) throws ClientException;

    abstract boolean exist(String name) throws ClientException;

}
