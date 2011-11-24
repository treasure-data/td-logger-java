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

public interface Client {

    String getServerStatus() throws IOException;

    /** Database operations */
    List<String> getDatabaseNames() throws IOException, APIException;

    boolean deleteDatabase(String databaseName) throws IOException, APIException;

    boolean createDatabase(String databaseName) throws IOException, APIException;

    /** Table operations */

    Map<String, Table> getTables(String databaseName) throws IOException, APIException;

    boolean createLogTable(String databaseName, String tableName) throws IOException, APIException;

    boolean createItemTable(String databaseName, String tableName) throws IOException, APIException;

    Table.Type deleteTable(String databaseName, String tableName) throws IOException, APIException;

    boolean updateSchema();

    // List<?> tail();
}
