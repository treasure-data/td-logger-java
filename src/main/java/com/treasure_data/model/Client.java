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

public interface Client {

    /** Database API */
    List<Database> getDatabases() throws ClientException;

    Database getDatabase(String databaseName) throws ClientException;

    boolean deleteDatabase(String databaseName) throws ClientException;

    boolean createDatabase(String databaseName) throws  ClientException;

    /** Table API */

    List<Table> getTables(String databaseName) throws ClientException;

    Table getTable(String databaseName, String tableName) throws ClientException;

    boolean createLogTable(String databaseName, String tableName) throws ClientException;

    boolean createItemTable(String databaseName, String tableName) throws ClientException;

    Table.Type deleteTable(String databaseName, String tableName) throws ClientException;

    boolean updateSchema(String databaseName, String tableName, List<List<String>> schema) throws ClientException;

    boolean tail() throws ClientException;

    /** Job API */

    void getJobs() throws ClientException;

    void showJob() throws ClientException;

    void getJobResult() throws ClientException;

    void getJobResultFormat() throws ClientException;

    void killJob() throws ClientException;

    void doHiveQuery() throws ClientException;

    /** Schedule API */

    void createSchedule(String scheduleName) throws ClientException;

    void deleteSchedule(String scheduleName) throws ClientException;

    @SuppressWarnings("rawtypes")
    List getSchedules() throws ClientException;

    void history() throws ClientException;

    /** Import API */

    void importData(String databaseName, String tableName, String fileName, byte[] bytes) throws ClientException;

    /** User API */

    String authenticate(String user, String password) throws ClientException;

    String getServerStatus() throws ClientException;
}
