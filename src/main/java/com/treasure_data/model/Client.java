package com.treasure_data.model;

import java.util.List;


public interface Client {

    /** Database operations */
    boolean createDatabase(String databaseName);

    Database getDatabase(String databaseName) throws NotFoundException;

    boolean deleteDatabase(String databaseName);

    /** Table operations */

    boolean createLogTable(String databaseName, String tableName);

    boolean createItemTable(String databaseName, String tableName);

    List<Table> getTables(String databaseName);

    Table getTable(String databaseName, String tableName) throws NotFoundException;

    boolean deleteTable(String databaseName, String tableName);

}
