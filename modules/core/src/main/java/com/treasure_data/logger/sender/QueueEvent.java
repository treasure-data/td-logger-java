//
// Treasure Data Logger for Java.
//
// Copyright (C) 2011 - 2013 Muga Nishizawa
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
package com.treasure_data.logger.sender;

public class QueueEvent {
    public String databaseName;

    public String tableName;

    public byte[] data;

    public long rowCount;

    public QueueEvent() {
    }

    public QueueEvent(String databaseName, String tableName, byte[] data, long rowCount) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.data = data;
        this.rowCount = rowCount;
    }

    @Override
    public String toString() {
        return String.format("Event{database=%s,table=%s,data.size=%d, row.count=%d}",
                new Object[] { databaseName, tableName, data.length, rowCount});
    }
}
