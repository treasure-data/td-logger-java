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

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractClient implements Client {

    private static Logger LOG = LoggerFactory.getLogger(AbstractClient.class);

    private String apiKey;

    private Pattern databasePat;

    private Pattern columnPat;

    protected AbstractClient(final String apiKey) {
        this.apiKey = apiKey;
        databasePat = Pattern.compile("^([a-z0-9_]+)$");
        columnPat = Pattern.compile("^([a-z0-9_]+)$");
    }

    public String getAPIKey() {
        return apiKey;
    }

    protected void setAPIKey(String apiKey) {
        this.apiKey = apiKey;
    }

    public boolean validateDatabaseName(String name) {
        if (name == null || name.equals("")) {
            LOG.info(String.format("Empty name is not allowed: %s",
                    new Object[] { name }));
            return false;
        }

        int len = name.length();
        if (len < 3 || 32 < len) {
            LOG.info(String.format("Name must be 3 to 32 characters, got %d characters.",
                    new Object[] { len }));
            return false;
        }

        if (!databasePat.matcher(name).matches()) {
            LOG.info("Name must consist only of alphabets, numbers, '_'.");
            return false;
        }

        return true;
    }

    public boolean validateTableName(String name) {
        return validateDatabaseName(name);
    }

    public boolean validateColumnName(String name) {
        if (name == null || name.equals("")) {
            LOG.info(String.format("Empty column name is not allowed: %s",
                    new Object[] { name }));
            return false;
        }

        int len = name.length();
        if (32 < len) {
            LOG.info(String.format("Column name must be to 32 characters, got %d characters.",
                    new Object[] { len }));
            return false;
        }

        if (!columnPat.matcher(name).matches()) {
            LOG.info("Column name must consist only of alphabets, numbers, '_'.");
            return false;
        }

        return true;
    }
}
