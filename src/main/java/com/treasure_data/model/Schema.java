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
import java.util.Map;

import org.json.simple.JSONValue;

class Schema {
    private static final long serialVersionUID = 1L;

    static enum Type {
        INT, LONG, STRING, FLOAT, DOUBLE,
    }

    static String toTypeName(Type type) {
        switch (type) {
        case INT:
            return "int";
        case LONG:
            return "long";
        case STRING:
            return "string";
        case FLOAT:
            return "float";
        case DOUBLE:
            return "double";
        default:
            throw new RuntimeException("fatal error: " + type);
        }
    }

    @SuppressWarnings("unchecked")
    static Schema toSchema(String jsonData) {
        Schema schema = new Schema();
        schema.schema = (List<Map<String, String>>) JSONValue.parse(jsonData);
        return schema;
    }

    static String toJsonData(Schema schema) {
        return JSONValue.toJSONString(schema.schema);
    }

    static Type toType(String typeName) {
        // TODO #MN must normalize the specified name
        if (typeName.equals("int")) {
            return Type.INT;
        } else if (typeName.equals("long")) {
            return Type.LONG;
        } else if (typeName.equals("string")) {
            return Type.STRING;
        } else if (typeName.equals("float")) {
            return Type.FLOAT;
        } else if (typeName.equals("double")) {
            return Type.DOUBLE;
        } else {
            throw new RuntimeException("fatal error: " + typeName);
        }
    }

    private List<Map<String, String>> schema;

    Schema() {
    }

}
