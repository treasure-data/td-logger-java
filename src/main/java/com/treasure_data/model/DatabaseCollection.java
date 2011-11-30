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
