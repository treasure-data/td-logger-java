package com.treasure_data.model;

public class DatabaseCollection extends ModelCollection<Database> {

    public DatabaseCollection(Client client) {
        super(client);
    }

    @Override
    boolean create(String name) throws ClientException {
        if (models.containsKey(name)) {
            models.get(name).create();
        } else {
            Database database = new Database(client, name, null);
            database.create();
            models.put(name, database);
        }
        return true;
    }

    @Override
    boolean delete(String name) throws ClientException {
        if (models.containsKey(name)) {
            Database database = models.remove(name);
            database.delete();
            return true;
        } else {
            throw new NotFoundException("");
        }
    }

    @Override
    boolean exist(String name) throws ClientException {
        // TODO Auto-generated method stub
        return false;
    }

}
