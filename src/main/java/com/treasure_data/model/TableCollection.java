package com.treasure_data.model;

public class TableCollection extends ModelCollection<Table> {

    private String databaseName;

    private String name;

    public TableCollection(Client client, String databaseName) {
        super(client);
        this.databaseName = databaseName;
    }

    @Override
    boolean create(String name) throws ClientException {
        if (models.containsKey(name)) {
            models.get(name).create();
        } else {
            Table table = new Table(client, databaseName, name);
            table.create();
            models.put(name, table);
        }
        return true;
    }

    @Override
    boolean delete(String name) throws ClientException {
        if (models.containsKey(name)) {
            Table table = models.remove(name);
            table.delete();
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
