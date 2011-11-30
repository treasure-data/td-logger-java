package com.treasure_data.model;

public class TableCollection extends ModelCollection<Table> {

    private String databaseName;

    private String name;

    public TableCollection(Client client, String databaseName) {
        super(client);
        this.databaseName = databaseName;
    }

    @Override
    public Table get(String name) throws ClientException {
        Table table;
        if (models.containsKey(name)) {
            table = models.get(name);
            table.create();
        } else {
            table = new Table(client, databaseName, name);
            table.create();
            models.put(name, table);
        }
        return table;
    }

    @Override
    public boolean delete(String name) throws ClientException {
        if (models.containsKey(name)) {
            Table table = models.remove(name);
            table.delete();
            return true;
        } else {
            throw new NotFoundException(""); // TODO #MN
        }
    }

    @Override
    public boolean exist(String name) throws ClientException {
        return models.containsKey(name);
    }

}
