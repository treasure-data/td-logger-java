package com.treasure_data.logger.sender;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import com.google.appengine.api.taskqueue.Queue;
import com.google.appengine.api.taskqueue.QueueFactory;
import com.google.appengine.api.taskqueue.TaskOptions;
import com.google.appengine.api.taskqueue.TaskOptions.Builder;
import com.google.appengine.api.taskqueue.TaskOptions.Method;
import com.treasure_data.logger.sender.HttpSender;

public class GAEHttpSender extends HttpSender {
    private static final SimpleDateFormat RFC2822FORMAT =
            new SimpleDateFormat( "E, dd MMM yyyy HH:mm:ss Z", Locale.ENGLISH );

    private Queue gaequeue;

    public GAEHttpSender(String host, int port, String apiKey) {
        super(host, port, apiKey);
    }

    @Override
    public void startBackgroundProcess() {
        // TODO #MN queue name should be specified by users
        gaequeue = QueueFactory.getDefaultQueue();
    }

    @Override
    public int getQueueSize() {
        // TODO #MN should consider the return value more carefully?
        return 0;
    }

    @Override
    public void putQueue(String databaseName, String tableName, byte[] bytes) {
        // TODO #MN need refactoring
        //String urlpart = "api.treasure-data.com:80/v3/table/import/%s/%s/msgpack.gz";
        //String url = String.format(urlpart, databaseName, tableName);
        TaskOptions opts = Builder.withPayload(bytes, "application/octet-stream")
                .url("/mygae")
                .header("Content-Length", "" + bytes.length)
                .param("database", databaseName)
                .param("table", tableName)
                //.payload(bytes)
                .method(Method.PUT);
        gaequeue.add(opts);
    }

    private static String toRFC2822Format(Date from) {
        return RFC2822FORMAT.format(from);
    }

    @Override
    public void flush() {
        try {
            flush0(true);
        } catch (IOException e) {
            // ignore
        }
    }

    @Override
    public void close() {
        try {
            flush0(true);
        } catch (IOException e) {
            // ignore
        }
    }
}
