package com.treasure_data.logger.sender;

import static org.junit.Assert.*;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.junit.Ignore;
import org.junit.Test;

import com.treasure_data.logger.Config;

public class TestHttpSender {

    public void internalSetUp() throws Exception {
        Properties props = System.getProperties();
        props.load(this.getClass().getClassLoader().getResourceAsStream("treasure-data.properties"));
    }

    @Test @Ignore
    public void testEmit() throws Exception {
        internalSetUp();
        Properties props = System.getProperties();
        String host = props.getProperty(
                Config.TD_LOGGER_API_SERVER_HOST, Config.TD_LOGGER_API_SERVER_HOST_DEFAULT);
        int port = Integer.parseInt(props.getProperty(
                Config.TD_LOGGER_API_SERVER_PORT, Config.TD_LOGGER_API_SERVER_PORT_DEFAULT));
        String apiKey = props.getProperty(Config.TD_LOGGER_API_KEY);
        HttpSender sender = new HttpSender(props, host, port, apiKey);
        Map<String, Object> data = new HashMap<String, Object>();
        data.put("k1", "v1");
        sender.emit("mugadb.loggertable", data);
        System.out.println("finished emit method");
        Thread.sleep(10 * 1000);
        sender.close();
        System.out.println("finished close method");
    }

    @Test
    public void testDstHostAndPort() throws IOException, InterruptedException {
        final ServerSocket server = new ServerSocket();
        final List<Boolean> result = Arrays.asList(false);
        final CountDownLatch cdl = new CountDownLatch(1);
        Executors.newSingleThreadExecutor().execute(new Runnable() {
            @Override
            public void run() {
                try {
                    server.bind(new InetSocketAddress(0));
                    Socket socket = server.accept();
                    socket.close();
                    result.set(0, true);
                    cdl.countDown();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        });
        TimeUnit.SECONDS.sleep(1);

        Properties props = System.getProperties();
        final HttpSender sender = new HttpSender(props, "localhost", server.getLocalPort(), "dummy");
        sender.startBackgroundProcess();
        HashMap<String, Object> records = new HashMap<String, Object>();
        records.put("keykey", "valval");
        sender.emit("foo.bar", records);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                sender.flush();
            }
        });
        TimeUnit.SECONDS.sleep(1);
        executor.shutdownNow();

        assertTrue(result.get(0));

        cdl.await(6, TimeUnit.SECONDS);
        server.close();
    }
}
