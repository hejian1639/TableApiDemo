/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.api.example.source;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import lombok.val;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.InetSocketAddress;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * A source function that reads strings from a socket. The source will read bytes from the socket
 * stream and convert them to characters, each byte individually. When the delimiter character is
 * received, the function will output the current string, and begin a new string.
 *
 * <p>The function strips trailing <i>carriage return</i> characters (\r) when the delimiter is the
 * newline character (\n).
 *
 * <p>The function can be set to reconnect to the server socket in case that the stream is closed on
 * the server side.
 */
public class HttpStreamFunction extends RichSourceFunction<JSONObject> {

    private static final long serialVersionUID = 1L;

    HttpServer server;
    private final int port;

    private volatile boolean isRunning = true;

    private transient BlockingQueue<JSONObject> queue;

    public HttpStreamFunction(int port) {
        this.port = port;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        queue = new ArrayBlockingQueue<>(1024);

        server = HttpServer.create(new InetSocketAddress(port), 0);
        server.createContext("/post", new PostHandler());
        server.start();
    }


    class PostHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            InputStream is = exchange.getRequestBody();
            val object = JSON.parseObject(is2string(is));
//            object.put("rowtime", System.currentTimeMillis());
            queue.offer(object);


            exchange.sendResponseHeaders(200, 0);
            exchange.getResponseBody().close();
        }

        private String is2string(InputStream is) throws IOException {
            final int bufferSize = 1024;
            final char[] buffer = new char[bufferSize];
            final StringBuilder out = new StringBuilder();
            Reader in = new InputStreamReader(is, "UTF-8");
            for (; ; ) {
                int rsz = in.read(buffer, 0, buffer.length);
                if (rsz < 0)
                    break;
                out.append(buffer, 0, rsz);
            }
            return out.toString();
        }

    }


    @Override
    public void run(SourceContext<JSONObject> ctx) throws Exception {

        while (isRunning) {
            val msg = queue.take();
            if (isRunning) {
                ctx.collect(msg);

            }


        }
        server.stop(0);
    }

    @Override
    public void cancel() {
        isRunning = false;
        queue.offer(new JSONObject());

    }
}
