package com.monetate.koupler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.Future;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP Listener
 */
public class TcpKoupler extends Koupler implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(TcpKoupler.class);
    private int port;

    public TcpKoupler(KinesisEventProducer producer, int port) {
        super(producer, 20);
        LOGGER.info("Firing up TCP listener on [{}]", port);
        this.port = port;
    }

    class TcpExceptionHandler implements ExceptionHandler {
        Socket socket = null;

        public TcpExceptionHandler(Socket socket) {
            this.socket = socket;
        }

        public void handleException(Exception e) {
            try {
                socket.close();
            } catch (IOException ioe) {
                LOGGER.error("Could not close socket", ioe);
            }
        }

    }

    @Override
    public void run() {
        ServerSocket listener = null;
        try {
            listener = new ServerSocket(port);
            while (true) {
                Socket socket = listener.accept();
                LOGGER.info("Accepting new socket [{}].", socket);
                BufferedReader data = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                Future<Integer> future = this.getThreadPool()
                        .submit(new KouplerThread(data, new TcpExceptionHandler(socket)));
            }
        } catch (Exception e) {
            if (listener != null) {
                try {
                    listener.close();
                } catch (IOException ioe) {
                    LOGGER.error("Could not close server socket.");
                }
            }
            LOGGER.error("Trouble with server socket/processing.", e);
        }
    }
}
