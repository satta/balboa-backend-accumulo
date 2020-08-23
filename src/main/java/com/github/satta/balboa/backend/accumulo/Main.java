package com.github.satta.balboa.backend.accumulo;

import com.github.satta.balboa.backend.BackendWorker;
import com.github.satta.balboa.backend.ProtocolException;
import org.apache.accumulo.core.client.*;
import org.apache.commons.cli.*;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;


public class Main {

    public static void main(String[] args) throws IOException, ProtocolException {
        BasicConfigurator.configure();
        Options options = new Options();
        options.addOption("u", false, "display current time");
        CommandLineParser parser = new DefaultParser();
        try {
            CommandLine cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
        }

        AccumuloClient client = Accumulo.newClient()
               .to("uno", "uno")
               .as("satta", "satta").build();
        ServerSocket server = new ServerSocket(4242);
        do {
            Socket socket = server.accept();
            try {
                new Thread(new BackendWorker(socket, new AccumuloProcessor(client))).start();
            } catch (AccumuloSecurityException e) {
                e.printStackTrace();
            } catch (AccumuloException e) {
                e.printStackTrace();
            } catch (TableNotFoundException e) {
                e.printStackTrace();
            }
        } while (true);
    }
}