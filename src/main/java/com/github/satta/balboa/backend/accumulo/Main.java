package com.github.satta.balboa.backend.accumulo;

import com.github.satta.balboa.backend.BackendWorker;
import com.github.satta.balboa.backend.ProtocolException;
import org.apache.accumulo.core.client.*;
import org.apache.commons.cli.*;
import org.apache.log4j.BasicConfigurator;

import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

public class Main {

    public static void main(String[] args) throws ProtocolException {
        BasicConfigurator.configure();
        Options options = new Options();
        options.addOption("c", true, "config properties file");
        CommandLineParser parser = new DefaultParser();

        Properties props = new Properties();
        try {
            CommandLine cmd = parser.parse(options, args);
            FileReader reader = new FileReader(cmd.getOptionValue("c"));
            props.load(reader);
            reader.close();

            AccumuloClient client = Accumulo.newClient()
                    .from(cmd.getOptionValue("c")).build();
            ServerSocket server = new ServerSocket(Integer.parseInt(props.getProperty("balboa.port", "4242")));
            do {
                Socket socket = server.accept();
                try {
                    Thread t = new Thread(new BackendWorker(socket, new AccumuloProcessor(client)));
                    t.start();
                } catch (AccumuloSecurityException | AccumuloException | TableNotFoundException e) {
                    System.err.println(e.getMessage());
                    System.exit(1);
                }
            } while (true);
        } catch (IOException | ParseException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }
}