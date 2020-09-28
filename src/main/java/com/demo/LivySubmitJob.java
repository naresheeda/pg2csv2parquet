package com.demo;

import com.caseware.txn2csv2parquet.ExportPgToParquet;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;


public class LivySubmitJob {
    private static LivyClient client;
    private static final Logger log = LoggerFactory.getLogger(LivySubmitJob.class);

    public static void main(String[] args) {
        try {
            long startTime = System.nanoTime();
            client = new LivyClientBuilder()
                    .setURI(new URI("http://ec2-34-205-15-89.compute-1.amazonaws.com:8998/"))
                    .setConf("livy.client.http.connection.timeout", "300s")
                    .setConf("connection.socket.timeout", "20m")
                    .build();

            client.addJar(new URI("s3a://export-tp-to-parquet/livy.jar"));
            client.addJar(new URI("s3a://export-tp-to-parquet/livyclient.jar"));
            client.addJar(new URI("s3a://export-tp-to-parquet/txn-to-parquet.jar"));
            String response = client.submit(new ExportPgToParquet("A8A127FB-39F8-4BFB-A2C9-BC7E331DA796", "7M0z7UwcSkim5b6uWkaY9A")).get();
            long endTime = System.nanoTime();
            long duration = (endTime - startTime);

            System.out.println("***Total process time****" + duration + "\n");
            System.out.println("***Response:*** " + response + "\n");


        } catch (URISyntaxException | IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            client.stop(true);
        }
    }
}