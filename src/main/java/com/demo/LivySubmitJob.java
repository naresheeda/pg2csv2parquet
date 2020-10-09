package com.demo;

import com.caseware.txn2csv2parquet.ExportPgToParquet;
import org.apache.livy.LivyClient;
import org.apache.livy.LivyClientBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.concurrent.ExecutionException;

public class LivySubmitJob {
    private static LivyClient client;

    public static void main(String[] args) {
        try {
            long startTime = System.nanoTime();
            client = new LivyClientBuilder()
                    .setURI(new URI("http://ec2-34-200-216-56.compute-1.amazonaws.com:8998/"))
                    .setConf("livy.client.http.connection.timeout", "300s")
                    .setConf("connection.socket.timeout", "20m")
                    .build();


            client.addJar(new URI("s3a://export-tp-to-parquet/livy.jar"));
            client.addJar(new URI("s3a://export-tp-to-parquet/livyclient.jar"));
            client.addJar(new URI("s3a://export-tp-to-parquet/postgresql-42.2.16.jar"));
            client.addJar(new URI("s3a://export-tp-to-parquet/txn-to-csv-to-parquet-using-livy.jar"));
            String response = client.submit(new ExportPgToParquet("A8A127FB-39F8-4BFB-A2C9-BC7E331DA796", "7M0z7UwcSkim5b6uWkaY9A")).get();
            long endTime = System.nanoTime();
            long duration = (endTime - startTime);

            System.out.println("***Total process time****" + duration + "\n");
            System.out.println("***Response:*** " + response + "\n");
            client.stop(true);


        } catch (URISyntaxException | IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            client.stop(true);
        }
    }
}