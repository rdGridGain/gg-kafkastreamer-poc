package com.app;

import org.apache.ignite.Ignition;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.configuration.ClientConfiguration;

public class Tester {

    private static final String cacheName = "VENDOR_SOURCE_CACHE";

    public static void main(String[] args) {
        try (IgniteClient igniteClient = Ignition.startClient(
                new ClientConfiguration().setAddresses("127.0.0.1:10800"))) {
            igniteClient.destroyCache(cacheName);
            System.out.println("Destroyed cache : "+cacheName);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
