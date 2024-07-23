package com.app;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;


public class VendorCQClient {

    public static final String VENDOR_CACHE_NAME = "VENDOR_SOURCE_CACHE";
    public static final String VENDOR_AGG_CACHE_NAME = "VENDOR_AGGREGATE_CACHE";

    public static void main(String[] args) {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        Ignition.setClientMode(true);
        try (Ignite ignite = Ignition.start("client-config.xml")) {
            Thread vendorCQ = new VendorCQ();
            vendorCQ.start();

            while (true) {
                try {
                    Thread.sleep(60000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
