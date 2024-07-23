package com.feeder;

import com.model.Vendor;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.Ignition;


import java.util.Map;
import java.util.TreeMap;

public class VendorFeeder implements AutoCloseable{

    private static final Log log = LogFactory.getLog(VendorFeeder.class);
    public static final String VENDOR_CACHE_NAME = "VENDOR_SOURCE_CACHE";
    private static IgniteCache<Integer, Vendor>  vendorCache;

    public static void main(String[] args) {
        System.setProperty(IgniteSystemProperties.IGNITE_QUIET, "false");

        Ignition.setClientMode(true);
        try (Ignite igClient = Ignition.start("client-config.xml")) {
            // Load  data into the grid.
            vendorCache = igClient.cache(VENDOR_CACHE_NAME);
            log.info("*** Vendor Feeder Started");
            TreeMap<Integer,Vendor> vendorList = createVendorList();
            log.info("*** " + vendorList.size() + " Vendors stored in the grid");
            vendorCache.putAll(vendorList);
            Map<Integer, Vendor> storedVendor = vendorCache.getAll(vendorList.keySet());
            for (Vendor ven : storedVendor.values()) {
                log.info("*** " + ven);
            }

            log.info("*** Vendor Feeder Completed");

        }catch (Exception ex) {
            log.error("Exception: " + ex.getMessage(), ex);
        }
    }


    private static TreeMap<Integer, Vendor> createVendorList(){
        TreeMap<Integer,Vendor> result = new TreeMap<Integer,Vendor>();
        Vendor vendor = new Vendor(1,"CREATE","TRUE", 12345L,"Supplier-credit-12345-TestUser");
        result.put(vendor.getId(),vendor);
        vendor = new Vendor(2,"UPDATE","TRUE", 12345L,"Supplier-credit-12345-TestUser");
        result.put(vendor.getId(),vendor);
        vendor = new Vendor(3,"CREATE","TRUE", 55555L,"Supplier-credit-55555-TestUser");
        result.put(vendor.getId(),vendor);
        vendor = new Vendor(4,"CREATE","TRUE", 12345L,"Supplier-credit-12345-TestUser");
        result.put(vendor.getId(),vendor);
        vendor = new Vendor(5,"CREATE","TRUE", 55555L,"Supplier-credit-55555-TestUser");
        result.put(vendor.getId(),vendor);
        vendor = new Vendor(6,"CREATE","TRUE", 55555L,"Supplier-credit-55555-TestUser");
        result.put(vendor.getId(),vendor);
        vendor = new Vendor(7,"UPDATE","TRUE", 55555L,"Supplier-credit-55555-TestUser");
        result.put(vendor.getId(),vendor);
        return result;
    }

    public void close() {
        vendorCache.close();
    }
}
