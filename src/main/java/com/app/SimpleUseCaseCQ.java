package com.app;


import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.query.ContinuousQuery;
import org.apache.ignite.cache.query.QueryCursor;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.lang.IgniteBiPredicate;

import javax.cache.Cache;
import javax.cache.configuration.Factory;
import javax.cache.event.*;

public class SimpleUseCaseCQ extends Thread implements AutoCloseable{

    private static final Log log = LogFactory.getLog(VendorCQ.class);

    private static IgniteCache<Long, BinaryObject>  simpleAggCache;
    private static IgniteCache<Integer, BinaryObject>  simpleCache;

    public void run() {
        try (Ignite igClient = Ignition.ignite()) {
            simpleCache = igClient.cache(VendorCQClient.VENDOR_CACHE_NAME); //Externalize this cache name to a
            simpleAggCache = igClient.cache(VendorCQClient.VENDOR_AGG_CACHE_NAME);

            log.info("*** VendorContinuousQuery register Continuous Query");
            log.info("**Vendor cache size: "+vendorCache.size());
            log.info("**Vendor Agg cache size: "+vendorAggCache.size());

            // Create new continuous query.
            ContinuousQuery<Integer, Vendor> query = new ContinuousQuery<>();


            // Initial Query
            query.setInitialQuery(new ScanQuery<>(
                    new IgniteBiPredicate<Integer, Vendor>() {
                        @Override
                        public boolean apply(Integer key, Vendor val) {
                            return (val.getEntityChangeAction().equalsIgnoreCase("CREATE") && val.getActive().equalsIgnoreCase("TRUE"));
                        }
                    }
            ));


            // This filter will be evaluated remotely on all nodes.
            // Entry that pass this filter will be sent to the caller.
            query.setRemoteFilterFactory(
                    new Factory<CacheEntryEventFilter<Integer, Vendor>>() {
                        @Override public CacheEntryEventFilter<Integer, Vendor> create() {
                            return new CacheEntryEventFilter<Integer, Vendor>() {
                                @Override
                                public boolean evaluate(CacheEntryEvent<? extends Integer, ? extends Vendor> cacheEntryEvent) throws CacheEntryListenerException {
                                    if (cacheEntryEvent.getEventType()== EventType.UPDATED || cacheEntryEvent.getEventType()== EventType.CREATED) {
                                        System.out.println("VendorContinuousQuery remote -> " + cacheEntryEvent.getValue());
                                        if (cacheEntryEvent.getValue().getEntityChangeAction().equalsIgnoreCase("CREATE") && cacheEntryEvent.getValue().getActive().equalsIgnoreCase("TRUE")) {
                                            return true;
                                        }
                                    }
                                    return false;
                                }
                            };
                        }
                    }
            );


            // Callback that is called locally when update notifications are received.
            query.setLocalListener(new CacheEntryUpdatedListener<Integer, Vendor>() {
                @Override
                public void onUpdated(Iterable<CacheEntryEvent<? extends Integer, ? extends Vendor>> evts) {
                    for (CacheEntryEvent<? extends Integer, ? extends Vendor> e : evts) {
                        log.info("*** Received event in mandatory local listener : "+e.getValue());
                        evaluateCounter(e.getValue(), igClient);
                    }
                }
            });

            // Execute query.
            log.info("*** Before vendorCache.query");
            try (QueryCursor<Cache.Entry<Integer, Vendor>> cur = vendorCache.query(query)) {
                log.info("*** After vendorCache.query: "+cur);
                for (Cache.Entry<Integer, Vendor> e : cur){
                    evaluateCounter(e.getValue(), igClient);
                }
                while (true) {
                    // Wait for a while, while callback is notified about remaining puts.
                    try {
                        Thread.sleep(2500);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }

    }

    private void evaluateCounter(Vendor vendor, Ignite ig) {
        Long acctId = vendor.getAccountID();
        log.info("****Got accId " + acctId + " from Vendor and size of vendorAggCache : "+vendorAggCache.size());

        if(vendorAggCache.containsKey(acctId)) {
            log.info("****Inside vendorAgg containsKey");
            VendorAgg vendorAgg = vendorAggCache.get(acctId);
            int currentCounter = vendorAgg.getCounter();
            vendorAgg.setCounter(currentCounter + 1);
            BinaryObject binvendorAgg = ig.binary().toBinary(vendorAgg);
            vendorAggCache.withKeepBinary().put(acctId, binvendorAgg);
            log.info("Current counter is " + currentCounter + " , new counter is " + vendorAgg.getCounter());
        }else {
            log.info("****Inside vendorAgg DOES NOT containsKey");
            VendorAgg newvendorAgg = new VendorAgg(acctId,vendor.getFullName(),1);
            vendorAggCache.put(acctId,newvendorAgg);
        }
    }


    public void close() {
        vendorAggCache.close();
    }
}
