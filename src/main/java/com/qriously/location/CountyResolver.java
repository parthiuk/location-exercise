package com.qriously.location;

import java.io.Closeable;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class CountyResolver implements Runnable, Closeable {

    private static Duration MAX_RUNTIME = Duration.ofSeconds(60);

    private CoordinateSupplier coordinateSupplier;

    private long start, end;
    private int attempted, resolved;
    private boolean didError = false;
    private Map<String, AtomicInteger> results = new ConcurrentHashMap<>();

    /**
     * CountyResolvers must be initialised with a coordinateSupplier.
     */
    CountyResolver(CoordinateSupplier coordinateSupplier) {
        this.coordinateSupplier = coordinateSupplier;
    }

    /**
     * Resolve the given coordinate to a US county.
     * Returns the LVL_2_ID code corresponding to the US county or null if unresolved.
     */
    public abstract String resolve(Coordinate coordinate);

    public abstract void process();
    
    /**
     * Run the resolver
     */
//    @Override
//    public void run() {
//        start = System.currentTimeMillis();
//
//        Coordinate coordinate;
//        try {
//            while (getRuntimeDuration() < MAX_RUNTIME.getSeconds() && (coordinate = coordinateSupplier.get()) != null) {
//                attempted++;
//                String county = resolve(coordinate);
//                if (county != null) {
//                    resolved++;
//                    results.computeIfAbsent(county, key -> new AtomicInteger(0)).incrementAndGet();
//                }
//                end = System.currentTimeMillis();
//            }
//        } catch (Exception ex) {
//            didError = true;
//        }
//        end = System.currentTimeMillis();
//
//    }

    
    @Override
    public void run() {
        start = System.currentTimeMillis();

        try {
            Coordinate coordinate;
            while (getRuntimeDuration() < MAX_RUNTIME.getSeconds() && (coordinate = coordinateSupplier.get()) != null) {
                attempted++;
                String county = resolve(coordinate);
                if (county != null) {
                    resolved++;
                    results.computeIfAbsent(county, key -> new AtomicInteger(0)).incrementAndGet();
                }
                end = System.currentTimeMillis();
            }
        } catch (Exception ex) {
            didError = true;
        }
        end = System.currentTimeMillis();

    }
    
    /**
     * Returns the results map containing LVL_2_ID, resolved count.
     */
    Map<String, AtomicInteger> getResults() {
        return results;
    }

    /**
     * Returns the count of resolved locations
     */
    int getResolvedCount() {
        return resolved;
    }

    /**
     * Returns the count of resolved locations
     */
    int getAttemptedCount() {
        return attempted;
    }

    /**
     * Returns the duration in seconds for the last run of the resolver.
     */
    long getRuntimeDuration() {
        return (end - start) / Duration.ofSeconds(1).toMillis();
    }

    /**
     * Returns the fraction of resolve attempts that were successful.
     */
    float getResolvedFraction() {
        return (float) resolved / attempted;
    }

    /**
     * Returns true if the resolver process failed otherwise false.
     */
    boolean resolverError() {
        return didError;
    }

}
