package com.qriously.location;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ResolverTest {

    @Test
    public void testBasicCountyResolver() throws IOException {
        try (CoordinateSupplier coordinateSupplier = new CoordinateSupplier();
             CountyResolver resolver = new BasicCountyResolver(coordinateSupplier)) {

            resolver.process();

            Assert.assertFalse("There were error when running the resolver", resolver.resolverError());

            System.out.println(String.format("Resolver managed to resolve %.2f of locations (%d / %d)",
                    resolver.getResolvedFraction(), resolver.getResolvedCount(), resolver.getAttemptedCount()));
            Assert.assertTrue("Resolver success rate was less that required threshold (90%)",
                    resolver.getResolvedFraction() > 0.9);

            float resolveRate = resolver.getResolvedCount() / resolver.getRuntimeDuration();
            System.out.println(String.format("Resolver managed to resolve locations at %.2f / second", resolveRate));
            Assert.assertTrue("Resolver rate was less than required threshold (1000 per second)", (resolveRate > 1000));
        }
    }

}
