package com.qriously.location;

import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.geotools.data.FileDataStore;
import org.geotools.data.FileDataStoreFinder;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.filter.text.cql2.CQL;
import org.geotools.filter.text.cql2.CQLException;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.filter.Filter;

public class BasicCountyResolver extends CountyResolver implements Closeable {

    private static final String TEMP_DIR = "java.io.tmpdir";
    private static final String[] SHAPE_FILE_EXTENSIONS = new String[]{ ".dbf", ".prj", ".shp", ".shx" };
    private final static String USA_COUNTIES = "usa_counties";

    private final String shapeFilePath;
    
    private final String geometryPropertyName; 
    
    private final SimpleFeatureSource featureSource;

    /**
     * Initialise the BasicCountyResolver
     */
    public BasicCountyResolver(CoordinateSupplier coordinateSupplier) throws IOException {
        super(coordinateSupplier);
        shapeFilePath = extractShapeFiles(USA_COUNTIES);
        
        FileDataStore store = FileDataStoreFinder.getDataStore(new File(shapeFilePath + ".shp"));
        featureSource = store.getFeatureSource();
        geometryPropertyName = featureSource.getSchema().getGeometryDescriptor().getLocalName();
    }

    public String getFilterCriteria(Coordinate coordinate) {
    	StringBuilder builder = new StringBuilder();
    	builder.append("CONTAINS(");
    	builder.append(geometryPropertyName);
    	builder.append(", POINT(");
    	builder.append(coordinate.longitude);
    	builder.append(" ");
    	builder.append(coordinate.latitude);
    	builder.append("))");
    	return builder.toString();
    }
    
    @Override
    public String resolve(Coordinate coordinate) {
        String countyId = null;
        try {
//        	String str = "CONTAINS(" + geometryPropertyName + ", POINT(" + coordinate.longitude + " " + coordinate.latitude + "))";
            Filter filter = CQL.toFilter(getFilterCriteria(coordinate));

            SimpleFeatureCollection features = featureSource.getFeatures(filter);
            SimpleFeatureIterator featureIterator = features.features();
            while (featureIterator.hasNext()) {
                SimpleFeature sf = featureIterator.next();
                countyId = sf.getAttribute("LVL_2_ID").toString();

                if (countyId != null) {
                    break;
                }
            }

            featureIterator.close();

        } catch (IOException | CQLException ex) {
            ex.printStackTrace();
        }

        return countyId;
    }

    /**
     * Extract shapefiles from bundled resources to a temporary location
     *
     * Returns the filesystem path of shapefile without file-extension
     */
    private String extractShapeFiles(String shapeFileWithoutExtension) throws IOException {
        String shapeFilePathRoot = Paths.get(
                System.getProperty(TEMP_DIR),
                shapeFileWithoutExtension + "-" + System.currentTimeMillis()).toString();

        for (String extension : SHAPE_FILE_EXTENSIONS) {
            File file = new File(shapeFilePathRoot + extension);
            byte[] buffer = new byte[1024];
            try (InputStream in = getClass().getResourceAsStream("/" + shapeFileWithoutExtension + extension);
                 OutputStream out = new FileOutputStream(file)) {
                int read;
                while ((read = in.read(buffer)) > 0) {
                    out.write(buffer, 0, read);
                }
            }
        }

        return shapeFilePathRoot;
    }


    @Override
    public void close() {
        for (String extension : SHAPE_FILE_EXTENSIONS) {
            File file = new File(shapeFilePath + extension);
            if (file.exists()) {
                file.delete();
            }
        }
    }
    
    public void process() {
    	ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (int i = 0; i < 10; i++) {
            executor.execute(this);
        }
        
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        System.out.println("Finished all threads");
    }
}
