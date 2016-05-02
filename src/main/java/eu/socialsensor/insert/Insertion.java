package eu.socialsensor.insert;

import java.io.File;
import java.io.InputStream;
import java.util.function.Supplier;

/**
 * Represents the insertion of data in each graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 */
public interface Insertion
{

    /**
     * Loads the data in each graph database
     * 
     * @param datasetDir
     */
    public void createGraph(Supplier<InputStream> dataset, int scenarioNumber);

}
