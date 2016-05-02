package eu.socialsensor.dataset;

import java.io.File;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

/**
 * 
 * @author Alexander Patrikalakis
 *
 */
public class DatasetFactory
{
    private static DatasetFactory theInstance = null;
    private final Map<Supplier<InputStream>, Dataset> datasetMap;

    private DatasetFactory()
    {
        datasetMap = new HashMap<>();
    }

    public static DatasetFactory getInstance()
    {
        if (theInstance == null)
        {
            theInstance = new DatasetFactory();
        }
        return theInstance;
    }

    public Dataset getDataset(Supplier<InputStream> datasetFile)
    {
        if (!datasetMap.containsKey(datasetFile))
        {
            datasetMap.put(datasetFile, new Dataset(datasetFile));
        }

        return datasetMap.get(datasetFile);
    }
}
