package eu.socialsensor.insert;

import java.io.File;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.util.TitanId;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;

import eu.socialsensor.main.GraphDatabaseType;

/**
 * Implementation of single Insertion in Titan graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class TitanSingleInsertion extends InsertionBase<TitanVertex>
{
    private final StandardTitanGraph titanGraph;

    public TitanSingleInsertion(TitanGraph titanGraph, GraphDatabaseType type, File resultsPath)
    {
        super(type, resultsPath);
        this.titanGraph = (StandardTitanGraph) titanGraph;
    }

    @Override
    public TitanVertex getOrCreate(String value)
    {
        Integer intValue = Integer.valueOf(value) + 1;
        final TitanVertex v;
        if (titanGraph.query().has("nodeId", Cmp.EQUAL, intValue).vertices().iterator().hasNext())
        {
            v = (TitanVertex) titanGraph.query().has("nodeId", Cmp.EQUAL, intValue).vertices().iterator().next();
        }
        else
        {
            final long titanVertexId = TitanId.toVertexId(intValue);
            v = titanGraph.addVertex();
            v.property("nodeId", intValue);
            titanGraph.tx().commit();
        }
        return v;
    }

    @Override
    public void relateNodes(TitanVertex src, TitanVertex dest)
    {
        try
        {
            src.addEdge("similar", dest);
            dest.addEdge("similar", src);
            titanGraph.tx().commit();
        }
        catch (Exception e)
        {
            titanGraph.tx().rollback(); //TODO(amcp) why can this happen? doesn't this indicate illegal state?
        }
    }
}
