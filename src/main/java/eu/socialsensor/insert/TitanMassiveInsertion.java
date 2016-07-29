package eu.socialsensor.insert;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.util.TitanId;

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import eu.socialsensor.main.GraphDatabaseType;

import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of massive Insertion in Titan graph database
 *
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 *
 */
public class TitanMassiveInsertion extends InsertionBase<TitanVertex>
{
    private final StandardTitanGraph titanGraph;
    private final Map<Integer, TitanVertex> vertexCache = new HashMap<>();

    public TitanMassiveInsertion(TitanGraph titanGraph, GraphDatabaseType type)
    {
        super(type, null);
        this.titanGraph = (StandardTitanGraph) titanGraph;
    }

    @Override
    public TitanVertex getOrCreate(String value)
    {
        final TitanVertex v;
        Integer intValue = Integer.valueOf(value) + 1;

        if (vertexCache.containsKey(intValue)) {
            v = vertexCache.get(intValue);
        } else {
            v = titanGraph.addVertex();
            v.property("nodeId", intValue);
            vertexCache.put(intValue, v);
        }

        return v;
    }

    @Override
    public void relateNodes(TitanVertex src, TitanVertex dest)
    {
        src.addEdge("similar", dest);
    }

    @Override
    protected void post() {
        titanGraph.tx().commit();
    }
}
