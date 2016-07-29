package eu.socialsensor.insert;

import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.util.TitanId;

import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import eu.socialsensor.main.GraphDatabaseType;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoader;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.BulkLoaderVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.bulkloading.IncrementalBulkLoader;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.File;
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
    private final Map<String, TitanVertex> vertexCache = new HashMap<String, TitanVertex>();

    public TitanMassiveInsertion(TitanGraph titanGraph, GraphDatabaseType type)
    {
        super(type, null);
        this.titanGraph = (StandardTitanGraph) titanGraph;
    }

    @Override
    public TitanVertex getOrCreate(String value)
    {
        final TitanVertex v;
        if (vertexCache.containsKey(value)) {
            v = vertexCache.get(value);
        } else {
            Integer intValue = Integer.valueOf(value) + 1;

            if (titanGraph.query().has("nodeId", Cmp.EQUAL, intValue).vertices().iterator().hasNext())
            {
                v = (TitanVertex) titanGraph.query().has("nodeId", Cmp.EQUAL, intValue).vertices().iterator().next();
            }
            else
            {
                final long titanVertexId = TitanId.toVertexId(intValue);
                v = titanGraph.addVertex();
                v.property("nodeId", intValue);
            }
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
