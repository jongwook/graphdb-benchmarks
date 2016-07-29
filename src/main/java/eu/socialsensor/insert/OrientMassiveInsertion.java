package eu.socialsensor.insert;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.graph.batch.OGraphBatchInsert;
import com.orientechnologies.orient.graph.batch.OGraphBatchInsertBasic;
import com.tinkerpop.blueprints.TransactionalGraph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;

import eu.socialsensor.main.GraphDatabaseType;
import org.neo4j.helpers.collection.MapUtil;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation of massive Insertion in OrientDB graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
public class OrientMassiveInsertion extends InsertionBase<Vertex> implements Insertion
{
    protected final OrientGraph orientGraph;
    protected final OIndex<?> index;

    private final AtomicInteger counter = new AtomicInteger();
    private Set<Long> cache = new HashSet<>();

    public OrientMassiveInsertion(OrientGraph orientGraph)
    {
        super(GraphDatabaseType.ORIENT_DB, null);
        this.orientGraph = orientGraph;
        this.index = this.orientGraph.getRawGraph().getMetadata().getIndexManager().getIndex("V.nodeId");
    }

    @Override
    protected Vertex getOrCreate(String value)
    {
        final int key = Integer.parseInt(value);

        Vertex v;
        final OIdentifiable rec = (OIdentifiable) index.get(key);
        if (rec != null)
        {
            return orientGraph.getVertex(rec);
        }

        v = orientGraph.addVertex(key, "nodeId", key);

        return v;
    }

    private int edges = 0;

    @Override
    protected void relateNodes(Vertex src, Vertex dest)
    {
        orientGraph.addEdge(null, src, dest, "similar");
        orientGraph.addEdge(null, dest, src, "similar");
        edges++;
        if (edges % 1000 == 0) {
            orientGraph.commit();
        }
    }

    @Override
    protected void post() {
        orientGraph.commit();
    }
}
