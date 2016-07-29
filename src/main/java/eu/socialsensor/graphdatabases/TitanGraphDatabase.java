package eu.socialsensor.graphdatabases;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Cmp;
import com.thinkaurelius.titan.core.schema.TitanManagement;
import com.thinkaurelius.titan.core.util.TitanCleanup;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import eu.socialsensor.insert.Insertion;
import eu.socialsensor.insert.TitanMassiveInsertion;
import eu.socialsensor.insert.TitanSingleInsertion;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.net.ConnectException;
import java.net.Socket;
import java.util.*;
import java.util.function.Supplier;

/**
 * Titan graph database implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class TitanGraphDatabase extends GraphDatabaseBase<Iterator<TitanVertex>, Iterator<Edge>, TitanVertex, Edge>
{
    public static final String INSERTION_TIMES_OUTPUT_PATH = "data/titan.insertion.times";

    double totalWeight;

    private TitanGraph titanGraph;
    public final BenchmarkConfiguration config;

    public TitanGraphDatabase(GraphDatabaseType type, BenchmarkConfiguration config, File dbStorageDirectory)
    {
        super(type, dbStorageDirectory);
        this.config = config;
        if (!GraphDatabaseType.TITAN_FLAVORS.contains(type))
        {
            throw new IllegalArgumentException(String.format("The graph database %s is not a Titan database.",
                type == null ? "null" : type.name()));
        }
    }

    @Override
    public void open()
    {
        open(false /* batchLoading */);
    }

    private static final Configuration generateBaseTitanConfiguration(GraphDatabaseType type, File dbPath,
        boolean batchLoading, BenchmarkConfiguration bench)
    {
        if (!GraphDatabaseType.TITAN_FLAVORS.contains(type))
        {
            throw new IllegalArgumentException("must provide a Titan database type but got "
                + (type == null ? "null" : type.name()));
        }

        if (dbPath == null)
        {
            throw new IllegalArgumentException("the dbPath must not be null");
        }
        if (!dbPath.exists() || !dbPath.canWrite() || !dbPath.isDirectory())
        {
            throw new IllegalArgumentException("db path must exist as a directory and must be writeable");
        }

        final Configuration conf = new MapConfiguration(new HashMap<String, String>());
        final Configuration storage = conf.subset(GraphDatabaseConfiguration.STORAGE_NS.getName());
        final Configuration ids = conf.subset(GraphDatabaseConfiguration.IDS_NS.getName());
        final Configuration metrics = conf.subset(GraphDatabaseConfiguration.METRICS_NS.getName());

        conf.addProperty(GraphDatabaseConfiguration.ALLOW_SETTING_VERTEX_ID.getName(), "true");

        // storage NS config. FYI, storage.idauthority-wait-time is 300ms
        String zookeeper = ConfigFactory.load().getString("hbase.zookeeper.quorum").split(":")[0];
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_BACKEND.getName(), type.getBackend());
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_HOSTS.getName(), zookeeper);
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_DIRECTORY.getName(), dbPath.getAbsolutePath());
        storage.addProperty(GraphDatabaseConfiguration.STORAGE_BATCH.getName(), batchLoading);
        storage.addProperty(GraphDatabaseConfiguration.BUFFER_SIZE.getName(), bench.getTitanBufferSize());
        storage.addProperty(GraphDatabaseConfiguration.PAGE_SIZE.getName(), bench.getTitanPageSize());

        if (type.getBackend().equals("hbase")) {
            System.err.println("hbaseExecutor.isShutDown(): " + S2GraphDatabase$.MODULE$.hbaseExecutor().isShutdown());
            try {
                Socket socket = new Socket(zookeeper, 2181);
                socket.close();
            } catch (IOException e) {
                while (true) {
                    try {
                        Socket socket = new Socket("localhost", 16010);
                        socket.close();
                        break;
                    } catch (IOException e2) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e3) {
                            throw new RuntimeException(e);
                        }
                    }
                }
            }
        }

        // ids NS config
        ids.addProperty(GraphDatabaseConfiguration.IDS_BLOCK_SIZE.getName(), bench.getTitanIdsBlocksize());

        // Titan metrics - https://github.com/thinkaurelius/titan/wiki/Titan-Performance-and-Monitoring
        metrics.addProperty(GraphDatabaseConfiguration.BASIC_METRICS.getName(), "true");
        metrics.addProperty("prefix", type.getShortname());
        if(bench.publishGraphiteMetrics()) {
            final Configuration graphite = metrics.subset(BenchmarkConfiguration.GRAPHITE);
            graphite.addProperty("hostname", bench.getGraphiteHostname());
            graphite.addProperty(BenchmarkConfiguration.CSV_INTERVAL, bench.getCsvReportingInterval());
        }
        if(bench.publishCsvMetrics()) {
            final Configuration csv = metrics.subset(GraphDatabaseConfiguration.METRICS_CSV_NS.getName());
            csv.addProperty(GraphDatabaseConfiguration.METRICS_CSV_DIR.getName(), bench.getCsvDir().getAbsolutePath());
            csv.addProperty(BenchmarkConfiguration.CSV_INTERVAL, bench.getCsvReportingInterval());
        }
        
        return conf;
    }

    private static final TitanGraph buildTitanGraph(GraphDatabaseType type, File dbPath, BenchmarkConfiguration bench,
        boolean batchLoading)
    {
        final Configuration conf = generateBaseTitanConfiguration(type, dbPath, batchLoading, bench);
        final Configuration storage = conf.subset(GraphDatabaseConfiguration.STORAGE_NS.getName());

        if (GraphDatabaseType.TITAN_CASSANDRA == type)
        {
            storage.addProperty("hostname", "localhost");
            storage.addProperty("transactions", Boolean.toString(batchLoading));
        }
        else if (GraphDatabaseType.TITAN_CASSANDRA_EMBEDDED == type)
        {
            // TODO(amcp) - this line seems broken:
            // throws: Unknown configuration element in namespace
            // [root.storage]: cassandra-config-dir
            storage.addProperty("cassandra-config-dir", "configuration/cassandra.yaml");
            storage.addProperty("transactions", Boolean.toString(!batchLoading));
            storage.addProperty("batch-loading", Boolean.toString(batchLoading));
        }
        else if (GraphDatabaseType.TITAN_DYNAMODB == type)
        {
        }
        else if (GraphDatabaseType.TITAN_HBASE == type)
        {
            storage.setProperty("hbase.region-count", 5);
        }
        return TitanFactory.open(conf);
    }

    private void open(boolean batchLoading)
    {
        titanGraph = buildTitanGraph(type, dbStorageDirectory, config, batchLoading);
    }

    @Override
    public void createGraphForSingleLoad()
    {
        open();
        createSchema();
    }

    @Override
    public void createGraphForMassiveLoad()
    {
        open(true /* batchLoading */);
        createSchema();

        //batchGraph = new BatchGraph<TitanGraph>(titanGraph, VertexIDType.NUMBER, 100000 /* bufferSize */);
        //batchGraph.setVertexIdKey(NODE_ID);
        //batchGraph.setLoadingFromScratch(true /* fromScratch */);
    }

    @Override
    public void massiveModeLoading(Supplier<InputStream> dataPath)
    {
        Insertion titanMassiveInsertion = new TitanMassiveInsertion(this.titanGraph, type);
        titanMassiveInsertion.createGraph(dataPath, 0 /* scenarioNumber */);
    }

    @Override
    public void singleModeLoading(Supplier<InputStream> dataPath, File resultsPath, int scenarioNumber)
    {
        Insertion titanSingleInsertion = new TitanSingleInsertion(this.titanGraph, type, resultsPath);
        titanSingleInsertion.createGraph(dataPath, scenarioNumber);
    }

    @Override
    public void shutdown()
    {
        if (titanGraph == null)
        {
            return;
        }
        try
        {
            titanGraph.close();
        }
        catch (IOError e)
        {
            // TODO Fix issue in shutting down titan-cassandra-embedded
            System.err.println("Failed to shutdown titan graph: " + e.getMessage());
        }

        titanGraph = null;
    }

    @Override
    public void delete()
    {
        titanGraph = buildTitanGraph(type, dbStorageDirectory, config, false /* batchLoading */);
        try
        {
            titanGraph.close();
        }
        catch (IOError e)
        {
            // TODO Fix issue in shutting down titan-cassandra-embedded
            System.err.println("Failed to shutdown titan graph: " + e.getMessage());
        }
        TitanCleanup.clear(titanGraph);
        try
        {
            titanGraph.close();
        }
        catch (IOError e)
        {
            // TODO Fix issue in shutting down titan-cassandra-embedded
            System.err.println("Failed to shutdown titan graph: " + e.getMessage());
        }
        Utils.deleteRecursively(dbStorageDirectory);
    }

    @Override
    public void shutdownMassiveGraph()
    {
        try
        {
            titanGraph.close();
        }
        catch (IOError e)
        {
            // TODO Fix issue in shutting down titan-cassandra-embedded
            System.err.println("Failed to shutdown titan graph: " + e.getMessage());
        }
        titanGraph = null;
    }

    @Override
    public void shortestPath(final TitanVertex fromNode, Integer node)
    {
        throw new NotImplementedException("Titan.shortestPath");
    }

    @Override
    public int getNodeCount()
    {
        long nodeCount = Iterators.size(titanGraph.vertices());
        return (int) nodeCount;
    }

    @Override
    public Set<Integer> getNeighborsIds(int nodeId)
    {
        Set<Integer> neighbors = new HashSet<Integer>();
        TitanVertex vertex = (TitanVertex) titanGraph.query().has(NODE_ID, Cmp.EQUAL, nodeId).vertices().iterator().next();

        Iterator<Edge> iter = vertex.edges(Direction.OUT, SIMILAR);
        while (iter.hasNext())
        {
            Integer neighborId = iter.next().outVertex().<Integer>property(NODE_ID).value();
            neighbors.add(neighborId);
        }
        return neighbors;
    }

    @Override
    public double getNodeWeight(int nodeId)
    {
        TitanVertex vertex = (TitanVertex) titanGraph.query().has(NODE_ID, Cmp.EQUAL, nodeId).vertices().iterator().next();
        double weight = getNodeOutDegree(vertex);
        return weight;
    }

    public double getNodeInDegree(TitanVertex vertex)
    {
        return (double) Iterators.size(vertex.edges(Direction.IN, SIMILAR));
    }

    public double getNodeOutDegree(TitanVertex vertex)
    {
        return (double) Iterators.size(vertex.edges(Direction.OUT, SIMILAR));
    }

    @Override
    public void initCommunityProperty()
    {
        int communityCounter = 0;
        for (TitanVertex v : titanGraph.query().vertices())
        {
            v.property(NODE_COMMUNITY, communityCounter);
            v.property(COMMUNITY, communityCounter);
            communityCounter++;
        }
    }

    @Override
    public Set<Integer> getCommunitiesConnectedToNodeCommunities(int nodeCommunities)
    {
        Set<Integer> communities = new HashSet<Integer>();
        Iterable<TitanVertex> vertices = titanGraph.query().has(NODE_COMMUNITY, nodeCommunities).vertices();
        for (TitanVertex vertex : vertices)
        {
            Iterator<Edge> iter = vertex.edges(Direction.OUT, SIMILAR);
            while (iter.hasNext())
            {
                int community = iter.next().outVertex().<Integer>property(COMMUNITY).value();
                communities.add(community);
            }
        }
        return communities;
    }

    @Override
    public Set<Integer> getNodesFromCommunity(int community)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        Iterable<TitanVertex> iter = titanGraph.query().has(COMMUNITY, community).vertices();
        for (TitanVertex v : iter)
        {
            Integer nodeId = v.<Integer>property(NODE_ID).value();
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public Set<Integer> getNodesFromNodeCommunity(int nodeCommunity)
    {
        Set<Integer> nodes = new HashSet<Integer>();
        Iterable<TitanVertex> iter = titanGraph.query().has(NODE_COMMUNITY, nodeCommunity).vertices();
        for (TitanVertex v : iter)
        {
            Integer nodeId = v.<Integer>property(NODE_ID).value();
            nodes.add(nodeId);
        }
        return nodes;
    }

    @Override
    public double getEdgesInsideCommunity(int vertexCommunity, int communityVertices)
    {
        double edges = 0;
        Iterable<TitanVertex> vertices = titanGraph.query().has(NODE_COMMUNITY, vertexCommunity).vertices();
        Iterable<TitanVertex> comVertices = titanGraph.query().has(COMMUNITY, communityVertices).vertices();
        for (TitanVertex vertex : vertices)
        {
            Iterator neighbors = vertex.vertices(Direction.OUT, SIMILAR);
            while(neighbors.hasNext()) {
                Object v = neighbors.next();
                if (Iterables.contains(comVertices, v))
                {
                    edges++;
                }
            }
        }
        return edges;
    }

    @Override
    public double getCommunityWeight(int community)
    {
        double communityWeight = 0;
        Iterable<TitanVertex> iter = titanGraph.query().has(COMMUNITY, community).vertices();
        if (Iterables.size(iter) > 1)
        {
            for (TitanVertex vertex : iter)
            {
                communityWeight += getNodeOutDegree(vertex);
            }
        }
        return communityWeight;
    }

    @Override
    public double getNodeCommunityWeight(int nodeCommunity)
    {
        double nodeCommunityWeight = 0;
        Iterable<TitanVertex> iter = titanGraph.query().has(NODE_COMMUNITY, nodeCommunity).vertices();
        for (TitanVertex vertex : iter)
        {
            nodeCommunityWeight += getNodeOutDegree(vertex);
        }
        return nodeCommunityWeight;
    }

    @Override
    public void moveNode(int nodeCommunity, int toCommunity)
    {
        Iterable<TitanVertex> fromIter = titanGraph.query().has(NODE_COMMUNITY, nodeCommunity).vertices();
        for (TitanVertex vertex : fromIter)
        {
            vertex.property(COMMUNITY, toCommunity);
        }
    }

    @Override
    public double getGraphWeightSum()
    {
        Iterator edges = titanGraph.edges();
        return (double) Iterators.size(edges);
    }

    @Override
    public int reInitializeCommunities()
    {
        Map<Integer, Integer> initCommunities = new HashMap<Integer, Integer>();
        int communityCounter = 0;
        for (TitanVertex v : titanGraph.query().vertices())
        {
            int communityId = v.<Integer>property(COMMUNITY).value();
            if (!initCommunities.containsKey(communityId))
            {
                initCommunities.put(communityId, communityCounter);
                communityCounter++;
            }
            int newCommunityId = initCommunities.get(communityId);
            v.property(COMMUNITY, newCommunityId);
            v.property(NODE_COMMUNITY, newCommunityId);
        }
        return communityCounter;
    }

    @Override
    public int getCommunity(int nodeCommunity)
    {
        TitanVertex vertex = (TitanVertex) titanGraph.query().has(NODE_COMMUNITY, nodeCommunity).vertices().iterator().next();
        int community = vertex.<Integer>property(COMMUNITY).value();
        return community;
    }

    @Override
    public int getCommunityFromNode(int nodeId)
    {
        TitanVertex vertex = (TitanVertex)titanGraph.query().has(NODE_ID, nodeId).vertices().iterator().next();
        return vertex.<Integer>property(COMMUNITY).value();
    }

    @Override
    public int getCommunitySize(int community)
    {
        Iterable<TitanVertex> vertices = titanGraph.query().has(COMMUNITY, community).vertices();
        Set<Integer> nodeCommunities = new HashSet<Integer>();
        for (TitanVertex v : vertices)
        {
            int nodeCommunity = v.<Integer>property(NODE_COMMUNITY).value();
            if (!nodeCommunities.contains(nodeCommunity))
            {
                nodeCommunities.add(nodeCommunity);
            }
        }
        return nodeCommunities.size();
    }

    @Override
    public Map<Integer, List<Integer>> mapCommunities(int numberOfCommunities)
    {
        Map<Integer, List<Integer>> communities = new HashMap<Integer, List<Integer>>();
        for (int i = 0; i < numberOfCommunities; i++)
        {
            Iterator<TitanVertex> verticesIter = titanGraph.query().has(COMMUNITY, i).vertices().iterator();
            List<Integer> vertices = new ArrayList<Integer>();
            while (verticesIter.hasNext())
            {
                Integer nodeId = verticesIter.next().<Integer>property(NODE_ID).value();
                vertices.add(nodeId);
            }
            communities.put(i, vertices);
        }
        return communities;
    }

    private void createSchema()
    {
        final TitanManagement mgmt = titanGraph.openManagement();
        if (!mgmt.containsGraphIndex(NODE_ID))
        {
            final PropertyKey key = mgmt.makePropertyKey(NODE_ID).dataType(Integer.class).make();
            mgmt.buildIndex(NODE_ID, TitanVertex.class).addKey(key).unique().buildCompositeIndex();
        }
        if (!mgmt.containsGraphIndex(COMMUNITY))
        {
            final PropertyKey key = mgmt.makePropertyKey(COMMUNITY).dataType(Integer.class).make();
            mgmt.buildIndex(COMMUNITY, TitanVertex.class).addKey(key).buildCompositeIndex();
        }
        if (!mgmt.containsGraphIndex(NODE_COMMUNITY))
        {
            final PropertyKey key = mgmt.makePropertyKey(NODE_COMMUNITY).dataType(Integer.class).make();
            mgmt.buildIndex(NODE_COMMUNITY, TitanVertex.class).addKey(key).buildCompositeIndex();
        }

        if (mgmt.getEdgeLabel(SIMILAR) == null)
        {
            mgmt.makeEdgeLabel(SIMILAR).multiplicity(Multiplicity.MULTI).directed().make();
        }
        mgmt.commit();
    }

    @Override
    public boolean nodeExists(int nodeId)
    {
        Iterable<TitanVertex> iter = titanGraph.query().has(NODE_ID, nodeId).vertices();
        return iter.iterator().hasNext();
    }

    @Override
    public Iterator<TitanVertex> getVertexIterator()
    {
        return titanGraph.query().vertices().iterator();
    }

    @Override
    public Iterator<Edge> getNeighborsOfVertex(TitanVertex v)
    {
        return v.edges(Direction.OUT, SIMILAR);
    }

    @Override
    public void cleanupVertexIterator(Iterator<TitanVertex> it)
    {
        return; // NOOP - do nothing
    }

    @Override
    public TitanVertex getOtherVertexFromEdge(Edge edge, TitanVertex oneVertex)
    {
        return (TitanVertex) (edge.inVertex().equals(oneVertex) ? edge.outVertex() : edge.inVertex());
    }

    @Override
    public Iterator<Edge> getAllEdges()
    {
        return titanGraph.edges();
    }

    @Override
    public TitanVertex getSrcVertexFromEdge(Edge edge)
    {
        return (TitanVertex) edge.inVertex();
    }

    @Override
    public TitanVertex getDestVertexFromEdge(Edge edge)
    {
        return (TitanVertex) edge.outVertex();
    }

    @Override
    public boolean edgeIteratorHasNext(Iterator<Edge> it)
    {
        return it.hasNext();
    }

    @Override
    public Edge nextEdge(Iterator<Edge> it)
    {
        return it.next();
    }

    @Override
    public void cleanupEdgeIterator(Iterator<Edge> it)
    {
        // NOOP
    }

    @Override
    public boolean vertexIteratorHasNext(Iterator<TitanVertex> it)
    {
        return it.hasNext();
    }

    @Override
    public TitanVertex nextVertex(Iterator<TitanVertex> it)
    {
        return it.next();
    }

    @Override
    public TitanVertex getVertex(Integer i)
    {
        return (TitanVertex) titanGraph.query().has(NODE_ID, i.intValue() + 1).vertices().iterator().next();
    }

    @Override
    public int getVertexId(TitanVertex vertex) {
        return vertex.<Integer>property(NODE_ID).value();
    }
}
