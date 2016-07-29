package eu.socialsensor.insert;

import eu.socialsensor.graphdatabases.Neo4jGraphDatabase;
import eu.socialsensor.main.BenchmarkingException;
import eu.socialsensor.main.GraphDatabaseType;
import org.neo4j.cypher.internal.javacompat.ExecutionEngine;
import org.neo4j.cypher.javacompat.internal.GraphDatabaseCypherService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.NullLogProvider;

import java.io.File;

/**
 * Implementation of single Insertion in Neo4j graph database
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 * 
 */
@SuppressWarnings("deprecation")
public class Neo4jSingleInsertion extends InsertionBase<Node>
{
    private final GraphDatabaseService neo4jGraph;
    private final ExecutionEngine engine;


    public Neo4jSingleInsertion(GraphDatabaseService neo4jGraph, File resultsPath)
    {
        super(GraphDatabaseType.NEO4J, resultsPath);
        this.neo4jGraph = neo4jGraph;

        engine = new ExecutionEngine(new GraphDatabaseCypherService(this.neo4jGraph), NullLogProvider.getInstance());
    }

    public Node getOrCreate(String nodeId)
    {
        Node result = null;

        try(final Transaction tx = neo4jGraph.beginTx())
        {
            result = neo4jGraph.findNode(Neo4jGraphDatabase.NODE_LABEL, "nodeId", nodeId);

            if (result == null) {
                try
                {
                    Node node = neo4jGraph.createNode(Neo4jGraphDatabase.NODE_LABEL);
                    node.setProperty("nodeId", nodeId);
                    tx.success();
                    result = node;
                }
                catch (Exception e)
                {
                    tx.failure();
                    throw new BenchmarkingException("unable to get or create node " + nodeId, e);
                }
            }
            tx.close();
        }

        return result;
    }

    @Override
    public void relateNodes(Node src, Node dest)
    {
        try (final Transaction tx = neo4jGraph.beginTx())
        {
            try
            {
                src.createRelationshipTo(dest, Neo4jGraphDatabase.RelTypes.SIMILAR);
                dest.createRelationshipTo(src, Neo4jGraphDatabase.RelTypes.SIMILAR);
                tx.success();
            }
            catch (Exception e)
            {
                tx.failure();
                throw new BenchmarkingException("unable to relate nodes", e);
            }
        }
    }
}
