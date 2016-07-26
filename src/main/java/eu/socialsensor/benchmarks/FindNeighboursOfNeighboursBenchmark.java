package eu.socialsensor.benchmarks;

import com.google.common.base.Stopwatch;
import eu.socialsensor.graphdatabases.GraphDatabase;
import eu.socialsensor.main.BenchmarkConfiguration;
import eu.socialsensor.main.BenchmarkType;
import eu.socialsensor.main.GraphDatabaseType;
import eu.socialsensor.utils.Utils;

import java.util.concurrent.TimeUnit;

/**
 * FindNeighboursOfAllNodesBenchmark implementation
 * 
 * @author sotbeis, sotbeis@iti.gr
 * @author Alexander Patrikalakis
 */
public class FindNeighboursOfNeighboursBenchmark extends PermutingBenchmarkBase implements RequiresGraphData
{
    private final int nodesCount;

    public FindNeighboursOfNeighboursBenchmark(BenchmarkConfiguration config)
    {
        super(config, BenchmarkType.FIND_NEIGHBOURS_OF_NEIGHBOURS);

        nodesCount = config.getNodesCount();
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        for (int i = 0; i < 100; i++) {
            GraphDatabase<?, ?, ?, ?> graphDatabase = Utils.createDatabaseInstance(bench, type);
            graphDatabase.open();
            Stopwatch watch = new Stopwatch();
            watch.start();
            long total = graphDatabase.findAllNeighboursOfNeighboursOfTheFirstFewNodes(nodesCount);
            System.err.println("type = " + type + "; total neighbours of neighbours = " + total);
            graphDatabase.shutdown();
            times.get(type).add((double) watch.elapsed(TimeUnit.MILLISECONDS));
        }
    }
}
