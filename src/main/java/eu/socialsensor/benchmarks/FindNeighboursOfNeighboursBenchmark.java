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
    private final int repetitions;

    public FindNeighboursOfNeighboursBenchmark(BenchmarkConfiguration config)
    {
        super(config, BenchmarkType.FIND_NEIGHBOURS_OF_NEIGHBOURS);

        nodesCount = config.getNodesCount();
        repetitions = config.getRepetitions();
    }

    @Override
    public void benchmarkOne(GraphDatabaseType type, int scenarioNumber)
    {
        GraphDatabase<?, ?, ?, ?> graphDatabase = Utils.createDatabaseInstance(bench, type);
        graphDatabase.open();

        for (int i = 0; i < repetitions; i++) {
            Stopwatch watch = new Stopwatch();
            watch.start();
            long total = graphDatabase.findAllNeighboursOfNeighboursOfTheFirstFewNodes(nodesCount);
            double elapsed = (double) watch.elapsed(TimeUnit.MILLISECONDS);
            System.err.println("type = " + type + "; trial + " + i + ", total neighbours of neighbours = " + total + ", elapsed = " + elapsed);

            times.get(type).add(elapsed);
        }
        graphDatabase.shutdown();
    }
}
