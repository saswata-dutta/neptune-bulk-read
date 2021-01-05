package org.saswata;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.SigV4WebSocketChannelizer;
import org.apache.tinkerpop.gremlin.driver.Tokens;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategy;

public class Main {
  public static void main(String[] args) throws IOException {
    final String NEPTUNE_READ = args[0]; // reader endpoint
    // path to file containing neptune bulk read source vertices
    final String SOURCE_FILE = args[1];
    final int NUM_THREADS = Integer.parseInt(args[2]);
    final int QUERY_TIMEOUT_MINUTES = Integer.parseInt(args[3]);
    final String DUMP_FOLDER = "out";

    final Cluster cluster = clusterProvider(NEPTUNE_READ, NUM_THREADS);
    final GraphTraversalSource g =
        graphProvider(cluster)
            .withStrategies(ReadOnlyStrategy.instance())
            .with(Tokens.ARGS_EVAL_TIMEOUT, TimeUnit.MINUTES.toMillis(QUERY_TIMEOUT_MINUTES))
            .withSideEffect("Neptune#repeatMode", "CHUNKED_DFS");

    final RelationDumper relationDumper = new RelationDumper(g, DUMP_FOLDER);

    process(SOURCE_FILE, NUM_THREADS, relationDumper);

    cluster.close();
  }

  static void process(String input, int numThreads, RelationDumper relationDumper) {

    try (Stream<String> in = Files.lines(Paths.get(input));
        BatchExecutor batchExecutor = new BatchExecutor(relationDumper, numThreads)) {

      in.forEach(acc -> batchExecutor.submit(acc.trim()));

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  static Cluster clusterProvider(String neptune, int pool_size) {

    Cluster.Builder clusterBuilder =
        Cluster.build()
            .addContactPoint(neptune)
            .port(8182)
            .enableSsl(true)
            .channelizer(SigV4WebSocketChannelizer.class)
            .serializer(Serializers.GRAPHBINARY_V1D0)
            .maxInProcessPerConnection(1)
            .minInProcessPerConnection(1)
            .maxSimultaneousUsagePerConnection(1)
            .minSimultaneousUsagePerConnection(1)
            .minConnectionPoolSize(pool_size)
            .maxConnectionPoolSize(pool_size);

    return clusterBuilder.create();
  }

  static GraphTraversalSource graphProvider(Cluster cluster) {
    RemoteConnection connection = DriverRemoteConnection.using(cluster);
    return AnonymousTraversalSource.traversal().withRemote(connection);
  }
}
