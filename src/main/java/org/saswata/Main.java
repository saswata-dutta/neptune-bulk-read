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
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;

public class Main {
  public static void main(String[] args) throws IOException {
    final String NEPTUNE_READ = args[0]; // reader endpoint
    // path to file containing neptune bulk read source vertices
    final String SOURCE_FILE = args[1];
    final int NUM_THREADS = Integer.parseInt(args[2]);
    final int QUERY_TIMEOUT_MINUTES = Integer.parseInt(args[3]);
    final String DUMP_FOLDER = "out";

    //    final Cluster cluster = clusterProvider(NEPTUNE_READ, NUM_THREADS);
    final GraphTraversalSource g = //        graphProvider(cluster)
        localGraphProvider()
            .withStrategies(ReadOnlyStrategy.instance())
            .with(Tokens.ARGS_EVAL_TIMEOUT, TimeUnit.MINUTES.toMillis(QUERY_TIMEOUT_MINUTES))
            .withSideEffect("Neptune#repeatMode", "CHUNKED_DFS");

    final RelationDumper relationDumper = new RelationDumper(g, DUMP_FOLDER);

    process(SOURCE_FILE, NUM_THREADS, relationDumper);

    //    cluster.close();
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

  static GraphTraversalSource localGraphProvider() {
    Graph graph = TinkerGraph.open();
    GraphTraversalSource g = graph.traversal();
    seedData(g);

    return g;
  }

  static void seedData(GraphTraversalSource g) {
    g.addV("aws__account")
        .property(T.id, "aws__account$$a1")
        .addV("aws__account")
        .property(T.id, "aws__account$$a2")
        .addV("aws__account")
        .property(T.id, "aws__account$$a3")
        .addV("aws__account")
        .property(T.id, "aws__account$$a4")
        .addV("aws__account")
        .property(T.id, "aws__account$$a5")
        .addV("aws__account")
        .property(T.id, "aws__account$$a6")
        .addV("aws__account")
        .property(T.id, "aws__account$$a7")
        .addV("aws__sfid")
        .property(T.id, "aws__sfid$$sf_a")
        .addV("aws__sfid")
        .property(T.id, "aws__sfid$$sf_b")
        .addV("aws__sfid")
        .property(T.id, "aws__sfid$$sf_c")
        .addV("cwb__aws__customer")
        .property(T.id, "cwb__aws__customer$$c_x")
        .addV("cwb__aws__customer")
        .property(T.id, "cwb__aws__customer$$c_y")
        .iterate();

    g.V("aws__account$$a1")
        .as("a1")
        .V("aws__account$$a2")
        .as("a2")
        .V("aws__account$$a3")
        .as("a3")
        .V("aws__account$$a4")
        .as("a4")
        .V("aws__account$$a5")
        .as("a5")
        .V("aws__account$$a6")
        .as("a6")
        .V("aws__account$$a7")
        .as("a7")
        .V("aws__sfid$$sf_a")
        .as("sf_a")
        .V("aws__sfid$$sf_b")
        .as("sf_b")
        .V("aws__sfid$$sf_c")
        .as("sf_c")
        .V("cwb__aws__customer$$c_x")
        .as("c_x")
        .V("cwb__aws__customer$$c_y")
        .as("c_y")
        .addE("aws__has_payer")
        .from("a2")
        .to("a4")
        .addE("aws__has_sfid")
        .from("a5")
        .to("sf_b")
        .addE("aws__has_sfid")
        .from("a3")
        .to("sf_a")
        .addE("aws__has_sfid")
        .from("a4")
        .to("sf_a")
        .addE("aws__has_sfid")
        .from("a6")
        .to("sf_c")
        .addE("aws__has_sfid")
        .from("a7")
        .to("sf_c")
        .addE("cwb__aws__has_customer")
        .from("sf_b")
        .to("c_x")
        .addE("cwb__aws__has_customer")
        .from("sf_a")
        .to("c_x")
        .addE("cwb__aws__has_customer")
        .from("c_x")
        .to("c_y")
        .addE("cwb__aws__has_customer")
        .from("a1")
        .to("c_y")
        .addE("cwb__aws__has_customer")
        .from("a6")
        .to("c_y")
        .iterate();
  }
}
