package org.saswata;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.hasLabel;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class RelationDumper {

  private static final String ACC_VERTEX_LABEL = "aws__account";

  private static final String SFID = "sfid";
  private static final String SFID_VERTEX_LABEL = "aws__sfid";
  private static final String[] SFID_EDGES = {"aws__has_sfid"};

  private static final String CUSTOMER = "customer";
  private static final String CUST_VERTEX_LABEL = "cwb__aws__customer";
  private static final String[] CUST_EDGES = {"aws__has_sfid", "cwb__aws__has_customer"};

  private static final String VERTEX_ID_SEPARATOR = "$$";
  private static final String VERTEX_ID_SEPARATOR_PAT = Pattern.quote(VERTEX_ID_SEPARATOR);

  private final GraphTraversalSource g;
  private final Path parent;

  public RelationDumper(GraphTraversalSource g, String folder) throws IOException {
    this.g = g;
    this.parent = Files.createDirectories(Paths.get(folder));
  }

  public void process(String line) throws IOException {
    String[] items = line.split(",");
    if (items.length != 2) throw new IllegalArgumentException("Malformed input line " + line);

    String type = items[0];
    String id = items[1];

    Stream<String> accIds = queryChildAccounts(type, id);
    Stream<String> outLines = accIds.map(it -> genOutLine(type, id, it));
    persist(type, id, outLines::iterator);
  }

  private void persist(String type, String id, Iterable<String> outLines) throws IOException {
    Files.write(parent.resolve(type + "__" + id), outLines, StandardCharsets.UTF_8);
  }

  private static String genOutLine(String type, String id, String accId) {
    return type + "," + id + "," + accId;
  }

  private Stream<String> queryChildAccounts(String type, String id) {
    System.out.println(g.V(getVertexId(type, id)).id().next());
    System.out.println(g.V(getVertexId(type, id))
        .repeat(in(getEdgeFilter(type)).simplePath())
        .emit(hasLabel(ACC_VERTEX_LABEL))
        .id().toList());

    return g.V(getVertexId(type, id))
        .repeat(in(getEdgeFilter(type)).simplePath())
        .emit(hasLabel(ACC_VERTEX_LABEL))
        .id()
        .toStream()
        .map(RelationDumper::extractAccountId);
  }

  private static String extractAccountId(Object id) {
    String[] parts = id.toString().split(VERTEX_ID_SEPARATOR_PAT);
    if (parts.length != 2) {
      throw new IllegalArgumentException("Found malformed namespaced vertex: " + id);
    }
    return parts[1];
  }

  private String getVertexId(String type, String id) {
    if (CUSTOMER.equals(type)) {
      return CUST_VERTEX_LABEL + VERTEX_ID_SEPARATOR + id;
    } else if (SFID.equals(type)) {
      return SFID_VERTEX_LABEL + VERTEX_ID_SEPARATOR + id;
    }

    throw new IllegalArgumentException("Bad input vertex type : " + type);
  }

  private String[] getEdgeFilter(String type) {
    if (CUSTOMER.equals(type)) {
      return CUST_EDGES;
    } else if (SFID.equals(type)) {
      return SFID_EDGES;
    }

    throw new IllegalArgumentException("Bad input vertex type : " + type);
  }
}
