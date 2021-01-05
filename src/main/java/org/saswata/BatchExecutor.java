package org.saswata;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BatchExecutor implements AutoCloseable {
  private final ExecutorService executorService;
  private final RelationDumper relationDumper;

  public BatchExecutor(RelationDumper relationDumper, int pool_size) {

    this.relationDumper = relationDumper;

    // fixed thread pool
    executorService =
        new ThreadPoolExecutor(
            pool_size,
            pool_size,
            0L,
            TimeUnit.MILLISECONDS,
            new ExecutorBlockingQueue<>(pool_size));
  }

  public void submit(String line) {
    if (line == null || line.length() < 1) return;

    executorService.submit(() -> {
      try {
        relationDumper.process(line);
      } catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  @Override
  public void close() throws Exception {
    executorService.shutdown();
    if (!executorService.awaitTermination(30, TimeUnit.MINUTES)) {
      System.err.println("Waited for 30 minutes, forced exit");
      System.exit(0);
    }
  }
}
