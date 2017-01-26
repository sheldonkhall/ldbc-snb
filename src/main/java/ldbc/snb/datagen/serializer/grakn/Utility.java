package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.GraknGraph;
import ai.grakn.graql.QueryBuilder;
import ai.grakn.graql.Var;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.BiConsumer;

class Utility {
    final static String keyspace = "SNB";

    private static final int numberOfRetries = 10;
    private static final int initialSleepTime = 100;
    private static final double exponentialSleepPower = 2;

    private static final Logger LOGGER = LoggerFactory.getLogger(Utility.class);

    static void flush(GraknGraph graph) {
        boolean hasFailed;
        int numberOfFailures = 0;
        do {
            hasFailed = false;
            try {
                graph.commit();
            } catch (Exception e) {
                LOGGER.debug("Exception: " + e.getMessage());
                hasFailed = true;
                numberOfFailures++;
                LOGGER.debug("Number of failures: " + numberOfFailures);
                if (numberOfFailures >= numberOfRetries) {
                    LOGGER.debug("REACHED MAX NUMBER OF RETRIES !!!!!!!!");
                    throw new RuntimeException(e);
                }
                try {
                    long sleepTime = (long) (initialSleepTime * Math.pow(exponentialSleepPower, numberOfFailures));
                    LOGGER.debug("Start sleeping for " + sleepTime + " ms");
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        } while (hasFailed);
    }

    static void flush(GraknGraph graph, BiConsumer<QueryBuilder, List<Var>> varConsumer, List<Var> varList) {
        boolean hasFailed;
        int numberOfFailures = 0;
        do {
            hasFailed = false;
            try {
                varConsumer.accept(graph.graql(), varList);
                graph.commit();
            } catch (Exception e) {
                LOGGER.debug("Exception: " + e.getMessage());
                hasFailed = true;
                numberOfFailures++;
                LOGGER.debug("Number of failures: " + numberOfFailures);
                if (numberOfFailures >= numberOfRetries) {
                    LOGGER.debug("REACHED MAX NUMBER OF RETRIES !!!!!!!!");
                    throw new RuntimeException(e);
                }
                try {
                    long sleepTime = (long) (initialSleepTime * Math.pow(exponentialSleepPower, numberOfFailures));
                    LOGGER.debug("Start sleeping for " + sleepTime + " ms");
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e1) {
                    e1.printStackTrace();
                }
            }
        } while (hasFailed);
    }

    static void putEntity(QueryBuilder queryBuilder, List<Var> varList) {
        if (!queryBuilder.match(varList.get(0)).ask().execute()) {
            queryBuilder.insert(varList.get(0)).execute();
        }
    }

    static void putRelation(QueryBuilder queryBuilder, List<Var> varList) {
        queryBuilder.match(varList.subList(1, varList.size())).insert(varList.get(0)).execute();
    }
}
