package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.graql.InsertQuery;

import java.util.Collection;

/**
 * Functionality for loading vars into a Grakn backend. The implementations of this class deal with the intricacies of
 * setting up the connections and sending and receiving data reliably.
 */
public interface GraqlVarLoader {
    void sendQueries(Collection<InsertQuery> queries);
}
