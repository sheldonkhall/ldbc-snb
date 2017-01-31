package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.graql.Var;
import com.google.common.collect.Lists;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;

import static ai.grakn.graql.Graql.var;
import static ldbc.snb.datagen.serializer.grakn.Utility.flush;

/**
 *
 */
public class PersonSerializer extends ldbc.snb.datagen.serializer.PersonSerializer {

    private GraknGraph graph;
    private static final Logger LOGGER = LoggerFactory.getLogger(PersonSerializer.class);

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {
        graph = Grakn.factory(Grakn.DEFAULT_URI, Objects.requireNonNull(conf.get("grakn.engine.keyspace"))).getGraph();
    }

    @Override
    public void close() {
        graph.close();
    }

    @Override
    protected void serialize(Person p) {
        LOGGER.debug("Serialising Person");
        String snbIdPerson = "person-" + Long.toString(p.accountId());

        Var personConcept = var(snbIdPerson).isa("person").has("snb-id", snbIdPerson);
        flush(graph, Utility::putEntity, Collections.singletonList(personConcept));

        Var hasName = var(snbIdPerson).has("first-name", String.valueOf(p.firstName()));
        flush(graph, Utility::putRelation, Lists.newArrayList(hasName, var(snbIdPerson).has("snb-id", snbIdPerson)));
    }

    @Override
    protected void serialize(StudyAt studyAt) {

    }

    @Override
    protected void serialize(WorkAt workAt) {

    }

    @Override
    protected void serialize(Person p, Knows knows) {
        LOGGER.debug("Serialising Knows");
        String snbIdPerson1 = "person-" + Long.toString(p.accountId());
        String snbIdPerson2 = "person-" + Long.toString(knows.to().accountId());
        Var knownPerson = var(snbIdPerson2).isa("person").has("snb-id", snbIdPerson2);
        flush(graph, Utility::putEntity, Collections.singletonList(knownPerson));

        Var relation = var().isa("knows")
                .rel("acquaintance1", snbIdPerson1)
                .rel("acquaintance2", snbIdPerson2);
        flush(graph, Utility::putRelation, Lists.newArrayList(relation,
                var(snbIdPerson1).has("snb-id", snbIdPerson1),
                var(snbIdPerson2).has("snb-id", snbIdPerson2)));
    }
}
