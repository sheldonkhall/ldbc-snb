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

import java.util.Collections;

import static ai.grakn.graql.Graql.var;
import static ldbc.snb.datagen.serializer.grakn.Utility.flush;
import static ldbc.snb.datagen.serializer.grakn.Utility.keyspace;

/**
 *
 */
public class PersonSerializer extends ldbc.snb.datagen.serializer.PersonSerializer {

    private GraknGraph graph;

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {
        graph = Grakn.factory(Grakn.DEFAULT_URI, keyspace).getGraph();
    }

    @Override
    public void close() {
        graph.close();
    }

    @Override
    protected void serialize(Person p) {
        String snbId = Long.toString(p.accountId());

        Var personConcept = var(snbId).isa("person");
        personConcept.has("snb-id", snbId);
        flush(graph, Utility::putEntity, Collections.singletonList(personConcept));

        Var hasName = var(snbId).has("name", String.valueOf(p.firstName() + " " + p.lastName()));
        flush(graph, Utility::putRelation, Lists.newArrayList(hasName, var(snbId).has("snb-id", snbId)));
    }

    @Override
    protected void serialize(StudyAt studyAt) {

    }

    @Override
    protected void serialize(WorkAt workAt) {

    }

    @Override
    protected void serialize(Person p, Knows knows) {
        String snbId1 = Long.toString(p.accountId());
        String snbId2 = Long.toString(knows.to().accountId());
        Var knownPerson = var(snbId2).isa("person").has("snb-id", snbId2);
        flush(graph, Utility::putEntity, Collections.singletonList(knownPerson));

        Var relation = var().isa("knows").rel("acquaintance1", snbId1).rel("acquaintance2", snbId2);
        flush(graph, Utility::putRelation, Lists.newArrayList(relation,
                var(snbId1).has("snb-id", snbId1), var(snbId2).has("snb-id", snbId2)));
    }
}
