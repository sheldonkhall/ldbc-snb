package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.graql.Var;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.var;

/**
 *
 */
public class GraknPersonSerializer extends PersonSerializer {

    final String keyspace = "SNB";
    GraqlVarLoader loader;

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {
        loader = new GraqlVarLoaderRESTImpl(keyspace);
        System.out.println("====== Worker starting to serialize person. ======");
    }

    @Override
    public void close() {

    }

    @Override
    protected void serialize(Person p) {
        Var personConcept = var().isa("person");
        personConcept.has("snb-id", Long.toString(p.accountId()));
        personConcept.has("first-name", String.valueOf(p.firstName() + " " + p.lastName()));

        loader.sendQueries(Arrays.asList(insert(personConcept)));
    }

    @Override
    protected void serialize(StudyAt studyAt) {

    }

    @Override
    protected void serialize(WorkAt workAt) {

    }

    @Override
    protected void serialize(Person p, Knows knows) {
        Var person = var("acq1").isa("person").has("snb-id", Long.toString(p.accountId()));
        Var knownPerson = var("acq2").isa("person").has("snb-id", Long.toString(knows.to().accountId()));
        Var relation = var().isa("knows").rel("acquaintance1", "acq1").rel("acquaintance2", "acq2");

        loader.sendQueries(Arrays.asList(insert(person, knownPerson, relation)));
    }
}
