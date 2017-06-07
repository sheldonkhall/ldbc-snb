package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.Grakn;
import ai.grakn.graql.VarPattern;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Objects;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.var;

/**
 *
 */
public class GraknPersonSerializer extends PersonSerializer {

    GraqlVarLoader loader;

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {
        System.out.println("====== Worker starting to serialize person. ======");
        String keyspace = Objects.requireNonNull(conf.get("grakn.engine.keyspace"));
        String potentialEngineURI = conf.get("grakn.engine.uri");
        String engineURI = potentialEngineURI != null ? potentialEngineURI : Grakn.DEFAULT_URI;
        loader = new GraqlVarLoaderRESTImpl(keyspace, engineURI);
    }

    @Override
    public void close() {

    }

    @Override
    protected void serialize(Person p) {
        VarPattern personConcept = var().isa("person");
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
        VarPattern person = var("acq1").isa("person").has("snb-id", Long.toString(p.accountId()));
        VarPattern knownPerson = var("acq2").isa("person").has("snb-id", Long.toString(knows.to().accountId()));
        VarPattern relation = var().isa("knows").rel("acquaintance1", "acq1").rel("acquaintance2", "acq2");

        loader.sendQueries(Arrays.asList(insert(person, knownPerson, relation)));
    }
}
