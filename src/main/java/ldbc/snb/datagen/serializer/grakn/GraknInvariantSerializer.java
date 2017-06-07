package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.Grakn;
import ai.grakn.graql.VarPattern;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;
import java.util.Objects;

import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;

public class GraknInvariantSerializer extends InvariantSerializer {

    GraqlVarLoader loader;

    public void initialize(Configuration conf, int reducerId) {
        System.out.println("====== Worker starting to serialize invariants. ======");
        System.out.println("====== WARNING -- serializer omits unused tags. ======");
        String keyspace = Objects.requireNonNull(conf.get("grakn.engine.keyspace"));
        String potentialEngineURI = conf.get("grakn.engine.uri");
        String engineURI = potentialEngineURI != null ? potentialEngineURI : Grakn.DEFAULT_URI;
        loader = new GraqlVarLoaderRESTImpl(keyspace, engineURI);
    }

    public void close() {
    }

    protected void serialize(final Place place) {
    }

    protected void serialize(final Organization organization) {
    }

    protected void serialize(final TagClass tagClass) {
    }

    protected void serialize(final Tag tag) {
        VarPattern tagConcept = var("tagConcept").isa("tag").has("snb-id", String.valueOf(tag.id));
        VarPattern tagConceptWithName = var("tagConcept").has("name", String.valueOf(tag.name));

        // If the tag is not in a relationship it is currently ignored.
        loader.sendQueries(Arrays.asList(match(tagConcept).insert(tagConceptWithName)));
    }

    public void reset() {

    }
}
