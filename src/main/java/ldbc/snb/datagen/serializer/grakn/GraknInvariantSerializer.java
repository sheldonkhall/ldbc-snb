package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.graql.Var;
import ai.grakn.graql.internal.pattern.Patterns;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import org.apache.hadoop.conf.Configuration;

import java.util.Arrays;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.match;
import static ai.grakn.graql.Graql.var;

public class GraknInvariantSerializer extends InvariantSerializer {

    final String keyspace = "SNB";
    GraqlVarLoader loader;

    public void initialize(Configuration conf, int reducerId) {
        loader = new GraqlVarLoaderRESTImpl(keyspace);
        System.out.println("====== Worker starting to serialize invariants. ======");
        System.out.println("====== WARNING -- serializer omits unused tags. ======");
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
        Var tagConcept = var().isa("tag");
        tagConcept.has("snb-id", String.valueOf(tag.id));
        Var tagConceptWithName = Patterns.copyOf(tagConcept.admin());
        tagConceptWithName.has("name", String.valueOf(tag.name));

        // If the tag is not in a relationship it is currently ignored.
        loader.sendQueries(Arrays.asList(match(tagConcept).insert(tagConceptWithName)));
    }

    public void reset() {

    }
}
