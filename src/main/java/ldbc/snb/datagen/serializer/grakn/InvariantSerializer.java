package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.graql.Var;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import org.apache.hadoop.conf.Configuration;

import static ai.grakn.graql.Graql.var;

public class InvariantSerializer extends ldbc.snb.datagen.serializer.InvariantSerializer {

    final String keyspace = "SNB";

    public void initialize(Configuration conf, int reducerId) {

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
        tagConcept.has("name", String.valueOf(tag.name));
    }

    public void reset() {

    }
}
