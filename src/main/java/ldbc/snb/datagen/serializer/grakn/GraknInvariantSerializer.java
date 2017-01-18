package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.engine.loader.Loader;
import ai.grakn.graql.Var;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import org.apache.hadoop.conf.Configuration;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.var;

public class GraknInvariantSerializer extends InvariantSerializer {

    Loader loader;

    public void initialize(Configuration conf, int reducerId) {
        loader = new Loader("SNB");
    }

    public void close() {
        loader.waitToFinish(5000);
    }

    protected void serialize(final Place place) {
    }

    protected void serialize(final Organization organization) {
    }

    protected void serialize(final TagClass tagClass) {
    }

    protected void serialize(final Tag tag) {
        Var tagConcept = var().isa("tag");
        tagConcept.hasResource(var().isa("snb-id").value(String.valueOf(tag.id)));
        tagConcept.hasResource(var().isa("name").value(String.valueOf(tag.name)));
        loader.add(insert(tagConcept));
    }

    public void reset() {

    }
}
