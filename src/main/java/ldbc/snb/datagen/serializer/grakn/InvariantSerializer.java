package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.graql.VarPattern;
import com.google.common.collect.Lists;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;

import static ai.grakn.GraknTxType.WRITE;
import static ai.grakn.graql.Graql.var;
import static ldbc.snb.datagen.serializer.grakn.Utility.flush;

public class InvariantSerializer extends ldbc.snb.datagen.serializer.InvariantSerializer {

    private GraknGraph graph;
    private static final Logger LOGGER = LoggerFactory.getLogger(InvariantSerializer.class);

    @Override
    public void initialize(Configuration conf, int reducerId) {
        graph = Grakn.session(Grakn.DEFAULT_URI, Objects.requireNonNull(conf.get("grakn.engine.keyspace"))).open(WRITE);
    }

    @Override
    public void close() {
        graph.close();
    }

    protected void serialize(final Place place) {
    }

    protected void serialize(final Organization organization) {
    }

    protected void serialize(final TagClass tagClass) {
    }

    protected void serialize(final Tag tag) {
        LOGGER.info("Serialising Tag");
        String snbIdTag = "tag-" + String.valueOf(tag.id);

        VarPattern tagConcept = var(snbIdTag).isa("tag").has("snb-id", snbIdTag);
        flush(graph, Utility::putEntity, Collections.singletonList(tagConcept));

        VarPattern hasName = var(snbIdTag).has("name", String.valueOf(tag.name));
        flush(graph, Utility::putRelation, Lists.newArrayList(hasName, var(snbIdTag).has("snb-id", snbIdTag)));
    }

    public void reset() {

    }
}
