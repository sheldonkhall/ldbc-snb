package ldbc.snb.datagen.test.grakn;

import ai.grakn.graql.Var;
import ldbc.snb.datagen.serializer.grakn.GraknInvariantSerializer;
import org.junit.Test;

import java.util.Arrays;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.Graql.insert;

/**
 *
 */
public class GraknSerialiserTest {

    @Test
    public void sendSomeDataToEngineTest() {
        Var tagConcept = var().isa("tag");
        tagConcept.has("snb-id",String.valueOf(0));
        tagConcept.has("name","test");

        new GraknInvariantSerializer().sendQueriesToLoader(Arrays.asList(insert(tagConcept)));
    }
}
