package ldbc.snb.datagen.test.grakn;

import ai.grakn.graql.VarPattern;
import ldbc.snb.datagen.serializer.grakn.GraqlVarLoaderRESTImpl;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;

import static ai.grakn.graql.Graql.var;
import static ai.grakn.graql.Graql.insert;

/**
 *
 */
public class GraknSerialiserTest {

    @Test
    public void sendSomeDataToEngineTest() {
        int numberOfTags = 100;
        GraqlVarLoaderRESTImpl serializer = new GraqlVarLoaderRESTImpl("SNB");
        for (int i=0; i<numberOfTags; i++) {
            String randomUUID = UUID.randomUUID().toString();

            System.out.println("serializing tag: "+randomUUID);
            VarPattern tagConcept = var().isa("tag");
            tagConcept.has("snb-id", randomUUID);
            tagConcept.has("name", "test");

            serializer.sendQueries(Arrays.asList(insert(tagConcept)));
        }
    }

    @Test
    public void sendARelationToEngineTest() {
        GraqlVarLoaderRESTImpl serializer = new GraqlVarLoaderRESTImpl("SNB");

        VarPattern person = var("acq1").isa("person").has("snb-id", UUID.randomUUID().toString());
        VarPattern knownPerson = var("acq2").isa("person").has("snb-id", UUID.randomUUID().toString());
        VarPattern relation = var().isa("knows").rel("acquaintance1", "acq1").rel("acquaintance2", "acq2");

        serializer.sendQueries(Arrays.asList(insert(person, knownPerson, relation)));
    }

    @Test
    public void serialiseActivityPostTest() {
        GraqlVarLoaderRESTImpl serializer = new GraqlVarLoaderRESTImpl("SNB");

        Collection<VarPattern> vars = new ArrayList<>();

        vars.add(var("comment").isa("comment")
                .has("snb-id", UUID.randomUUID().toString())
                .has("content", UUID.randomUUID().toString())
                .has("creation-date", UUID.randomUUID().toString()));

        vars.add(var("author").isa("person").has("snb-id", UUID.randomUUID().toString()));

        vars.add(var().isa("writes").rel("writer", "author").rel("written", "comment"));

        for( Integer t=0; t<10; t++) {
            vars.add(var("tag-"+t.toString()).isa("tag").has("snb-id", t.toString()));

            vars.add(var().isa("tagging").rel("tagged-subject", "comment").rel("subject-tag", "tag-"+t.toString()));
        }

        serializer.sendQueries(Arrays.asList(insert(vars)));
    }

    @Test
    public void serialiseActivityCommentTest(){
        GraqlVarLoaderRESTImpl serializer = new GraqlVarLoaderRESTImpl("SNB");

        Collection<VarPattern> vars = new ArrayList<>();

        vars.add(var("comment").isa("comment")
                .has("snb-id", UUID.randomUUID().toString())
                .has("content", UUID.randomUUID().toString())
                .has("creation-date", UUID.randomUUID().toString()));

        vars.add(var("original-comment").isa("comment").has("snb-id", UUID.randomUUID().toString()));

        vars.add(var().isa("reply").rel("reply-owner","original-comment").rel("reply-content","comment"));

        vars.add(var("author").isa("person").has("snb-id", UUID.randomUUID().toString()));

        for( Integer t=0; t<10; t++ ) {
            vars.add(var("tag-"+t.toString()).isa("tag").has("snb-id", t.toString()));

            vars.add(var().isa("tagging").rel("tagged-subject", "comment").rel("subject-tag", "tag-"+t.toString()));
        }

        serializer.sendQueries(Arrays.asList(insert(vars)));
    }

    @Test
    public void serialiseActivityLikeTest() {
        GraqlVarLoaderRESTImpl serializer = new GraqlVarLoaderRESTImpl("SNB");

        VarPattern person = var("person").isa("person").has("snb-id", UUID.randomUUID().toString());
        VarPattern comment = var("comment").isa("comment").has("snb-id", UUID.randomUUID().toString());
        VarPattern relation = var().isa("likes").rel("liker", "person").rel("liked", "comment");

        serializer.sendQueries(Arrays.asList(insert(person,comment,relation)));
    }
}
