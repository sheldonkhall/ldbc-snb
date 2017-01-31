package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.Grakn;
import ai.grakn.GraknGraph;
import ai.grakn.graql.Var;
import com.google.common.collect.Lists;
import ldbc.snb.datagen.objects.Comment;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;
import ldbc.snb.datagen.objects.Like;
import ldbc.snb.datagen.objects.Message;
import ldbc.snb.datagen.objects.Photo;
import ldbc.snb.datagen.objects.Post;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;

import static ai.grakn.graql.Graql.var;
import static ldbc.snb.datagen.serializer.grakn.Utility.flush;

/**
 *
 */
public class PersonActivitySerializer extends ldbc.snb.datagen.serializer.PersonActivitySerializer {

    private GraknGraph graph;
    private static final Logger LOGGER = LoggerFactory.getLogger(PersonActivitySerializer.class);

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {
        graph = Grakn.factory(Grakn.DEFAULT_URI, Objects.requireNonNull(conf.get("grakn.engine.keyspace"))).getGraph();
    }

    @Override
    public void close() {
        graph.close();
    }

    @Override
    protected void serialize(Forum forum) {

    }

    @Override
    protected void serialize(Post post) {
        LOGGER.debug("Serialising Post");
        serialiseMessage(post);
    }

    @Override
    protected void serialize(Comment comment) {
        LOGGER.debug("Serialising Comment");
        serialiseMessage(comment);

        String snbId = Long.toString(comment.messageId());
        String snbId2 = Long.toString(comment.replyOf());
        Var commentConcept2 = var(snbId2).isa("comment").has("snb-id", snbId2);
        flush(graph, Utility::putEntity, Collections.singletonList(commentConcept2));
        Var reply = var().isa("reply")
                .rel("reply-content", snbId)
                .rel("reply-owner", snbId2);
        flush(graph, Utility::putRelation, Lists.newArrayList(reply,
                var(snbId).has("snb-id", snbId), var(snbId2).has("snb-id", snbId2)));
    }

    @Override
    protected void serialize(Photo photo) {

    }

    @Override
    protected void serialize(ForumMembership membership) {

    }

    @Override
    protected void serialize(Like like) {
        LOGGER.debug("Serialising Likes");
        String snbIdPerson = "person-" + Long.toString(like.user);
        String snbIdMessage = "message-" + Long.toString(like.messageId);
        Var person = var(snbIdPerson).isa("person").has("snb-id", snbIdPerson);
        flush(graph, Utility::putEntity, Collections.singletonList(person));

        Var relation = var().isa("likes")
                .rel("liker", snbIdPerson)
                .rel("liked", snbIdMessage);
        flush(graph, Utility::putRelation, Lists.newArrayList(relation,
                var(snbIdPerson).has("snb-id", snbIdPerson),
                var(snbIdMessage).has("snb-id", snbIdMessage)));
    }

    private void serialiseMessage(Message message) {
        String snbIdMessage = "message-" + Long.toString(message.messageId());

        Var commentConcept = var(snbIdMessage).isa("comment").has("snb-id", snbIdMessage);
        flush(graph, Utility::putEntity, Collections.singletonList(commentConcept));

        Var hasResources = var(snbIdMessage)
                .has("content", message.content())
                .has("creation-date", Long.toString(message.creationDate()));
        flush(graph, Utility::putRelation, Lists.newArrayList(hasResources,
                var(snbIdMessage).has("snb-id", snbIdMessage)));

        String snbIdPerson = "person-" + Long.toString(message.author().accountId());
        Var person = var(snbIdPerson).isa("person").has("snb-id", snbIdPerson);
        flush(graph, Utility::putEntity, Collections.singletonList(person));

        Var hasCreator = var().isa("writes")
                .rel("writer", snbIdPerson)
                .rel("written", snbIdMessage);
        flush(graph, Utility::putRelation, Lists.newArrayList(hasCreator,
                var(snbIdMessage).has("snb-id", snbIdMessage),
                var(snbIdPerson).has("snb-id", snbIdPerson)));

        for (Integer t : message.tags()) {
            String snbIdTag = "tag-" + Integer.toString(t);
            Var tagConcept = var(snbIdTag).isa("tag").has("snb-id", snbIdTag);
            flush(graph, Utility::putEntity, Collections.singletonList(tagConcept));

            Var tagging = var().isa("tagging")
                    .rel("tagged-subject", snbIdMessage)
                    .rel("subject-tag", snbIdTag);
            flush(graph, Utility::putRelation, Lists.newArrayList(tagging,
                    var(snbIdMessage).has("snb-id", snbIdMessage),
                    var(snbIdTag).has("snb-id", snbIdTag)));
        }
    }
}
