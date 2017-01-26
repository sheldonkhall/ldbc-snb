package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.graql.Var;
import ldbc.snb.datagen.objects.Comment;
import ldbc.snb.datagen.objects.Forum;
import ldbc.snb.datagen.objects.ForumMembership;
import ldbc.snb.datagen.objects.Like;
import ldbc.snb.datagen.objects.Photo;
import ldbc.snb.datagen.objects.Post;
import org.apache.hadoop.conf.Configuration;

import java.util.ArrayList;
import java.util.Collection;

import static ai.grakn.graql.Graql.var;

/**
 *
 */
public class PersonActivitySerializer extends ldbc.snb.datagen.serializer.PersonActivitySerializer {

    final String keyspace = "SNB";

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {

    }

    @Override
    public void close() {

    }

    @Override
    protected void serialize(Forum forum) {

    }

    @Override
    protected void serialize(Post post) {
        Collection<Var> vars = new ArrayList<>();

        vars.add(var("comment").isa("comment")
                .has("snb-id", Long.toString(post.messageId()))
                .has("content", post.content())
                .has("creation-date", Long.toString(post.creationDate())));

        vars.add(var("author").isa("person").has("snb-id", Long.toString(post.author().accountId())));

        vars.add(var().isa("writes").rel("writer", "author").rel("written", "comment"));

        for (Integer t : post.tags()) {
            vars.add(var("tag-" + t.toString()).isa("tag").has("snb-id", t.toString()));

            vars.add(var().isa("tagging").rel("tagged-subject", "comment").rel("subject-tag", "tag-" + t.toString()));
        }
    }

    @Override
    protected void serialize(Comment comment) {
        Collection<Var> vars = new ArrayList<>();

        vars.add(var("comment").isa("comment")
                .has("snb-id", Long.toString(comment.messageId()))
                .has("content", comment.content())
                .has("creation-date", Long.toString(comment.creationDate())));

        vars.add(var("original-comment").isa("comment").has("snb-id", Long.toString(comment.replyOf())));

        vars.add(var().isa("reply").rel("reply-owner", "original-comment").rel("reply-content", "comment"));

        vars.add(var("author").isa("person").has("snb-id", Long.toString(comment.author().accountId())));

        for (Integer t : comment.tags()) {
            vars.add(var("tag-" + t.toString()).isa("tag").has("snb-id", t.toString()));

            vars.add(var().isa("tagging").rel("tagged-subject", "comment").rel("subject-tag", "tag-" + t.toString()));
        }
    }

    @Override
    protected void serialize(Photo photo) {

    }

    @Override
    protected void serialize(ForumMembership membership) {

    }

    @Override
    protected void serialize(Like like) {
        Var person = var("person").isa("person").has("snb-id", Long.toString(like.user));
        Var comment = var("comment").isa("comment").has("snb-id", Long.toString(like.messageId));
        Var relation = var().isa("likes").rel("liker", "person").rel("liked", "comment");
    }
}
