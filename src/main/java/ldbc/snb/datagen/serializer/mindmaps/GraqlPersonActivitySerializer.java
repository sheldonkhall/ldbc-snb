package ldbc.snb.datagen.serializer.mindmaps;

import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.Var;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.DataOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import static io.mindmaps.graql.api.query.QueryBuilder.var;

public class GraqlPersonActivitySerializer extends PersonActivitySerializer {

    final static String POST_TRANSACTION_REQUEST_URL = "http://10.0.1.9:8080/transaction";
    final static int batchSize = 10;
    final static int sleep = 2500;

    private List<Var> varList;
    private QueryBuilder qb;
    private int serializedEntities;

    public static HttpURLConnection buildConnectionPost(String engineUrlPost, String content) {
        try {
            URL url = new URL(engineUrlPost);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setDoInput(true);
            connection.setDoOutput(true);
            connection.connect();

            DataOutputStream output = new DataOutputStream(connection.getOutputStream());
            output.writeBytes(content);
            output.close();

            return connection;
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    public static void sendToEngine(String graqlQuery, int sleep) throws IOException {
        System.out.println("graqlQuery = " + graqlQuery);
        try {
            HttpURLConnection connection = buildConnectionPost(POST_TRANSACTION_REQUEST_URL, graqlQuery);

            int responseCode = connection.getResponseCode();
            if (responseCode != 201) {
                System.out.println("Connection Code: " + responseCode);
            }
            connection.disconnect();
            if (sleep > 0) {
                Thread.sleep(sleep);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void initialize(Configuration conf, int reducerId) {
        qb = QueryBuilder.build();
        varList = new ArrayList<>();
        serializedEntities = 0;

    }


    @Override
    public void close() {
        try {
            varList.forEach(System.out::println);
            sendToEngine(qb.insert(varList).toString(), sleep);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void serialize(final Forum forum) {

        if (++serializedEntities % batchSize == 0) {
            try {
                sendToEngine(qb.insert(varList).toString(), sleep);
                varList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String dateString = Dictionaries.dates.formatDateTime(forum.creationDate());

        String varForumId = "forum-" + Long.toString(forum.id());

        varList.add(var(varForumId).id(varForumId));

        varList.add(var(varForumId).isa("forum").id(varForumId));

        varList.add(var(forum.title().replaceAll("\\s+", "")).isa("name").value(forum.title()));

        varList.add(var().isa("forum-has-resource")
                .rel("forum-target", var(varForumId))
                .rel("forum-value", var(forum.title().replaceAll("\\s+", ""))));

        //================ creation date =======================

        varList.add(var(Integer.toString(dateString.hashCode())).isa("creation-date").value(dateString));

        varList.add(var().isa("forum-has-resource")
                .rel("forum-target", var(varForumId))
                .rel("forum-value", var(Integer.toString(dateString.hashCode()))));

        //================= forum-with-moderator ==============

        String moderatorId = "person-" + Long.toString(forum.moderator().accountId());

        varList.add(var(moderatorId).id(moderatorId));

        varList.add(var().isa("forum-has-resource")
                .rel("forum-moderator", var(moderatorId))
                .rel("forum-with-moderator", var(varForumId)));


        //Associate tags to the current forum
        for (Integer i : forum.tags()) {
            String tagId = "tag-" + Integer.toString(i);
            varList.add(var(tagId).id(tagId));

            varList.add(
                    var().isa("with-tag")
                            .rel("subject-with-tag", var(varForumId))
                            .rel("tag-of-subject", var(tagId))
            );
        }


    }

    protected void serialize(final Post post) {

        if (++serializedEntities % batchSize == 0) {
            try {
                sendToEngine(qb.insert(varList).toString(), sleep);
                varList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        messageResources(post, "post");

        String messageId = "message-" + Long.toString(post.messageId());

        // ====== MESSAGE LOCATED IN =====================

        varList.add(var("country-" + post.countryId()).id("country-" + post.countryId()));

        varList.add(var().isa("located-in")
                .rel("subject-with-location", var(messageId))
                .rel("location-of-subject", var("country-" + post.countryId())));

        // ====== MESSAGE HAS CREATOR =====================

        varList.add(var("person-" + post.author().accountId()).id("person-" + post.author().accountId()));

        varList.add(var().isa("creates")
                .rel("message-created", var(messageId))
                .rel("message-creator", var("person-" + post.author().accountId())));

        // ====== MESSAGE HAS TAGS =====================


        for (Integer t : post.tags()) {
            varList.add(var("tag-" + Integer.toString(t)).id("tag-" + Integer.toString(t)));

            varList.add(var().isa("with-tag")
                    .rel("subject-with-tag", var(messageId))
                    .rel("tag-of-subject", var("tag-" + Integer.toString(t))));
        }

        // ====== POST CONTAINED IN FORUM =====================

        varList.add(var("forum-" + post.forumId()).id("forum-" + post.forumId()));

        varList.add(var().isa("container-of")
                .rel("post-with-container", var(messageId))
                .rel("container-of-post", var("forum-" + post.forumId())));

        // ====== POST LANGUAGE =====================

        String language = "language-" + Dictionaries.languages.getLanguageName(post.language());

        varList.add(var(language).isa("language").id(language));

        varList.add(var().isa("post-has-resource")
                .rel("post-target", var(messageId))
                .rel("post-value", var(language)));


    }

    protected void serialize(final Comment comment) {

        messageResources(comment, "comment");

        String commentId = "message-" + Long.toString(comment.messageId());


        if (comment.replyOf() == comment.postId()) {

            //comment to a post
            varList.add(var("message-" + Long.toString(comment.postId())).id("message-" + Long.toString(comment.postId())));

            varList.add(var().isa("reply-of")
                    .rel("reply", var(commentId))
                    .rel("message-with-reply", var("message-" + Long.toString(comment.postId()))));

        } else {

            //comment to a comment
            varList.add(var("message-" + Long.toString(comment.replyOf())).id("message-" + Long.toString(comment.replyOf())));

            varList.add(var().isa("reply-of")
                    .rel("reply", var(commentId))
                    .rel("message-with-reply", var("message-" + Long.toString(comment.replyOf()))));
        }

        // ====== MESSAGE LOCATED IN =====================

        varList.add(var("country-" + comment.countryId()).id("country-" + comment.countryId()));

        varList.add(var().isa("located-in")
                .rel("subject-with-location", var(commentId))
                .rel("location-of-subject", var("country-" + comment.countryId())));

        // ====== MESSAGE HAS CREATOR =====================

        varList.add(var("person-" + comment.author().accountId()).id("person-" + comment.author().accountId()));

        varList.add(var().isa("creates")
                .rel("message-created", var(commentId))
                .rel("message-creator", var("person-" + comment.author().accountId())));

        // ====== MESSAGE HAS TAGS =====================


        for (Integer t : comment.tags()) {
            varList.add(var("tag-" + Integer.toString(t)).id("tag-" + Integer.toString(t)));

            varList.add(var().isa("with-tag")
                    .rel("subject-with-tag", var(commentId))
                    .rel("tag-of-subject", var("tag-" + Integer.toString(t))));
        }


    }

    protected void serialize(final Photo photo) {

    }

    protected void serialize(final ForumMembership membership) {

        varList.add(var("person-" + Long.toString(membership.person().accountId())).id("person-" + Long.toString(membership.person().accountId())));
        varList.add(var("forum-" + Long.toString(membership.forumId())).id("forum-" + Long.toString(membership.forumId())));

        varList.add(var("forum-membership-" + Long.toString(membership.person().accountId()) + Long.toString(membership.forumId())).isa("forum-membership")
                .rel("forum-with-member", var("forum-" + Long.toString(membership.forumId())))
                .rel("forum-member", var("person-" + Long.toString(membership.person().accountId()))));

        String relationVariableValue = "joinDate-" + Dictionaries.dates.formatDateTime(membership.creationDate()).hashCode();
        varList.add(var(relationVariableValue).isa("creation-date").value(Dictionaries.dates.formatDateTime(membership.creationDate())));

        varList.add(var().isa("relation-has-resource")
                .rel("relation-value", var(relationVariableValue))
                .rel("relation-target", var("forum-membership-" + Long.toString(membership.person().accountId()) + Long.toString(membership.forumId()))));


    }

    protected void serialize(final Like like) {

        varList.add(var("person-" + Long.toString(like.user)).id("person-" + Long.toString(like.user)));
        varList.add(var("message-" + Long.toString(like.messageId)).id("message-" + Long.toString(like.messageId)));

        varList.add(var("likes-" + Long.toString(like.user) + Long.toString(like.messageId)).isa("likes")
                .rel("messaged-liked", var("message-" + Long.toString(like.messageId)))
                .rel("message-liked-by", var("person-" + Long.toString(like.user))));

        String relationVariableValue = "likingDate-" + Dictionaries.dates.formatDateTime(like.date).hashCode();
        varList.add(var(relationVariableValue).isa("creation-date").value(Dictionaries.dates.formatDateTime(like.date)));

        varList.add(var().isa("relation-has-resource")
                .rel("relation-value", var(relationVariableValue))
                .rel("relation-target", var("likes-" + Long.toString(like.user) + Long.toString(like.messageId))));

    }

    private void messageResources(Message message, String instanceType) {
        String messageId = "message-" + Long.toString(message.messageId());
        varList.add(var(messageId).id(messageId).isa(instanceType));

        String creationDate = Dictionaries.dates.formatDateTime(message.creationDate());

        // ====== MESSAGE CREATION DATE =====================

        varList.add(var(Integer.toString(creationDate.hashCode())).isa("creation-date").value(creationDate));

        varList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var(Integer.toString(creationDate.hashCode()))));

        // ====== MESSAGE LOCATION IP =====================

        varList.add(var(Integer.toString(message.ipAddress().toString().hashCode())).isa("location-ip").value(message.ipAddress().toString()));

        varList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var(Integer.toString(message.ipAddress().toString().hashCode()))));

        // ====== MESSAGE BROWSER =====================

        varList.add(var(Integer.toString(Dictionaries.browsers.getName(message.browserId()).hashCode())).isa("browser-used").value(Dictionaries.browsers.getName(message.browserId())));

        varList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var(Integer.toString(Dictionaries.browsers.getName(message.browserId()).hashCode()))));


        // ====== MESSAGE CONTENT =====================

        String varNameMessageValue = Integer.toString(message.content().hashCode());
        varList.add(var(varNameMessageValue).isa("content").value(message.content()));

        varList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var(varNameMessageValue)));

        // ====== MESSAGE LENGTH =====================

        varList.add(var("length-" + messageId).isa("length").value(Integer.toString(message.content().length())));

        varList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var("length-" + messageId)));
    }

    public void reset() {

    }
}
