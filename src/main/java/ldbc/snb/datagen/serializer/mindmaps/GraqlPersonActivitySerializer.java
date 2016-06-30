package ldbc.snb.datagen.serializer.mindmaps;

import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.Var;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import static io.mindmaps.graql.api.query.QueryBuilder.var;

public class GraqlPersonActivitySerializer extends PersonActivitySerializer {

    final static int batchSize = 20;
    final static int sleep = 200;
    static int count = 0;

    static private List<Var> entitiesList;
    static private List<Map<String, Var>> relationshipsList;
    static private QueryBuilder qb;
    static private int serializedEntities;
    static private Set<String> messagesSent;

    private BufferedWriter bufferedWriter;

    @Override
    public void initialize(Configuration conf, int reducerId) {
        count++;
        System.out.println("================ COUNT = " + count + "==================");
        qb = QueryBuilder.build();
        entitiesList = new ArrayList<>();
        relationshipsList = new ArrayList<>();
        serializedEntities = 0;
        messagesSent = new HashSet<>();

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(GraqlPersonSerializer.filePath, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void close() {
        try {
            entitiesList.forEach(System.out::println);
            String graqlString = qb.insert(entitiesList).toString();
            GraqlPersonSerializer.sendToEngine(graqlString, sleep);

            graqlString = graqlString.replaceAll("; ", ";\n");
            bufferedWriter.write(graqlString.substring(7) + "\n");

            int i = 0;
            ArrayList<Var> currentList = new ArrayList<>();
            int sizeRelationships = relationshipsList.size();
            for (Map<String, Var> current : relationshipsList) {
                System.out.println("==================  ADDING LIST N " +
                        i + "/" + sizeRelationships + "=======================");
                if (++i % batchSize == 0) {
                    try {
                        graqlString = qb.insert(currentList).toString();
                        GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                        graqlString = graqlString.replaceAll("; ", ";\n");
                        bufferedWriter.write(graqlString.substring(7) + "\n");

                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    currentList = new ArrayList<>();

                }
                for (String key : current.keySet()) {
                    if (key.startsWith("message-")) {
                        if (messagesSent.contains(key)) {
                            currentList.add(current.get(key));
                        } else {
//                            System.out.println("======= BAAALLLSSSSS!!! VERY HAIRYYYYYY!!  ===========");
                            currentList.add(current.get(key).isa("comment"));
                        }
                    } else {
                        currentList.add(current.get(key));
                    }
                }
            }
            bufferedWriter.write("\n");
            bufferedWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void serialize(final Forum forum) {

//        if (++serializedEntities % batchSize == 0) {
//            try {
//                sendToEngine(qb.insert(entitiesList).toString(), sleep);
//                entitiesList = new ArrayList<>();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }

        String dateString = Dictionaries.dates.formatDateTime(forum.creationDate());

        String varForumId = "forum-" + Long.toString(forum.id());

        //  relationshipsList.add(var(varForumId).id(varForumId));

        entitiesList.add(var(varForumId).isa("forum").id(varForumId));

        System.out.println("SERIALISED FORUM ===> " + varForumId);

        entitiesList.add(var("forumtitle-" + Integer.toString(forum.title().hashCode())).isa("name").value(forum.title()));

        entitiesList.add(var().isa("entity-has-resource")
                .rel("entity-target", var(varForumId))
                .rel("entity-value", var("forumtitle-" + Integer.toString(forum.title().hashCode()))));

        //================ creation date =======================

        entitiesList.add(var(Integer.toString(dateString.hashCode())).isa("creation-date").value(dateString));

        entitiesList.add(var().isa("forum-has-resource")
                .rel("forum-target", var(varForumId))
                .rel("forum-value", var(Integer.toString(dateString.hashCode()))));

        //================= forum-with-moderator ==============

        String moderatorId = "person-" + Long.toString(forum.moderator().accountId());

        entitiesList.add(var(moderatorId).id(moderatorId));

        entitiesList.add(var().isa("forum-moderated-by")
                .rel("forum-moderator", var(moderatorId))
                .rel("forum-with-moderator", var(varForumId)));


        //Associate tags to the current forum
        for (Integer i : forum.tags()) {
            String tagId = "tag-" + Integer.toString(i);
            entitiesList.add(var(tagId).isa("tag").id(tagId));

            entitiesList.add(
                    var().isa("with-tag")
                            .rel("subject-with-tag", var(varForumId))
                            .rel("tag-of-subject", var(tagId))
            );
        }


    }

    protected void serialize(final Post post) {
        if (++serializedEntities % batchSize == 0) {
            try {
                String graqlString = qb.insert(entitiesList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriter.write(graqlString.substring(7) + "\n");
                entitiesList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String messageId = "message-" + Long.toString(post.messageId());
        messagesSent.add(messageId);

        entitiesList.add(var(messageId).isa("post").id(messageId));

        System.out.println("FUCK YOU: ->" + "message-" + post.messageId() + " isa post");

        messageResources(post);

        // ====== MESSAGE LOCATED IN =====================

        entitiesList.add(var("country-" + post.countryId()).isa("country").id("country-" + post.countryId()));

        entitiesList.add(var().isa("located-in")
                .rel("subject-with-location", var(messageId))
                .rel("location-of-subject", var("country-" + post.countryId())));

        // ====== MESSAGE HAS CREATOR =====================

        entitiesList.add(var("person-" + post.author().accountId()).id("person-" + post.author().accountId()));

        entitiesList.add(var().isa("creates")
                .rel("message-created", var(messageId))
                .rel("message-creator", var("person-" + post.author().accountId())));

        // ====== MESSAGE HAS TAGS =====================


        for (Integer t : post.tags()) {
            entitiesList.add(var("tag-" + Integer.toString(t)).isa("tag").id("tag-" + Integer.toString(t)));

            entitiesList.add(var().isa("with-tag")
                    .rel("subject-with-tag", var(messageId))
                    .rel("tag-of-subject", var("tag-" + Integer.toString(t))));
        }

        // ====== POST CONTAINED IN FORUM =====================
        Map<String, Var> currentList = new HashMap<>();

        currentList.put("forum-" + post.forumId(), var("forum-" + post.forumId()).isa("forum").id("forum-" + post.forumId()));
        currentList.put(messageId, var(messageId).id(messageId));
        currentList.put("relationship", var().isa("container-of")
                .rel("post-with-container", var(messageId))
                .rel("container-of-post", var("forum-" + post.forumId())));
        relationshipsList.add(currentList);

        // ====== POST LANGUAGE =====================

        String language = "language-" + Dictionaries.languages.getLanguageName(post.language());

        entitiesList.add(var(language).isa("language").id(language));

        entitiesList.add(var().isa("post-has-resource")
                .rel("post-target", var(messageId))
                .rel("post-value", var(language)));
    }

    protected void serialize(final Comment comment) {
        if (++serializedEntities % batchSize == 0) {
            try {
                String graqlString = qb.insert(entitiesList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriter.write(graqlString.substring(7) + "\n");

                entitiesList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        entitiesList.add(var("message-" + comment.messageId()).isa("comment").id("message-" + comment.messageId()));
        messagesSent.add("message-" + comment.messageId());
        System.out.println("APPENA INSERITO MESSAGE VARLIST: ->" + "message-" + comment.messageId() + " isa comment");

        messageResources(comment);

        String commentId = "message-" + Long.toString(comment.messageId());

        Map<String, Var> currentList = new HashMap<>();

        currentList.put(commentId, var(commentId).isa("comment").id("message-" + comment.messageId()));
        if (comment.replyOf() == comment.postId()) {
            //comment to a post
            currentList.put("message-" + Long.toString(comment.postId()), var("message-" + Long.toString(comment.postId())).id("message-" + Long.toString(comment.postId())));

            currentList.put("relationship", var().isa("reply-of")
                    .rel("reply", var(commentId))
                    .rel("message-with-reply", var("message-" + Long.toString(comment.postId()))));

        } else {

            //comment to a comment
            currentList.put("message-" + Long.toString(comment.replyOf()), var("message-" + Long.toString(comment.replyOf())).id("message-" + Long.toString(comment.replyOf())));

            currentList.put("relationship", var().isa("reply-of")
                    .rel("reply", var(commentId))
                    .rel("message-with-reply", var("message-" + Long.toString(comment.replyOf()))));
        }
        relationshipsList.add(currentList);

        // ====== MESSAGE LOCATED IN =====================

        entitiesList.add(var("country-" + comment.countryId()).isa("country").id("country-" + comment.countryId()));

        entitiesList.add(var().isa("located-in")
                .rel("subject-with-location", var(commentId))
                .rel("location-of-subject", var("country-" + comment.countryId())));

        // ====== MESSAGE HAS CREATOR =====================

        entitiesList.add(var("person-" + comment.author().accountId()).id("person-" + comment.author().accountId()));

        entitiesList.add(var().isa("creates")
                .rel("message-created", var(commentId))
                .rel("message-creator", var("person-" + comment.author().accountId())));

        // ====== MESSAGE HAS TAGS =====================


        for (Integer t : comment.tags()) {
            entitiesList.add(var("tag-" + Integer.toString(t)).isa("tag").id("tag-" + Integer.toString(t)));

            entitiesList.add(var().isa("with-tag")
                    .rel("subject-with-tag", var(commentId))
                    .rel("tag-of-subject", var("tag-" + Integer.toString(t))));
        }
    }

    protected void serialize(final Photo photo) {

    }

    protected void serialize(final ForumMembership membership) {
//        if (++serializedEntities % batchSize == 0) {
//            try {
//                sendToEngine(qb.insert(entitiesList).toString(), sleep);
//                entitiesList = new ArrayList<>();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        Map<String, Var> currentList = new HashMap<>();
        currentList.put("penis", var("person-" + Long.toString(membership.person().accountId())).id("person-" + Long.toString(membership.person().accountId())));
        currentList.put("fuckyou", var("forum-" + Long.toString(membership.forumId())).isa("forum").id("forum-" + Long.toString(membership.forumId())));

        currentList.put("ihatethis", var("forum-membership-" + Long.toString(membership.person().accountId()) + Long.toString(membership.forumId())).isa("forum-membership")
                .rel("forum-with-member", var("forum-" + Long.toString(membership.forumId())))
                .rel("forum-member", var("person-" + Long.toString(membership.person().accountId()))));

        String relationVariableValue = "joinDate-" + Dictionaries.dates.formatDateTime(membership.creationDate()).hashCode();
        currentList.put("tomorrow", var(relationVariableValue).isa("creation-date").value(Dictionaries.dates.formatDateTime(membership.creationDate())));

        currentList.put("today", var().isa("relation-has-resource")
                .rel("relation-value", var(relationVariableValue))
                .rel("relation-target", var("forum-membership-" + Long.toString(membership.person().accountId()) + Long.toString(membership.forumId()))));
        relationshipsList.add(currentList);

    }

    protected void serialize(final Like like) {
//        if (++serializedEntities % batchSize == 0) {
//            try {
//                sendToEngine(qb.insert(entitiesList).toString(), sleep);
//                entitiesList = new ArrayList<>();
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
        Map<String, Var> currentList = new HashMap<>();

        currentList.put("tomorrofutureagain", var("person-" + Long.toString(like.user)).id("person-" + Long.toString(like.user)));
        //THEORETICALLY SHOULD WORK WITHOUT ISA MESSAGE
        currentList.put("message-" + Long.toString(like.messageId), var("message-" + Long.toString(like.messageId))
//                .isa("comment")
                .id("message-" + Long.toString(like.messageId)));

        currentList.put("no", var("likes-" + Long.toString(like.user) + Long.toString(like.messageId)).isa("likes")
                .rel("message-liked", var("message-" + Long.toString(like.messageId)))
                .rel("message-liked-by", var("person-" + Long.toString(like.user))));

        String relationVariableValue = "likingDate-" + Dictionaries.dates.formatDateTime(like.date).hashCode();
        currentList.put("maybe", var(relationVariableValue).isa("creation-date").value(Dictionaries.dates.formatDateTime(like.date)));

        currentList.put("gallina", var().isa("relation-has-resource")
                .rel("relation-value", var(relationVariableValue))
                .rel("relation-target", var("likes-" + Long.toString(like.user) + Long.toString(like.messageId))));
        relationshipsList.add(currentList);

    }

    private void messageResources(Message message) {
        String messageId = "message-" + Long.toString(message.messageId());

        String creationDate = Dictionaries.dates.formatDateTime(message.creationDate());

        // ====== MESSAGE CREATION DATE =====================

        entitiesList.add(var(Integer.toString(creationDate.hashCode())).isa("creation-date").value(creationDate));

        entitiesList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var(Integer.toString(creationDate.hashCode()))));

        // ====== MESSAGE LOCATION IP =====================

        entitiesList.add(var(Integer.toString(message.ipAddress().toString().hashCode())).isa("location-ip").value(message.ipAddress().toString()));

        entitiesList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var(Integer.toString(message.ipAddress().toString().hashCode()))));

        // ====== MESSAGE BROWSER =====================

        entitiesList.add(var(Integer.toString(Dictionaries.browsers.getName(message.browserId()).hashCode())).isa("browser-used").value(Dictionaries.browsers.getName(message.browserId())));

        entitiesList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var(Integer.toString(Dictionaries.browsers.getName(message.browserId()).hashCode()))));


        // ====== MESSAGE CONTENT =====================

        String varNameMessageValue = Integer.toString(message.content().hashCode());
        entitiesList.add(var(varNameMessageValue).isa("content").value(message.content()));

        entitiesList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var(varNameMessageValue)));

        // ====== MESSAGE LENGTH =====================

        entitiesList.add(var("length-" + messageId).isa("length").value(Integer.toString(message.content().length())));

        entitiesList.add(var().isa("message-has-resource")
                .rel("message-target", var(messageId))
                .rel("message-value", var("length-" + messageId)));
    }

    public void reset() {

    }
}
