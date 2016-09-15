package ldbc.snb.datagen.serializer.mindmaps;

import io.mindmaps.graql.Graql;
import io.mindmaps.graql.Var;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.*;
import ldbc.snb.datagen.serializer.PersonActivitySerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static io.mindmaps.graql.Graql.var;


public class GraqlPersonActivitySerializer extends PersonActivitySerializer {

    private static Set<String> ids = new HashSet<>();
    private BufferedWriter bufferedWriter;

    private long startTime;

    @Override
    public void initialize(Configuration conf, int reducerId) {
        System.out.println("======================== Person Activity ========================");
        startTime = System.currentTimeMillis();

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(GraqlPersonSerializer.filePath, true));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        long endTime = System.currentTimeMillis();
        System.out.println("========================  TIME USED INVARIANT  ========================");
        System.out.println(endTime - startTime + " ms");
        System.out.println("=======================================================================");
    }

    protected void serialize(final Forum forum) {
        List<Var> varList = new ArrayList<>();
        String dateString = Dictionaries.dates.formatDateTime(forum.creationDate());
        String idForum = "forum-" + Long.toString(forum.id());
//        System.out.println("SERIALISING FORUM ===> " + idForum);

        varList.add(var().isa("forum")
                .id(idForum)
//                .value(forum.title())
                .has("title", forum.title())
                .has("creation-date", dateString));

        String idPerson = "person-" + Long.toString(forum.moderator().accountId());
        varList.add(var().isa("moderates")
                .rel("moderator", var().id(idPerson))
                .rel("moderated", var().id(idForum)));

        for (Integer i : forum.tags()) {
            String idTag = "tag-" + Integer.toString(i);
            varList.add(var().isa("tagging")
                    .rel("tagged-subject", var().id(idForum))
                    .rel("subject-tag", var().id(idTag)));
        }

        writeToFile(varList, "# " + idForum);
    }

    protected void serialize(final Post post) {

        List<Var> varList = new ArrayList<>();
        String idMessage = "message-" + Long.toString(post.messageId());
//        System.out.println("SERIALISING POST ===> " + idMessage);

        varList.add(var().isa("post").id(idMessage)
                .has("creation-date", Dictionaries.dates.formatDateTime(post.creationDate()))
                .has("browser-used", Dictionaries.browsers.getName(post.browserId()))
                .has("location-ip", post.ipAddress().toString())
                .has("content", post.content().replace("\n", " "))
                .has("length", Integer.toString(post.content().length())));

        varList.add(var().isa("resides")
                .rel("located-subject", var().id(idMessage))
                .rel("subject-location", var().id("country-" + post.countryId())));

        varList.add(var().isa("writes")
                .rel("written", var().id(idMessage))
                .rel("writer", var().id("person-" + post.author().accountId())));

        varList.addAll(post.tags().stream().map(t -> var().isa("tagging")
                .rel("tagged-subject", var().id(idMessage))
                .rel("subject-tag", var().id("tag-" + Integer.toString(t)))).collect(Collectors.toList()));

        varList.add(var().isa("containing")
                .rel("contained-post", var().id(idMessage))
                .rel("post-container", var().id("forum-" + post.forumId())));

        String idLanguage = "language-" + Dictionaries.languages.getLanguageName(post.language());
        if (!ids.contains(idLanguage)) {
//            varList.add(var().isa("language").id(idLanguage)
//                    .value(Dictionaries.languages.getLanguageName(post.language())));
            varList.add(var().isa("language").id(idLanguage));
            ids.add(idLanguage);
        }
        varList.add(var().isa("written-in")
                .rel("post-with-language", var().id(idMessage))
                .rel("language-written", var().id(idLanguage)));

        writeToFile(varList, "# post-" + post.messageId());
    }

    protected void serialize(final Comment comment) {

        List<Var> varList = new ArrayList<>();
        String idMessage = "message-" + comment.messageId();
//        System.out.println("SERIALISING COMMENT ===> " + idMessage);

        varList.add(var().isa("comment").id(idMessage)
                .has("creation-date", Dictionaries.dates.formatDateTime(comment.creationDate()))
                .has("browser-used", Dictionaries.browsers.getName(comment.browserId()))
                .has("location-ip", comment.ipAddress().toString())
                .has("content", comment.content().replace("\n", " "))
                .has("length", Integer.toString(comment.content().length())));

        String idComment = "message-" + Long.toString(comment.messageId());
        varList.add(var().isa("reply")
                .rel("reply-content", var().id(idComment))
                .rel("reply-owner", var().id("message-" + Long.toString(comment.replyOf()))));

        varList.add(var().isa("resides")
                .rel("located-subject", var().id(idComment))
                .rel("subject-location", var().id("country-" + comment.countryId())));

        varList.add(var().isa("writes")
                .rel("written", var().id(idComment))
                .rel("writer", var().id("person-" + comment.author().accountId())));

        varList.addAll(comment.tags().stream().map(t -> var().isa("tagging")
                .rel("tagged-subject", var().id(idComment))
                .rel("subject-tag", var().id("tag-" + Integer.toString(t)))).collect(Collectors.toList()));

        writeToFile(varList, "# comment-" + comment.messageId());
    }

    protected void serialize(final Photo photo) {

    }

    protected void serialize(final ForumMembership membership) {
        Var var = var().isa("membership")
                .rel("membered-forum", var().id("forum-" + Long.toString(membership.forumId())))
                .rel("forum-member", var().id("person-" + Long.toString(membership.person().accountId())))
                .has("creation-date", Dictionaries.dates.formatDateTime(membership.creationDate()));

        writeToFile(Collections.singletonList(var), "# forum membership");
    }

    protected void serialize(final Like like) {
        if (like.type == Like.LikeType.POST || like.type == Like.LikeType.COMMENT) {

            Var var = var().isa("likes")
                    .rel("liked", var().id("message-" + Long.toString(like.messageId)))
                    .rel("liker", var().id("person-" + Long.toString(like.user)))
                    .has("creation-date", Dictionaries.dates.formatDateTime(like.date));

            writeToFile(Collections.singletonList(var), "# like");
        }
    }

    public void reset() {

    }

    private void writeToFile(List<Var> varList, String info) {
        try {
            String graqlString = Graql.insert(varList).toString();
//            graqlString = graqlString.replace("; ", ";\n");
            bufferedWriter.write(info + "\n");
            bufferedWriter.write(graqlString.substring(7) + "\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
