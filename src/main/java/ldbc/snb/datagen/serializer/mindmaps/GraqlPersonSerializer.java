package ldbc.snb.datagen.serializer.mindmaps;

import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.Var;

import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedWriter;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.mindmaps.graql.api.query.QueryBuilder.var;

public class GraqlPersonSerializer extends PersonSerializer {

    final static String POST_TRANSACTION_REQUEST_URL = "http://10.0.1.9:8080/transaction";
    final static String filePath = "./ldbc-snb-data.gql";
    final static int batchSize = 10;
    final static int sleep = 2500;

    private static Set<String> ids = new HashSet<>();

    private List<Var> varList;
    private QueryBuilder queryBuilder;
    private int serializedPersons;

    private BufferedWriter bufferedWriter;

    long startTime;
    long endTime;

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

    private void personWithResource(String varNamePerson, String isa,
                                    String resourceValue) {
        String varNameResource = varNamePerson + "_" + isa + "_" + resourceValue.hashCode();

        varList.add(var(varNamePerson).id(varNamePerson));
        varList.add(var(varNameResource).isa(isa).value(resourceValue));
        varList.add(var().isa("person-has-resource")
                .rel("person-target", var(varNamePerson))
                .rel("person-value", var(varNameResource)));
    }

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {

        serializedPersons = 0;
        queryBuilder = QueryBuilder.build();
        startTime = System.currentTimeMillis();
        varList = new ArrayList<>();

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(filePath, false));
            bufferedWriter.write("insert\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void close() {
        endTime = System.currentTimeMillis();
        try {
            String graqlString = queryBuilder.insert(varList).toString();
            sendToEngine(graqlString, sleep);

            graqlString = graqlString.replaceAll("; ", ";\n");
            bufferedWriter.write(graqlString.substring(7));
            bufferedWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("========================  TIME USED ========================");
        System.out.println(endTime - startTime + " ms");
    }


    @Override
    protected void serialize(Person p) {

        System.out.println("====== ABOUT TO SERIALISE A NEW PERSON! ===== NUMBER: " + serializedPersons);

        serializedPersons++;
        if (serializedPersons % batchSize == 0) {
            try {
                bufferedWriter.write("\n# a new batch\n");

                String graqlString = queryBuilder.insert(varList).toString();
                sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriter.write(graqlString.substring(7));
                varList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String gender = (p.gender() == 4) ? "male" : "female";
        String birthdayDateString = Dictionaries.dates.formatDate(p.birthDay());
        String creationDateString = Dictionaries.dates.formatDateTime(p.creationDate());

        //Let's use the Graql power!!!!!!

        String varNamePerson = "person-" + Long.toString(p.accountId());
        if (!ids.contains(varNamePerson)) {
            varList.add(var(varNamePerson).isa("person").id(varNamePerson).value(p.firstName() + " " + p.lastName()));
//            ids.add(varNamePerson);
        } else {
            varList.add(var(varNamePerson).id(varNamePerson).value(p.firstName() + " " + p.lastName()));
        }

        personWithResource(varNamePerson, "first-name", p.firstName());
        personWithResource(varNamePerson, "last-name", p.lastName());
        personWithResource(varNamePerson, "gender", gender);
        personWithResource(varNamePerson, "browser-used", Dictionaries.browsers.getName(p.browserId()));
        personWithResource(varNamePerson, "creation-date", creationDateString);
        personWithResource(varNamePerson, "birthday", birthdayDateString);
        personWithResource(varNamePerson, "location-ip", p.ipAddress().toString());


        //Resource relationships for all the languages a person is capable of speaking
        ArrayList<Integer> languages = p.languages();
        for (Integer language : languages) {
            personWithResource(varNamePerson, "language", Dictionaries.languages.getLanguageName(language));
        }

        //Resource relationships for all the email addresses associated to the current person
        for (String email : p.emails()) {
            personWithResource(varNamePerson, "email", email);
        }

        //Relationship for current city
        String varNameCity = "city-" + p.cityId();
        if (!ids.contains(varNameCity)) {
            varList.add(var(varNameCity).isa("city").id(varNameCity));
//            ids.add(varNameCity);
        } else {
            varList.add(var(varNameCity).id(varNameCity));
        }
        varList.add(var().isa("located-in")
                .rel("subject-with-location", var(varNamePerson))
                .rel("location-of-subject", var(varNameCity)));

        //Resource relationships for the current person's interests
        for (Integer interest : p.interests()) {
            String varNameTag = "tag-" + interest;
            if (!ids.contains(varNameTag)) {
                varList.add(var(varNameTag).isa("tag").id("tag-" + interest));
//                ids.add(varNameTag);
            } else {
                varList.add(var(varNameTag).id("tag-" + interest));
            }
            varList.add(var().isa("with-tag")
                    .rel("subject-with-tag", var(varNamePerson))
                    .rel("tag-of-subject", var(varNameTag)));
        }

        System.out.println("====== DONE SERIALISING THE CURRENT PERSON ======");
    }

    @Override
    protected void serialize(final StudyAt studyAt) {
        String dateString = Dictionaries.dates.formatYear(studyAt.year);

        String varNamePerson = "person-" + Long.toString(studyAt.user);
        varList.add(var(varNamePerson).id(varNamePerson));

        String varNameUniversity = "university-" + Long.toString(studyAt.university);
        if (!ids.contains(varNameUniversity)) {
            varList.add(var(varNameUniversity).isa("university").id(varNameUniversity));
//            ids.add(varNameUniversity);
        } else {
            varList.add(var(varNameUniversity).id(varNameUniversity));
        }

        String varNameRelation = "study-at_" + varNamePerson + "_" + varNameUniversity;
        varList.add(var(varNameRelation).isa("study-at")
                .rel("student", var(varNamePerson))
                .rel("university-with-student", var(varNameUniversity)));

        String varNameResource = varNameRelation + "_class-year_" + dateString.hashCode();
        varList.add(var(varNameResource).isa("class-year").value(dateString));

        varList.add(var().isa("study-at-has-resource")
                .rel("study-at-target", var(varNameRelation))
                .rel("study-at-value", var(varNameResource)));
    }

    @Override
    protected void serialize(final WorkAt workAt) {
        String dateString = Dictionaries.dates.formatYear(workAt.year);

        String varNamePerson = "person-" + Long.toString(workAt.user);
        varList.add(var(varNamePerson).id(varNamePerson));

        String varNameCompany = "company-" + Long.toString(workAt.company);
        if (!ids.contains(varNameCompany)) {
            varList.add(var(varNameCompany).isa("company").id(varNameCompany));
//            ids.add(varNameCompany);
        } else {
            varList.add(var(varNameCompany).id(varNameCompany));
        }

        String varNameRelation = "work-at_" + varNamePerson + "_" + varNameCompany;
        varList.add(var(varNameRelation).isa("work-at")
                .rel("employee", var(varNamePerson))
                .rel("company-with-employee", var(varNameCompany)));

        String varNameResource = varNameRelation + "_work-from_" + dateString.hashCode();
        varList.add(var(varNameResource).isa("work-from").value(dateString));

        varList.add(var().isa("work-at-has-resource")
                .rel("work-at-target", var(varNameRelation))
                .rel("work-at-value", var(varNameResource)));
    }

    @Override
    protected void serialize(final Person p, Knows knows) {
        //Person knows person relationship
        String dateString = Dictionaries.dates.formatDateTime(knows.creationDate());

        String acquaintanceVarName1 = "person-" + Long.toString(p.accountId());
        varList.add(var(acquaintanceVarName1).id(acquaintanceVarName1));

        String acquaintanceVarName2 = "person-" + Long.toString(knows.to().accountId());
        if (!ids.contains(acquaintanceVarName2)) {
            varList.add(var(acquaintanceVarName2).isa("person").id(acquaintanceVarName2));
//            ids.add(acquaintanceVarName2);
        } else {
            varList.add(var(acquaintanceVarName2).id(acquaintanceVarName2));
        }

        String varNameRelation = "knows_" + acquaintanceVarName1 + "_" + acquaintanceVarName2;
        varList.add(var(varNameRelation).isa("knows")
                .rel("acquaintance1", var(acquaintanceVarName1))
                .rel("acquaintance2", var(acquaintanceVarName2)));

        String varNameResource = varNameRelation + "_creation-date_" + dateString.hashCode();
        varList.add(var(varNameResource).isa("creation-date").value(dateString));

        varList.add(var().isa("relation-has-resource")
                .rel("relation-target", var(varNameRelation))
                .rel("relation-value", var(varNameResource)));
    }
}
