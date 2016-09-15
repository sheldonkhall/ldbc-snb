package ldbc.snb.datagen.serializer.mindmaps;

import io.mindmaps.graql.Graql;
import io.mindmaps.graql.Var;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Knows;
import ldbc.snb.datagen.objects.Person;
import ldbc.snb.datagen.objects.StudyAt;
import ldbc.snb.datagen.objects.WorkAt;
import ldbc.snb.datagen.serializer.PersonSerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static io.mindmaps.graql.Graql.var;


public class GraqlPersonSerializer extends PersonSerializer {

    final static String filePath = "./ldbc-snb-data.gql";

    private static Set<String> ids = new HashSet<>();

    private List<Var> varList;
    private int serializedPersons;

    private BufferedWriter bufferedWriter;

    private long startTime;

    private void personWithResource(String idPerson,
                                    String resourceType,
                                    String resourceValue) {
        varList.add(var().id(idPerson).has(resourceType, resourceValue));
    }

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {

        serializedPersons = 0;
        startTime = System.currentTimeMillis();

        try {
            bufferedWriter = new BufferedWriter(new FileWriter(filePath, false));
//            bufferedWriter.write("insert\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void close() {
        long endTime = System.currentTimeMillis();
        try {
            bufferedWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println();
        System.out.println("==========================  TIME USED PERSON  =========================");
        System.out.println(endTime - startTime + " ms");
        System.out.println("=======================================================================\n");
    }


    @Override
    protected void serialize(Person p) {

        System.out.println("======   ABOUT TO SERIALISE A NEW PERSON!  ===== NUMBER: " + serializedPersons);

        serializedPersons++;
        varList = new ArrayList<>();

        String idPerson = "person-" + Long.toString(p.accountId());
        if (!ids.contains(idPerson)) {
//            varList.add(var().isa("person").id(idPerson).value(p.firstName() + " " + p.lastName()));
            varList.add(var().isa("person").id(idPerson));
            ids.add(idPerson);
        } else {
//            varList.add(var().id(idPerson).value(p.firstName() + " " + p.lastName()));
            varList.add(var().id(idPerson));
        }

        String gender = (p.gender() == 1) ? "male" : "female";
        String birthdayDateString = Dictionaries.dates.formatDate(p.birthDay());
        String creationDateString = Dictionaries.dates.formatDateTime(p.creationDate());

        personWithResource(idPerson, "firstname", p.firstName());
        personWithResource(idPerson, "lastname", p.lastName());
        personWithResource(idPerson, "gender", gender);
        personWithResource(idPerson, "browser-used", Dictionaries.browsers.getName(p.browserId()));
        personWithResource(idPerson, "creation-date", creationDateString);
        personWithResource(idPerson, "birthday", birthdayDateString);
        personWithResource(idPerson, "location-ip", p.ipAddress().toString());

        long age = 2016 - Long.parseLong(birthdayDateString.substring(0, 4));
        varList.add(var().id(idPerson).has("age", age));

        //Resource relationships for all the languages a person is capable of speaking
        for (Integer language : p.languages()) {
            String idLanguage = "language-" + Dictionaries.languages.getLanguageName(language);
            if (!ids.contains(idLanguage)) {
//                varList.add(var().isa("language").id(idLanguage)
//                        .value(Dictionaries.languages.getLanguageName(language)));
                varList.add(var().isa("language").id(idLanguage));
                ids.add(idLanguage);
            }
            varList.add(var().isa("speaks")
                    .rel("speaker", var().id(idPerson))
                    .rel("language-spoken", var().id(idLanguage)));
        }

        //Resource relationships for all the email addresses associated to the current person
        for (String email : p.emails()) {
            personWithResource(idPerson, "email", email);
        }

        //Relationship for current city
        String idCity = "city-" + p.cityId();
        varList.add(var().isa("resides")
                .rel("located-subject", var().id(idPerson))
                .rel("subject-location", var().id(idCity)));

        //Resource relationships for the current person's interests
        for (Integer interest : p.interests()) {
            String idTag = "tag-" + interest;
            varList.add(var().isa("tagging")
                    .rel("tagged-subject", var().id(idPerson))
                    .rel("subject-tag", var().id(idTag)));
        }

        writeToFile();

        System.out.println("====== DONE SERIALISING THE CURRENT PERSON ======");
    }

    @Override
    protected void serialize(final StudyAt studyAt) {
        String dateString = Dictionaries.dates.formatYear(studyAt.year);
        String idPerson = "person-" + Long.toString(studyAt.user);
        String idUniversity = "university-" + Long.toString(studyAt.university);

        Var var = var().isa("attends")
                .rel("student", var().id(idPerson))
                .rel("enrolled-university", var().id(idUniversity))
                .has("class-year", dateString);
        writeToFile(var);
    }

    @Override
    protected void serialize(final WorkAt workAt) {
        String dateString = Dictionaries.dates.formatYear(workAt.year);
        String idPerson = "person-" + Long.toString(workAt.user);
        String idCompany = "company-" + Long.toString(workAt.company);

        Var var = var().isa("employment")
                .rel("employee", var().id(idPerson))
                .rel("employer", var().id(idCompany))
                .has("employment-startdate", dateString);
        writeToFile(var);
    }

    @Override
    protected void serialize(final Person p, Knows knows) {
        //Person knows person relationship
        String dateString = Dictionaries.dates.formatDateTime(knows.creationDate());
        String idPerson1 = "person-" + Long.toString(p.accountId());
        String idPerson2 = "person-" + Long.toString(knows.to().accountId());

        if (!ids.contains(idPerson2)) {
            writeToFile(var().isa("person").id(idPerson2));
            ids.add(idPerson2);
        }

        Var var = var().isa("knows")
                .rel("acquaintance1", var().id(idPerson1))
                .rel("acquaintance2", var().id(idPerson2))
                .has("creation-date", dateString);
        writeToFile(var);
    }

    private void writeToFile(Var var) {
        try {
            String graqlString = Graql.insert(var).toString();
//            graqlString = graqlString.replace("; ", ";\n");
            bufferedWriter.write(graqlString.substring(7) + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeToFile() {
        try {
            bufferedWriter.write("\n# person " + serializedPersons + "\n");
            String graqlString = Graql.insert(varList).toString();

//            graqlString = graqlString.replace("; ", ";\n");
            bufferedWriter.write(graqlString.substring(7) + "\n\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
