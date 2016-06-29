package ldbc.snb.datagen.serializer.mindmaps;


import io.mindmaps.graql.api.query.QueryBuilder;
import io.mindmaps.graql.api.query.Var;
import ldbc.snb.datagen.dictionary.Dictionaries;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static io.mindmaps.graql.api.query.QueryBuilder.var;

public class GraqlInvariantSerializer extends InvariantSerializer {

    final static String filePath = "./ldbc-snb-data.gql";
    final static int batchSize = 40;
    final static int sleep = 200;


    private List<Var> varList;
    private QueryBuilder queryBuilder;
    private int serializedEntities;

    private BufferedWriter bufferedWriter;

    long startTime;
    long endTime;

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {
        serializedEntities = 0;
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
            GraqlPersonSerializer.sendToEngine(graqlString, sleep);

            graqlString = graqlString.replaceAll("; ", ";\n");
            bufferedWriter.write(graqlString.substring(7));
            bufferedWriter.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("========================  TIME USED INVARIANT  ========================");
        System.out.println(endTime - startTime + " ms");
    }

    @Override
    protected void serialize(Place place) {
        serializedEntities++;
        if (serializedEntities % batchSize == 0) {
            try {
                bufferedWriter.write("\n# a new batch\n");

                String graqlString = queryBuilder.insert(varList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriter.write(graqlString.substring(7));
                varList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        String varNamePlace = place.getType() + "-" + place.getId();
        System.out.println("SERIALISING PLACE ===> "+ varNamePlace);
        varList.add(var(varNamePlace).isa(place.getType()).id(varNamePlace).value(place.getName()));

        String varNameResource = place.getType() + "_name_" + place.getName().hashCode();
        varList.add(var(varNameResource).isa("name").value(place.getName()));

        varList.add(var().isa("entity-has-resource")
                .rel("entity-value", var(varNameResource))
                .rel("entity-target", var(varNamePlace)));

        // relation is-part-of
        if (place.getType().equals(Place.CITY)) {
            String varNameLocation2 = Place.COUNTRY + "-" + Dictionaries.places.belongsTo(place.getId());
            varList.add(var(varNameLocation2).isa(Place.COUNTRY).id(varNameLocation2));

            varList.add(var().isa("is-part-of")
                    .rel("location1", var(varNamePlace))
                    .rel("location2", var(varNameLocation2)));

        } else if (place.getType().equals(Place.COUNTRY)) {
            String varNameLocation2 = Place.CONTINENT + "-" + Dictionaries.places.belongsTo(place.getId());
            varList.add(var(varNameLocation2).isa(Place.CONTINENT).id(varNameLocation2));

            varList.add(var().isa("is-part-of")
                    .rel("location1", var(varNamePlace))
                    .rel("location2", var(varNameLocation2)));
        }
    }

    @Override
    protected void serialize(Organization organization) {
        serializedEntities++;
        if (serializedEntities % batchSize == 0) {
            try {
                bufferedWriter.write("\n# a new batch\n");

                String graqlString = queryBuilder.insert(varList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriter.write(graqlString.substring(7));
                varList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String varNameOrganization = organization.type.toString() + organization.id;
        System.out.println("SERIALISING ORGANIZATION ===> "+ varNameOrganization);
        varList.add(var(varNameOrganization).isa(organization.type.toString())
                .id(varNameOrganization).value(organization.name));

        String varNameResource = varNameOrganization + "_name_" + organization.name.hashCode();
        varList.add(var(varNameResource).isa("name").value(organization.name));

        varList.add(var().isa("entity-has-resource")
                .rel("entity-value", var(varNameResource))
                .rel("entity-target", var(varNameOrganization)));

        // relation location-of-subject
        if (organization.type.toString().equals("university")) {
            String varNamePlace = "city-" + organization.location;
            varList.add(var(varNamePlace).isa("city").id(varNamePlace));

            varList.add(var().isa("located-in")
                    .rel("subject-with-location", var(varNameOrganization))
                    .rel("location-of-subject", var(varNamePlace)));

        } else if (organization.type.toString().equals("company")) {
            String varNamePlace = "country-" + organization.location;
            varList.add(var(varNamePlace).isa("country").id(varNamePlace));

            varList.add(var().isa("located-in")
                    .rel("subject-with-location", var(varNameOrganization))
                    .rel("location-of-subject", var(varNamePlace)));
        }
    }

    @Override
    protected void serialize(TagClass tagClass) {
        serializedEntities++;
        if (serializedEntities % batchSize == 0) {
            try {
                bufferedWriter.write("\n# a new batch\n");

                String graqlString = queryBuilder.insert(varList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriter.write(graqlString.substring(7));
                varList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String varNameTagClass = "tag-class-" + tagClass.id;
        System.out.println("SERIALISING TAG CLASS ===> "+ varNameTagClass);
        varList.add(var(varNameTagClass).isa("tag-class").id(varNameTagClass)
                .value(tagClass.name));

        String varNameResource = varNameTagClass + "_name_" + tagClass.name.hashCode();
        varList.add(var(varNameResource).isa("name")
                .value(tagClass.name));

        varList.add(var().isa("entity-has-resource")
                .rel("entity-value", var(varNameResource))
                .rel("entity-target", var(varNameTagClass)));

        // relation subclass-of
        if (tagClass.parent != -1) {
            String varNameTagClassSuper = "tag-class-" + tagClass.parent;
            varList.add(var(varNameTagClassSuper).isa("tag-class").id(varNameTagClassSuper));
            varList.add(var().isa("subclass-of")
                    .rel("superclass", var(varNameTagClassSuper))
                    .rel("subclass", varNameTagClass));
        }
    }

    @Override
    protected void serialize(Tag tag) {
        serializedEntities++;
        if (serializedEntities % batchSize == 0) {
            try {
                bufferedWriter.write("\n# a new batch\n");

                String graqlString = queryBuilder.insert(varList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriter.write(graqlString.substring(7));
                varList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String varNameTag = "tag-" + tag.id;
        System.out.println("SERIALISING TAG ===> "+ varNameTag);
        varList.add(var(varNameTag).isa("tag").id(varNameTag)
                .value(tag.name));

        String varNameResource = varNameTag + "_name_" + tag.name.hashCode();
        varList.add(var(varNameResource).isa("name")
                .value(tag.name));

        varList.add(var().isa("entity-has-resource")
                .rel("entity-value", var(varNameResource))
                .rel("entity-target", var(varNameTag)));
    }
}
