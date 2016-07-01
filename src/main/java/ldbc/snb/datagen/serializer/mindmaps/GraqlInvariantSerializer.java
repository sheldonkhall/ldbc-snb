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
import static ldbc.snb.datagen.serializer.mindmaps.GraqlPersonSerializer.formatString;
import static ldbc.snb.datagen.serializer.mindmaps.GraqlPersonSerializer.formatVar;

public class GraqlInvariantSerializer extends InvariantSerializer {

    final static String filePath = "./ldbc-snb-data-invariant.gql";
    final static String filePathCategory = "./ldbc-snb-data-invariant-tag-class.gql";
    final static String filePathTag = "./ldbc-snb-data-invariant-tag.gql";
    final static int batchSize = 40;
    final static int sleep = 200;


    private List<Var> varList;
    private QueryBuilder queryBuilder;
    private int serializedEntities;

    private BufferedWriter bufferedWriterAll;
    private BufferedWriter bufferedWriterCategory;
    private BufferedWriter bufferedWriterTag;

    long startTime;
    long endTime;

    private Var var;

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
            bufferedWriterAll = new BufferedWriter(new FileWriter(filePath, true));

            bufferedWriterCategory = new BufferedWriter(new FileWriter(filePathCategory));
            bufferedWriterCategory.write("insert \n");

            bufferedWriterTag = new BufferedWriter(new FileWriter(filePathTag));
            bufferedWriterTag.write("insert \n");

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
            bufferedWriterAll.write(graqlString.substring(7) + "\n");

            bufferedWriterAll.write("\n");
            bufferedWriterAll.close();

            bufferedWriterCategory.close();
            bufferedWriterTag.close();

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
                String graqlString = queryBuilder.insert(varList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriterAll.write(graqlString.substring(7) + "\n");
                varList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        String varNamePlace = place.getType() + "-" + place.getId();
        System.out.println("SERIALISING PLACE ===> " + varNamePlace);
        varList.add(var(varNamePlace).isa(place.getType()).id(varNamePlace).value(place.getName()));

        String varNameResource = place.getType() + "_name_" + place.getName().hashCode();
        varList.add(var(varNameResource).isa("name").value(place.getName()));

        varList.add(var().isa("entity-resource")
                .rel("entity-resource-value", var(varNameResource))
                .rel("entity-resource-owner", var(varNamePlace)));

        // relation sublocate
        if (place.getType().equals(Place.CITY)) {
            String varNameLocation2 = Place.COUNTRY + "-" + Dictionaries.places.belongsTo(place.getId());
            varList.add(var(varNameLocation2).isa(Place.COUNTRY).id(varNameLocation2));

            varList.add(var().isa("sublocate")
                    .rel("member-location", var(varNamePlace))
                    .rel("container-location", var(varNameLocation2)));

        } else if (place.getType().equals(Place.COUNTRY)) {
            String varNameLocation2 = Place.CONTINENT + "-" + Dictionaries.places.belongsTo(place.getId());
            varList.add(var(varNameLocation2).isa(Place.CONTINENT).id(varNameLocation2));

            varList.add(var().isa("sublocate")
                    .rel("member-location", var(varNamePlace))
                    .rel("container-location", var(varNameLocation2)));
        }
    }

    @Override
    protected void serialize(Organization organization) {

        serializedEntities++;
        if (serializedEntities % batchSize == 0) {
            try {
                bufferedWriterAll.write("\n# a new batch\n");

                String graqlString = queryBuilder.insert(varList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriterAll.write(graqlString.substring(7) + "\n");
                varList = new ArrayList<>();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        String varNameOrganization = organization.type.toString() + "-" + organization.id;
        System.out.println("SERIALISING ORGANIZATION ===> " + varNameOrganization);
        varList.add(var(varNameOrganization).isa(organization.type.toString())
                .id(varNameOrganization).value(organization.name));

        String varNameResource = varNameOrganization + "_name_" + organization.name.hashCode();
        varList.add(var(varNameResource).isa("name").value(organization.name));

        varList.add(var().isa("entity-resource")
                .rel("entity-resource-value", var(varNameResource))
                .rel("entity-resource-owner", var(varNameOrganization)));

        // relation resides
        if (organization.type.toString().equals("university")) {
            String varNamePlace = "city-" + organization.location;
            varList.add(var(varNamePlace).isa("city").id(varNamePlace));

            varList.add(var().isa("resides")
                    .rel("located-subject", var(varNameOrganization))
                    .rel("subject-location", var(varNamePlace)));

        } else if (organization.type.toString().equals("company")) {
            String varNamePlace = "country-" + organization.location;
            varList.add(var(varNamePlace).isa("country").id(varNamePlace));

            varList.add(var().isa("resides")
                    .rel("located-subject", var(varNameOrganization))
                    .rel("subject-location", var(varNamePlace)));
        }
    }

    @Override
    protected void serialize(TagClass tagClass) {
        try {
            serializedEntities++;
            if (serializedEntities % batchSize == 0) {

                String graqlString = queryBuilder.insert(varList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = formatString(graqlString);
                bufferedWriterAll.write(graqlString);
                varList = new ArrayList<>();
            }

            String varNameCategory = "category-" + tagClass.id;
            System.out.println("SERIALISING TAG CATEGORY ===> " + varNameCategory);

            var = var(varNameCategory).isa("category").id(varNameCategory).value(tagClass.name);
            varList.add(var);
            bufferedWriterCategory.write(formatVar(var));

            String varNameResource = varNameCategory + "_name_" + tagClass.name.hashCode();
            var = var(varNameResource).isa("name").value(tagClass.name);
            varList.add(var);
            bufferedWriterCategory.write(formatVar(var));

            var = var().isa("entity-resource")
                    .rel("entity-resource-value", var(varNameResource))
                    .rel("entity-resource-owner", var(varNameCategory));
            varList.add(var);
            bufferedWriterCategory.write(formatVar(var));

            // relation grouping
            if (tagClass.parent != -1) {
                String varNameCategorySuper = "category-" + tagClass.parent;
                var = var(varNameCategorySuper).isa("category").id(varNameCategorySuper);
                varList.add(var);
                bufferedWriterCategory.write(formatVar(var));

                var = var().isa("subgrouping")
                        .rel("supergroup", var(varNameCategorySuper))
                        .rel("subgroup", varNameCategory);
                varList.add(var);
                bufferedWriterCategory.write(formatVar(var));
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void serialize(Tag tag) {
        try {
            serializedEntities++;
            if (serializedEntities % batchSize == 0) {

                String graqlString = queryBuilder.insert(varList).toString();
                GraqlPersonSerializer.sendToEngine(graqlString, sleep);

                graqlString = graqlString.replaceAll("; ", ";\n");
                bufferedWriterAll.write(graqlString.substring(7) + "\n");
                varList = new ArrayList<>();
            }

            String varNameTag = "tag-" + tag.id;
            System.out.println("SERIALISING TAG ===> " + varNameTag);
            var = var(varNameTag).isa("tag").id(varNameTag).value(tag.name);
            varList.add(var);
            bufferedWriterTag.write(formatVar(var));

            String varNameResource = varNameTag + "_name_" + tag.name.hashCode();
            var = var(varNameResource).isa("name").value(tag.name);
            varList.add(var);
            bufferedWriterTag.write(formatVar(var));

            var = var().isa("entity-resource")
                    .rel("entity-resource-value", var(varNameResource))
                    .rel("entity-resource-owner", var(varNameTag));
            varList.add(var);
            bufferedWriterTag.write(formatVar(var));

            String varNameCategory = "category-" + tag.tagClass;
            var = var(varNameCategory).isa("category").id(varNameCategory);
            varList.add(var);
            bufferedWriterTag.write(formatVar(var));

            var = var().isa("grouping")
                    .rel("tag-group", var(varNameCategory))
                    .rel("grouped-tag", var(varNameTag));
            varList.add(var);
            bufferedWriterTag.write(formatVar(var));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
