package ldbc.snb.datagen.serializer.mindmaps;


import io.mindmaps.graql.Graql;
import io.mindmaps.graql.Var;
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

import static io.mindmaps.graql.Graql.var;


public class GraqlInvariantSerializer extends InvariantSerializer {

    private BufferedWriter bufferedWriter;

    private long startTime;

    @Override
    public void reset() {

    }

    @Override
    public void initialize(Configuration conf, int reducerId) {
        System.out.println();
        System.out.println("========================   PERSON INVARIANT    ========================");
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
        System.out.println();
        System.out.println("========================  TIME USED INVARIANT  ========================");
        System.out.println(endTime - startTime + " ms");
        System.out.println("=======================================================================");
    }

    @Override
    protected void serialize(Place place) {
        List<Var> varList = new ArrayList<>();
        String idPlace = place.getType() + "-" + place.getId();
//        System.out.println("SERIALISING PLACE ===> " + idPlace);
        varList.add(var().isa(place.getType()).id(idPlace)
//                .value(place.getName())
                .has("name", place.getName()));

        if (place.getType().equals(Place.CITY)) {
            String idPlace2 = Place.COUNTRY + "-" + Dictionaries.places.belongsTo(place.getId());
            varList.add(var().isa("sublocate")
                    .rel("member-location", var().id(idPlace))
                    .rel("container-location", var().id(idPlace2)));

        } else if (place.getType().equals(Place.COUNTRY)) {
            String idPlace2 = Place.CONTINENT + "-" + Dictionaries.places.belongsTo(place.getId());
            varList.add(var().isa("sublocate")
                    .rel("member-location", var().id(idPlace))
                    .rel("container-location", var().id(idPlace2)));
        }

        writeToFile(varList, "# " + idPlace);
    }

    @Override
    protected void serialize(Organization organization) {
        List<Var> varList = new ArrayList<>();
        String idOrganization = organization.type.toString() + "-" + organization.id;
//        System.out.println("SERIALISING ORGANIZATION ===> " + idOrganization);
        varList.add(var().isa(organization.type.toString())
                .id(idOrganization)
//                .value(organization.name)
                .has("name", organization.name));

        if (organization.type.toString().equals("university")) {
            String idPlace = "city-" + organization.location;

            varList.add(var().isa("resides")
                    .rel("located-subject", var().id(idOrganization))
                    .rel("subject-location", var().id(idPlace)));

        } else if (organization.type.toString().equals("company")) {
            String idPlace = "country-" + organization.location;

            varList.add(var().isa("resides")
                    .rel("located-subject", var().id(idOrganization))
                    .rel("subject-location", var().id(idPlace)));
        }

        writeToFile(varList, "# " + idOrganization);
    }

    @Override
    protected void serialize(TagClass tagClass) {
        String idCategory = "category-" + tagClass.id;
//        System.out.println("SERIALISING TAG CATEGORY ===> " + idCategory);
        List<Var> varList = new ArrayList<>();

        varList.add(var().isa("category").id(idCategory)
//                .value(tagClass.name)
                .has("name", tagClass.name));

        if (tagClass.parent != -1) {
            String idCategoryParent = "category-" + tagClass.parent;
            varList.add(var().isa("subgrouping")
                    .rel("supergroup", var().id(idCategoryParent))
                    .rel("subgroup", var().id(idCategory)));
        }

        writeToFile(varList, "# " + idCategory);
    }

    @Override
    protected void serialize(Tag tag) {
        String idTag = "tag-" + tag.id;
//        System.out.println("SERIALISING TAG ===> " + idTag);
        List<Var> varList = new ArrayList<>();

        varList.add(var().isa("tag").id(idTag)
//                .value(tag.name)
                .has("name", tag.name));

        String idCategory = "category-" + tag.tagClass;
        varList.add(var().isa("grouping")
                .rel("tag-group", var().id(idCategory))
                .rel("grouped-tag", var().id(idTag)));

        writeToFile(varList, "# " + idTag);
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
