package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.graql.InsertQuery;
import ai.grakn.graql.Var;
import ai.grakn.util.REST;
import ldbc.snb.datagen.objects.Organization;
import ldbc.snb.datagen.objects.Place;
import ldbc.snb.datagen.objects.Tag;
import ldbc.snb.datagen.objects.TagClass;
import ldbc.snb.datagen.serializer.InvariantSerializer;
import mjson.Json;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;

import javax.xml.ws.http.HTTPException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.stream.Collectors;

import static ai.grakn.graql.Graql.insert;
import static ai.grakn.graql.Graql.var;
import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.TASK_LOADER_INSERTS;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.WebPath.TASKS_URI;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;

public class GraknInvariantSerializer extends InvariantSerializer {

    final String keyspace = "SNB";

    public void initialize(Configuration conf, int reducerId) {
    }

    public void close() {
    }

    protected void serialize(final Place place) {
    }

    protected void serialize(final Organization organization) {
    }

    protected void serialize(final TagClass tagClass) {
    }

    protected void serialize(final Tag tag) {
        System.out.println("serializing tag: "+tag.name);
        Var tagConcept = var().isa("tag");
        tagConcept.has("snb-id", String.valueOf(tag.id));
        tagConcept.has("name", String.valueOf(tag.name));

        sendQueriesToLoader(Arrays.asList(insert(tagConcept)));
    }

    public void sendQueriesToLoader(Collection<InsertQuery> queries) {

        HttpURLConnection currentConn = getHost("http://localhost:4567/tasks/schedule?"+getPostParams());
        String response = executePost(currentConn, getConfiguration(queries));

        int responseCode = getResponseCode(currentConn);
        if (responseCode != REST.HttpConn.OK) {
            throw new HTTPException(responseCode);
        }

//        System.out.println(Json.read(response).at("id").asString());
    }

    private HttpURLConnection getHost(String host) {
        HttpURLConnection urlConn = null;
        try {
            urlConn = (HttpURLConnection) new URL(host).openConnection();
            urlConn.setDoOutput(true);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return urlConn;
    }

    private String executePost(HttpURLConnection connection, String body){

        try {
            // create post
            connection.setRequestMethod(REST.HttpConn.POST_METHOD);
            connection.addRequestProperty(REST.HttpConn.CONTENT_TYPE, REST.HttpConn.APPLICATION_POST_TYPE);

            // add body and execute
            connection.setRequestProperty(REST.HttpConn.CONTENT_LENGTH, Integer.toString(body.length()));
            connection.getOutputStream().write(body.getBytes(REST.HttpConn.UTF8));
            connection.getOutputStream().flush();

            // get response
//            return IOUtils.toString(connection.getInputStream());
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            connection.disconnect();
        }

        return null;
    }

    /**
     * Transform queries into Json configuration needed by the Loader task
     * @param queries queries to include in configuration
     * @return configuration for the loader task
     */
    private String getConfiguration(Collection<InsertQuery> queries){
        return Json.object()
                .set(KEYSPACE_PARAM, keyspace)
                .set(TASK_LOADER_INSERTS, queries.stream().map(InsertQuery::toString).collect(Collectors.toList()))
                .toString();
    }

    /**
     * Get the response code from an HTTP Connection safely
     * @param connection to get response code from
     * @return response code
     */
    private int getResponseCode(HttpURLConnection connection){
        try {
            return connection.getResponseCode();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    private String getPostParams(){
        return TASK_CLASS_NAME_PARAMETER + "=" + "ai.grakn.engine.loader.LoaderTask" + "&" +
                TASK_RUN_AT_PARAMETER + "=" + new Date().getTime() + "&" +
                TASK_CREATOR_PARAMETER + "=" + "me";
    }

    public void reset() {

    }
}
