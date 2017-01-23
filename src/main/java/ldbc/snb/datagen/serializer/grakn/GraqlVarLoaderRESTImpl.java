package ldbc.snb.datagen.serializer.grakn;

import ai.grakn.graql.InsertQuery;
import ai.grakn.util.REST;
import mjson.Json;
import org.apache.commons.io.IOUtils;

import javax.xml.ws.http.HTTPException;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.Date;
import java.util.stream.Collectors;

import static ai.grakn.util.REST.Request.KEYSPACE_PARAM;
import static ai.grakn.util.REST.Request.TASK_LOADER_INSERTS;
import static ai.grakn.util.REST.Request.TASK_CLASS_NAME_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_CREATOR_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_RUN_AT_PARAMETER;
import static ai.grakn.util.REST.Request.TASK_STATUS_PARAMETER;
import static ai.grakn.util.REST.WebPath.TASKS_URI;
import static ai.grakn.util.REST.WebPath.TASKS_SCHEDULE_URI;

/**
 * Uses the REST endpoint of Grakn engine to execute the queries.
 */
public class GraqlVarLoaderRESTImpl implements GraqlVarLoader {

    String keyspace;

    /**
     * Constructor that assumes Grakn is listening on the default address.
     *
     * @param keyspace the keyspace to execute the queries against.
     */
    public GraqlVarLoaderRESTImpl(String keyspace) {
        this.keyspace = keyspace;
    }

    @Override
    public void sendQueries(Collection<InsertQuery> queries) {
        HttpURLConnection currentConn = getHost("http://localhost:4567/tasks/schedule?"+getPostParams());
        String response = executePost(currentConn, getConfiguration(queries));

        int responseCode = getResponseCode(currentConn);
        if (responseCode != REST.HttpConn.OK) {
            throw new HTTPException(responseCode);
        }

        System.out.println(Json.read(response).at("id").asString());
    }

    /**
     * Instantiate the connection object and handle the error.
     *
     * @param host the engine URL plus required REST parameters.
     * @return the connection.
     */
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

    /**
     * POST the query to the REST API and return the response.
     *
     * @param connection the connection to send the request across.
     * @param body the POST body.
     * @return the response from the REST controller.
     */
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
            return IOUtils.toString(connection.getInputStream());
        } catch (IOException e){
            e.printStackTrace();
        } finally {
            connection.disconnect();
        }

        return null;
    }

    /**
     * Transform queries into Json configuration needed by Grakn engine.
     *
     * @param queries queries to include in configuration
     * @return configuration for Grakn engine
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

    /**
     * Generate the required default parameters to use the Grakn engine REST API.
     *
     * @return the parameters.
     */
    private String getPostParams(){
        return TASK_CLASS_NAME_PARAMETER + "=" + "ai.grakn.engine.loader.LoaderTask" + "&" +
                TASK_RUN_AT_PARAMETER + "=" + new Date().getTime() + "&" +
                TASK_CREATOR_PARAMETER + "=" + "me";
    }
}
