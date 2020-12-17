package com.mongodb.hadoop.pig.util;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MongoClientURIUtil {
    private static final Logger logger = LoggerFactory.getLogger(MongoClientURIUtil.class.getName());

    /**
     * Mapping a query string into a map that contains a key and list of its values.
     * It is very likely that the same key appear more than once in the query string.
     * @param params a query string
     * @return map of query string from the url
     */
    private static Map<String, List<String>> getQueryMap(String params) {
        Map<String, List<String>> map = new HashMap<String, List<String>>();
        for (String param : params.split("&")) {
            String name = param.split("=")[0];  
            String value = param.split("=")[1]; 
            List<String> val = map.containsKey(name) ? 
                map.get(name) : new ArrayList<String>();
            val.add(value);
            map.put(name, val);
        }
        return map;
    }

    /**
     * Rebuild a URI to be compatible with current MongoDB JDBC.
     * Mapping all replica_set_members as a comma-separated host.
     * @param locationPath the value of location path
     * @return a new rebuilt URI
     */
    public static String rebuildUri(String locationPath) {
        try {
            URIBuilder uriBuilder = new URIBuilder(locationPath);
            
            // Removing replicaSetMembers from query parameters as
            // all replica set members will be put as a comma-separated 
            // hostnames in current MongoURI implementation
            List<String> replicaSetMembers = new ArrayList<String>();
            List<NameValuePair> queryParameters = uriBuilder.getQueryParams();
            for (Iterator<NameValuePair> queryParameterItr = queryParameters.iterator(); queryParameterItr.hasNext();) {
                NameValuePair queryParameter = queryParameterItr.next();
                if (queryParameter.getName().equals("replica_set_members")) {
                    replicaSetMembers.add(queryParameter.getValue());
                    queryParameterItr.remove();
                }
            }
            uriBuilder.setParameters(queryParameters);

            // Rebuild a new hostname from all comma-separated replica members
            uriBuilder.setHost(uriBuilder.getHost()+ ":" + uriBuilder.getPort() + "," + StringUtils.join(replicaSetMembers, ","));

            // Removing port number as existing port was appended on newly-built host
            uriBuilder.setPort(-1); 

            logger.info("Rebuilt URI: "+ uriBuilder.build().toString());

            return uriBuilder.build().toString();
            
        } catch (URISyntaxException e) {
            logger.error("Unable to parse uri", e);
        }

        return locationPath;
    }

    public static boolean containsReplicaSetMembers(String location) {
        try {
            URI uri = new URI(location);
            Map<String, List<String>> queryMap = getQueryMap(uri.getQuery());
            
            return queryMap.containsKey("replica_set_members"); 
        } catch (URISyntaxException e) {
            logger.error("Unable to parse uri", e);
        }

        return false;
    }
    
}