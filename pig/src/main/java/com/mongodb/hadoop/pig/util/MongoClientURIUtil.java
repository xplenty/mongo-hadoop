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

    public static boolean containsReplicaSetMembers(String locationPath) {
        try {
            URIBuilder uriBuilder = new URIBuilder(locationPath);
			List<NameValuePair> queryParameters = uriBuilder.getQueryParams();
			for (Iterator<NameValuePair> queryParameterItr = queryParameters.iterator(); queryParameterItr.hasNext();) {
                NameValuePair queryParameter = queryParameterItr.next();
                if (queryParameter.getName().equals("replica_set_members")) {
					return true;
                }
            }

            return false;
        } catch (URISyntaxException e) {
            logger.error("Unable to parse uri", e);
        }

        return false;
    }
    
}