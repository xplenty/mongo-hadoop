/*
 * Copyright 2010-2013 10gen Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.hadoop.util;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import com.mongodb.MongoClientURI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Manages SSH tunnels for MongoDB connections using public key authentication.
 * 
 * <p>This class provides support for creating SSH tunnels to MongoDB servers,
 * including support for MongoDB SRV records. It is designed for scenarios where:
 * <ul>
 *   <li>Your Hadoop cluster nodes have SSH private keys</li>
 *   <li>The customer's bastion host has the corresponding public keys in ~/.ssh/authorized_keys</li>
 *   <li>MongoDB is only accessible from the bastion host</li>
 * </ul>
 * 
 * <p><b>Authentication Model:</b>
 * <pre>
 * Hadoop Node (has private key) --SSH--> Bastion Host (has public key) ---> MongoDB
 * </pre>
 * 
 * <p><b>Key Discovery:</b> If no private key path is specified, the manager will
 * automatically search for keys in standard locations:
 * <ul>
 *   <li>~/.ssh/id_rsa</li>
 *   <li>~/.ssh/id_ed25519</li>
 *   <li>~/.ssh/id_ecdsa</li>
 *   <li>~/.ssh/id_dsa</li>
 * </ul>
 */
public class SSHTunnelManager {
    private static final Log LOG = LogFactory.getLog(SSHTunnelManager.class);
    
    /**
     * Standard SSH private key file names to search for (in order of preference)
     */
    private static final String[] DEFAULT_KEY_FILES = {
        "id_rsa",
        "id_ed25519", 
        "id_ecdsa",
        "id_dsa"
    };
    
    private static final AtomicInteger LOCAL_PORT_COUNTER = new AtomicInteger(27018);
    private static final Map<String, TunnelInfo> ACTIVE_TUNNELS = new ConcurrentHashMap<String, TunnelInfo>();
    private static final Map<String, Session> SSH_SESSIONS = new ConcurrentHashMap<String, Session>();
    
    // Pattern to extract host and port from MongoDB connection strings
    private static final Pattern HOST_PORT_PATTERN = Pattern.compile("([^:,]+)(?::(\\d+))?");
    
    /**
     * Information about an SSH tunnel
     */
    private static class TunnelInfo {
        final String originalHost;
        final int originalPort;
        final int localPort;
        final String tunnelKey;
        
        TunnelInfo(String originalHost, int originalPort, int localPort, String tunnelKey) {
            this.originalHost = originalHost;
            this.originalPort = originalPort;
            this.localPort = localPort;
            this.tunnelKey = tunnelKey;
        }
    }
    
    /**
     * SSH configuration holder for public key authentication.
     * 
     * <p>In the typical enterprise setup:
     * <ul>
     *   <li>Your Hadoop cluster's public key is stored on the customer's bastion host</li>
     *   <li>The private key remains on your Hadoop cluster nodes (typically ~/.ssh/id_rsa or ~/.ssh/id_ed25519)</li>
     *   <li>This config specifies where to find the private key on the Hadoop nodes</li>
     * </ul>
     * 
     * <p><b>Note on privateKeyPassphrase:</b> This is only needed if your private key FILE
     * is encrypted with a passphrase. For automated systems like Hadoop, private keys are
     * typically NOT passphrase-protected, so you can ignore this setting.
     */
    public static class SSHConfig {
        private final String sshHost;
        private final int sshPort;
        private final String sshUser;
        private final String sshPassword;  // Fallback only - not recommended
        private final String sshPrivateKeyPath;
        private final String sshPrivateKeyPassphrase;  // Only if private key file is encrypted
        private final boolean strictHostKeyChecking;
        private final String knownHostsPath;
        private final boolean useDefaultKeys;
        
        public SSHConfig(String sshHost, int sshPort, String sshUser, 
                        String sshPassword, String sshPrivateKeyPath, 
                        String sshPrivateKeyPassphrase, boolean strictHostKeyChecking) {
            this(sshHost, sshPort, sshUser, sshPassword, sshPrivateKeyPath, 
                 sshPrivateKeyPassphrase, strictHostKeyChecking, null, true);
        }
        
        public SSHConfig(String sshHost, int sshPort, String sshUser, 
                        String sshPassword, String sshPrivateKeyPath, 
                        String sshPrivateKeyPassphrase, boolean strictHostKeyChecking,
                        String knownHostsPath, boolean useDefaultKeys) {
            this.sshHost = sshHost;
            this.sshPort = sshPort;
            this.sshUser = sshUser;
            this.sshPassword = sshPassword;
            this.sshPrivateKeyPath = sshPrivateKeyPath;
            this.sshPrivateKeyPassphrase = sshPrivateKeyPassphrase;
            this.strictHostKeyChecking = strictHostKeyChecking;
            this.knownHostsPath = knownHostsPath;
            this.useDefaultKeys = useDefaultKeys;
        }
        
        public String getSshHost() { return sshHost; }
        public int getSshPort() { return sshPort; }
        public String getSshUser() { return sshUser; }
        public String getSshPassword() { return sshPassword; }
        public String getSshPrivateKeyPath() { return sshPrivateKeyPath; }
        public String getSshPrivateKeyPassphrase() { return sshPrivateKeyPassphrase; }
        public boolean isStrictHostKeyChecking() { return strictHostKeyChecking; }
        public String getKnownHostsPath() { return knownHostsPath; }
        public boolean isUseDefaultKeys() { return useDefaultKeys; }
    }
    
    /**
     * Creates an SSH tunnel for a MongoDB URI if SSH configuration is provided
     * 
     * @param uri The original MongoDB URI
     * @param sshConfig SSH configuration (can be null)
     * @return Modified URI with tunneled connections, or original URI if no SSH config
     * @throws JSchException if SSH tunnel creation fails
     */
    public static MongoClientURI createTunnel(MongoClientURI uri, SSHConfig sshConfig) 
            throws JSchException {
        if (sshConfig == null || sshConfig.getSshHost() == null) {
            LOG.debug("No SSH configuration provided, using direct connection");
            return uri;
        }
        
        String connectionString = uri.getURI();
        LOG.info("Creating SSH tunnel for MongoDB URI: " + connectionString);
        
        // Handle mongodb+srv:// URIs
        if (connectionString.startsWith("mongodb+srv://")) {
            return createSRVTunnel(uri, sshConfig);
        }
        
        // Handle regular mongodb:// URIs
        return createStandardTunnel(uri, sshConfig);
    }
    
    /**
     * Creates SSH tunnel for MongoDB SRV connections
     */
    private static MongoClientURI createSRVTunnel(MongoClientURI uri, SSHConfig sshConfig) 
            throws JSchException {
        // For SRV records, we need to resolve the actual MongoDB hosts first
        // This is a simplified approach - in production you'd want to use DNS SRV lookup
        LOG.warn("MongoDB+SRV tunneling requires resolving SRV records. " +
                 "Using fallback to standard connection for now.");
        
        // Extract the domain from the SRV URI
        String srvUri = uri.getURI();
        Pattern srvPattern = Pattern.compile("mongodb\\+srv://(?:([^:]+):([^@]+)@)?([^/]+)(?:/(.*))?");
        Matcher matcher = srvPattern.matcher(srvUri);
        
        if (matcher.matches()) {
            String username = matcher.group(1);
            String password = matcher.group(2);
            String host = matcher.group(3);
            String database = matcher.group(4);
            
            // For SRV, typically the actual MongoDB runs on port 27017
            // In production, you would resolve the SRV record to get actual hosts
            int localPort = createSingleTunnel(host, 27017, sshConfig);
            
            // Build new connection string with tunnel
            StringBuilder newUri = new StringBuilder("mongodb://");
            if (username != null && password != null) {
                newUri.append(username).append(":").append(password).append("@");
            }
            newUri.append("localhost:").append(localPort);
            if (database != null) {
                newUri.append("/").append(database);
            }
            
            // Preserve options from original URI
            if (uri.getOptions() != null) {
                newUri.append("?").append(uri.getOptions());
            }
            
            LOG.info("Created tunneled URI: " + newUri.toString());
            return new MongoClientURI(newUri.toString());
        }
        
        throw new JSchException("Unable to parse MongoDB+SRV URI: " + srvUri);
    }
    
    /**
     * Creates SSH tunnel for standard MongoDB connections
     */
    private static MongoClientURI createStandardTunnel(MongoClientURI uri, SSHConfig sshConfig) 
            throws JSchException {
        List<String> hosts = uri.getHosts();
        StringBuilder tunneledHosts = new StringBuilder();
        
        for (String hostPort : hosts) {
            if (tunneledHosts.length() > 0) {
                tunneledHosts.append(",");
            }
            
            Matcher matcher = HOST_PORT_PATTERN.matcher(hostPort);
            if (matcher.matches()) {
                String host = matcher.group(1);
                int port = matcher.group(2) != null ? Integer.parseInt(matcher.group(2)) : 27017;
                
                int localPort = createSingleTunnel(host, port, sshConfig);
                tunneledHosts.append("localhost:").append(localPort);
            }
        }
        
        // Reconstruct the URI with tunneled hosts
        String originalUri = uri.getURI();
        String newUri = originalUri.replaceFirst(
            "mongodb://[^/]+/",
            "mongodb://" + tunneledHosts.toString() + "/"
        );
        
        // Handle URIs without database
        if (!newUri.contains("/")) {
            newUri = originalUri.replaceFirst(
                "mongodb://.*",
                "mongodb://" + tunneledHosts.toString()
            );
        }
        
        LOG.info("Created tunneled URI: " + newUri);
        return new MongoClientURI(newUri);
    }
    
    /**
     * Creates a single SSH tunnel for a host:port combination
     */
    private static int createSingleTunnel(String remoteHost, int remotePort, SSHConfig sshConfig) 
            throws JSchException {
        String tunnelKey = sshConfig.getSshHost() + ":" + remoteHost + ":" + remotePort;
        
        // Check if tunnel already exists
        TunnelInfo existingTunnel = ACTIVE_TUNNELS.get(tunnelKey);
        if (existingTunnel != null) {
            LOG.debug("Reusing existing tunnel for " + tunnelKey);
            return existingTunnel.localPort;
        }
        
        // Get or create SSH session
        Session session = getOrCreateSession(sshConfig);
        
        // Allocate a local port
        int localPort = LOCAL_PORT_COUNTER.getAndIncrement();
        
        // Create the tunnel
        LOG.info("Creating SSH tunnel: localhost:" + localPort + " -> " + 
                 remoteHost + ":" + remotePort + " via " + sshConfig.getSshHost());
        session.setPortForwardingL(localPort, remoteHost, remotePort);
        
        // Store tunnel info
        TunnelInfo tunnelInfo = new TunnelInfo(remoteHost, remotePort, localPort, tunnelKey);
        ACTIVE_TUNNELS.put(tunnelKey, tunnelInfo);
        
        return localPort;
    }
    
    /**
     * Gets or creates an SSH session using public key authentication.
     * 
     * <p>The session is created using the following authentication precedence:
     * <ol>
     *   <li>Explicit private key path if provided</li>
     *   <li>Auto-discovered keys from ~/.ssh/ (id_rsa, id_ed25519, etc.)</li>
     *   <li>Password authentication as fallback (not recommended)</li>
     * </ol>
     */
    private static Session getOrCreateSession(SSHConfig config) throws JSchException {
        String sessionKey = config.getSshHost() + ":" + config.getSshPort() + ":" + config.getSshUser();
        
        Session session = SSH_SESSIONS.get(sessionKey);
        if (session != null && session.isConnected()) {
            return session;
        }
        
        JSch jsch = new JSch();
        
        // Configure known_hosts for host key verification
        configureKnownHosts(jsch, config);
        
        // Configure authentication - try public key auth first
        boolean keyConfigured = configurePublicKeyAuth(jsch, config);
        
        if (!keyConfigured && (config.getSshPassword() == null || config.getSshPassword().isEmpty())) {
            throw new JSchException(
                "No SSH authentication configured. Please either:\n" +
                "1. Specify a private key path via mongo.ssh.private_key_path\n" +
                "2. Ensure default keys exist in ~/.ssh/ (id_rsa, id_ed25519, etc.)\n" +
                "3. Provide a password via mongo.ssh.password (not recommended)\n\n" +
                "For public key authentication, ensure your Hadoop cluster's public key " +
                "is added to the bastion host's ~/.ssh/authorized_keys"
            );
        }
        
        session = jsch.getSession(config.getSshUser(), config.getSshHost(), config.getSshPort());
        
        // Set password if provided (fallback authentication)
        if (config.getSshPassword() != null && !config.getSshPassword().isEmpty()) {
            LOG.warn("Using password authentication. Public key authentication is recommended.");
            session.setPassword(config.getSshPassword());
        }
        
        // Configure SSH session properties
        Properties sshProperties = new Properties();
        if (config.isStrictHostKeyChecking()) {
            sshProperties.put("StrictHostKeyChecking", "yes");
        } else {
            sshProperties.put("StrictHostKeyChecking", "no");
        }
        // Prefer public key authentication
        sshProperties.put("PreferredAuthentications", "publickey,keyboard-interactive,password");
        session.setConfig(sshProperties);
        
        // Set timeouts for long-running Hadoop jobs
        session.setServerAliveInterval(30000); // 30 seconds keep-alive
        session.setServerAliveCountMax(10);    // Allow 10 missed keep-alives
        session.setTimeout(120000);            // 2 minute connection timeout
        
        // Connect
        LOG.info("Establishing SSH connection to " + config.getSshHost() + ":" + config.getSshPort() + 
                 " as user '" + config.getSshUser() + "' using public key authentication");
        session.connect();
        
        LOG.info("SSH connection established successfully to bastion host: " + config.getSshHost());
        
        SSH_SESSIONS.put(sessionKey, session);
        return session;
    }
    
    /**
     * Configure known_hosts file for host key verification
     */
    private static void configureKnownHosts(JSch jsch, SSHConfig config) {
        try {
            String knownHostsPath = config.getKnownHostsPath();
            if (knownHostsPath != null && !knownHostsPath.isEmpty()) {
                File knownHostsFile = new File(knownHostsPath);
                if (knownHostsFile.exists()) {
                    jsch.setKnownHosts(knownHostsPath);
                    LOG.debug("Using known_hosts file: " + knownHostsPath);
                }
            } else {
                // Try default known_hosts location
                String userHome = System.getProperty("user.home");
                String defaultKnownHosts = userHome + File.separator + ".ssh" + File.separator + "known_hosts";
                File defaultKnownHostsFile = new File(defaultKnownHosts);
                if (defaultKnownHostsFile.exists()) {
                    jsch.setKnownHosts(defaultKnownHosts);
                    LOG.debug("Using default known_hosts file: " + defaultKnownHosts);
                }
            }
        } catch (JSchException e) {
            LOG.warn("Could not configure known_hosts: " + e.getMessage());
        }
    }
    
    /**
     * Configure public key authentication.
     * 
     * <p>This method handles the case where your Hadoop cluster's public key 
     * is stored on the customer's bastion host. The private key on the Hadoop
     * node is used to authenticate.
     * 
     * @return true if at least one key was configured
     */
    private static boolean configurePublicKeyAuth(JSch jsch, SSHConfig config) throws JSchException {
        boolean keyConfigured = false;
        List<String> configuredKeys = new ArrayList<String>();
        
        // First, try explicit private key path if provided
        if (config.getSshPrivateKeyPath() != null && !config.getSshPrivateKeyPath().isEmpty()) {
            File keyFile = new File(config.getSshPrivateKeyPath());
            if (keyFile.exists() && keyFile.canRead()) {
                addIdentity(jsch, config.getSshPrivateKeyPath(), config.getSshPrivateKeyPassphrase());
                configuredKeys.add(config.getSshPrivateKeyPath());
                keyConfigured = true;
                LOG.info("Using explicit private key: " + config.getSshPrivateKeyPath());
            } else {
                LOG.warn("Specified private key not found or not readable: " + config.getSshPrivateKeyPath());
            }
        }
        
        // If no explicit key or useDefaultKeys is true, try to find keys in ~/.ssh/
        if (config.isUseDefaultKeys()) {
            String sshDir = getSSHDirectory();
            if (sshDir != null) {
                for (String keyFileName : DEFAULT_KEY_FILES) {
                    String keyPath = sshDir + File.separator + keyFileName;
                    File keyFile = new File(keyPath);
                    if (keyFile.exists() && keyFile.canRead()) {
                        try {
                            addIdentity(jsch, keyPath, config.getSshPrivateKeyPassphrase());
                            configuredKeys.add(keyPath);
                            keyConfigured = true;
                            LOG.debug("Added SSH identity from default location: " + keyPath);
                        } catch (JSchException e) {
                            LOG.debug("Could not add key " + keyPath + ": " + e.getMessage());
                        }
                    }
                }
            }
        }
        
        if (keyConfigured) {
            LOG.info("Configured " + configuredKeys.size() + " SSH private key(s) for authentication");
        }
        
        return keyConfigured;
    }
    
    /**
     * Add an identity (private key) to JSch
     */
    private static void addIdentity(JSch jsch, String keyPath, String passphrase) throws JSchException {
        if (passphrase != null && !passphrase.isEmpty()) {
            jsch.addIdentity(keyPath, passphrase);
        } else {
            jsch.addIdentity(keyPath);
        }
    }
    
    /**
     * Get the SSH directory for the current user
     */
    private static String getSSHDirectory() {
        String userHome = System.getProperty("user.home");
        if (userHome != null) {
            String sshDir = userHome + File.separator + ".ssh";
            File sshDirFile = new File(sshDir);
            if (sshDirFile.exists() && sshDirFile.isDirectory()) {
                return sshDir;
            }
        }
        return null;
    }
    
    /**
     * Resolves MongoDB SRV records to actual host:port combinations
     * Note: This is a simplified implementation. In production, you would
     * use proper DNS SRV and TXT record lookups.
     */
    private static List<String> resolveSRVHosts(String srvHost) throws UnknownHostException {
        // This would normally do DNS SRV lookup
        // For now, we'll just return a placeholder
        LOG.warn("SRV resolution not fully implemented. Using default MongoDB port.");
        return java.util.Arrays.asList(srvHost + ":27017");
    }
    
    /**
     * Closes all SSH tunnels and sessions
     */
    public static void closeAll() {
        LOG.info("Closing all SSH tunnels and sessions");
        
        ACTIVE_TUNNELS.clear();
        
        for (Map.Entry<String, Session> entry : SSH_SESSIONS.entrySet()) {
            Session session = entry.getValue();
            if (session != null && session.isConnected()) {
                try {
                    session.disconnect();
                    LOG.debug("Closed SSH session: " + entry.getKey());
                } catch (Exception e) {
                    LOG.warn("Error closing SSH session: " + entry.getKey(), e);
                }
            }
        }
        SSH_SESSIONS.clear();
    }
    
    /**
     * Closes tunnels associated with a specific SSH configuration
     */
    public static void closeTunnels(SSHConfig config) {
        if (config == null) {
            return;
        }
        
        String sessionKey = config.getSshHost() + ":" + config.getSshPort() + ":" + config.getSshUser();
        Session session = SSH_SESSIONS.remove(sessionKey);
        
        if (session != null && session.isConnected()) {
            session.disconnect();
            LOG.info("Closed SSH session: " + sessionKey);
        }
        
        // Remove associated tunnels
        String prefix = config.getSshHost() + ":";
        java.util.Iterator<Map.Entry<String, TunnelInfo>> iterator = ACTIVE_TUNNELS.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, TunnelInfo> entry = iterator.next();
            if (entry.getValue().tunnelKey.startsWith(prefix)) {
                iterator.remove();
            }
        }
    }
}
