/*
 * Example demonstrating SSH tunneling with mongo-hadoop
 */

package com.mongodb.hadoop.examples;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;
import org.bson.BasicBSONObject;

import java.io.IOException;

public class SSHTunnelExample {
    
    /**
     * Example Mapper that processes MongoDB documents through SSH tunnel
     */
    public static class SSHTunnelMapper 
            extends Mapper<Object, BSONObject, Text, IntWritable> {
        
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        
        @Override
        public void map(Object key, BSONObject value, Context context) 
                throws IOException, InterruptedException {
            // Example: count words in a text field
            String text = (String) value.get("text");
            if (text != null) {
                String[] words = text.split("\\s+");
                for (String w : words) {
                    word.set(w);
                    context.write(word, one);
                }
            }
        }
    }
    
    /**
     * Example Reducer that aggregates results
     */
    public static class SSHTunnelReducer 
            extends Reducer<Text, IntWritable, Text, BSONWritable> {
        
        private BSONWritable bsonWritable = new BSONWritable();
        
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            // Create output document
            BasicBSONObject output = new BasicBSONObject();
            output.put("word", key.toString());
            output.put("count", sum);
            
            bsonWritable.setDoc(output);
            context.write(key, bsonWritable);
        }
    }
    
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SSHTunnelExample <input-uri> <output-uri>");
            System.err.println("\nExample URIs:");
            System.err.println("  mongodb://user:pass@remote-host:27017/database.input_collection");
            System.err.println("  mongodb+srv://user:pass@cluster.mongodb.net/database.collection");
            System.exit(1);
        }
        
        Configuration conf = new Configuration();
        
        // MongoDB connection settings
        MongoConfigUtil.setInputURI(conf, args[0]);
        MongoConfigUtil.setOutputURI(conf, args[1]);
        
        // Configure SSH tunneling
        configureSSHTunnel(conf);
        
        // Configure the job
        Job job = Job.getInstance(conf, "MongoDB SSH Tunnel Example");
        job.setJarByClass(SSHTunnelExample.class);
        
        // Set mapper and reducer
        job.setMapperClass(SSHTunnelMapper.class);
        job.setCombinerClass(SSHTunnelReducer.class);
        job.setReducerClass(SSHTunnelReducer.class);
        
        // Set input/output formats
        job.setInputFormatClass(MongoInputFormat.class);
        job.setOutputFormatClass(MongoOutputFormat.class);
        
        // Set key/value classes
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BSONWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        
        // Submit the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
    
    /**
     * Configure SSH tunneling parameters
     */
    private static void configureSSHTunnel(Configuration conf) {
        // Check environment variables for SSH configuration
        String sshEnabled = System.getenv("MONGO_SSH_ENABLED");
        if ("true".equalsIgnoreCase(sshEnabled)) {
            MongoConfigUtil.setSSHEnabled(conf, true);
            
            // SSH host configuration
            String sshHost = System.getenv("MONGO_SSH_HOST");
            if (sshHost != null) {
                MongoConfigUtil.setSSHHost(conf, sshHost);
            }
            
            String sshPort = System.getenv("MONGO_SSH_PORT");
            if (sshPort != null) {
                MongoConfigUtil.setSSHPort(conf, Integer.parseInt(sshPort));
            } else {
                MongoConfigUtil.setSSHPort(conf, 22);
            }
            
            // SSH authentication
            String sshUser = System.getenv("MONGO_SSH_USER");
            if (sshUser != null) {
                MongoConfigUtil.setSSHUser(conf, sshUser);
            }
            
            // Try private key authentication first
            String privateKey = System.getenv("MONGO_SSH_PRIVATE_KEY");
            if (privateKey != null) {
                MongoConfigUtil.setSSHPrivateKeyPath(conf, privateKey);
                
                String keyPassphrase = System.getenv("MONGO_SSH_KEY_PASSPHRASE");
                if (keyPassphrase != null) {
                    MongoConfigUtil.setSSHPrivateKeyPassphrase(conf, keyPassphrase);
                }
            } else {
                // Fall back to password authentication
                String sshPassword = System.getenv("MONGO_SSH_PASSWORD");
                if (sshPassword != null) {
                    MongoConfigUtil.setSSHPassword(conf, sshPassword);
                }
            }
            
            // SSH options
            String strictHostKey = System.getenv("MONGO_SSH_STRICT_HOST_KEY");
            if ("true".equalsIgnoreCase(strictHostKey)) {
                MongoConfigUtil.setSSHStrictHostKeyChecking(conf, true);
            }
            
            System.out.println("SSH tunneling enabled for MongoDB connections");
            System.out.println("SSH Host: " + MongoConfigUtil.getSSHHost(conf));
            System.out.println("SSH Port: " + MongoConfigUtil.getSSHPort(conf));
            System.out.println("SSH User: " + MongoConfigUtil.getSSHUser(conf));
        } else {
            System.out.println("SSH tunneling disabled (set MONGO_SSH_ENABLED=true to enable)");
        }
    }
    
    /**
     * Example of programmatic configuration
     */
    public static void configureProgrammatically(Configuration conf) {
        // Enable SSH tunneling
        MongoConfigUtil.setSSHEnabled(conf, true);
        
        // Configure SSH server
        MongoConfigUtil.setSSHHost(conf, "bastion.example.com");
        MongoConfigUtil.setSSHPort(conf, 22);
        MongoConfigUtil.setSSHUser(conf, "hadoop");
        
        // Option 1: Use private key
        MongoConfigUtil.setSSHPrivateKeyPath(conf, "/home/hadoop/.ssh/id_rsa");
        // MongoConfigUtil.setSSHPrivateKeyPassphrase(conf, "passphrase"); // if encrypted
        
        // Option 2: Use password (not recommended for production)
        // MongoConfigUtil.setSSHPassword(conf, "password");
        
        // SSH options
        MongoConfigUtil.setSSHStrictHostKeyChecking(conf, false); // true for production
    }
}
