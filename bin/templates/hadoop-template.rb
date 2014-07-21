#! /usr/bin/ruby

@signal = 'TERM'
@format = false

def stopService(service, name)
  pid = `jps | grep #{service} | cut -d' ' -f1`
  if pid != ''
    puts "Shutting down #{name}"
    %x( kill -s #{@signal} #{pid} )
  end
end

def startService(bin, service)
  puts "Starting #{service}"
  system "@HADOOP_HOME@/bin/#{bin} #{service} &> '@PROJECT_HOME@/logs/#{service}.log' &"
end

def start()
  shutdown

  if @format
    force=''
    %x( rm -rf @HADOOP_BINARIES@/tmpdir/ )
    if '@HADOOP_VERSION@'.start_with?('2.*')
      force='-force'
    end
    system "@HADOOP_HOME@/bin/@BIN@ namenode -format #{force} &> '@PROJECT_HOME@/logs/namenode-format.out' &"
  end

  Dir.glob("@PROJECT_HOME@/logs/*.log") do |logFile|
    File.truncate(logFile, 0)
  end

  startService '@BIN@', 'namenode'
  sleep 5
  startService '@BIN@', 'datanode'
  if !'@HADOOP_VERSION@'.start_with?('1.*')
    startService 'yarn', 'resourcemanager'
    startService 'yarn', 'nodemanager'
  else
    startService 'hadoop', 'jobtracker'
    startService 'hadoop', 'tasktracker'
  end
  sleep 15

  if '@HADOOP_VERSION@'.include? 'cdh4'
    %x( @HADOOP_HOME@/bin/hadoop fs -mkdir @HIVE_HOME@/lib )
    %x( @HADOOP_HOME@/bin/hadoop fs -put @HIVE_HOME@/lib/hive-builtins- *.jar @HIVE_HOME@/lib )
    sleep 5
  end

  puts 'Starting hiveserver'
  env = {}
  env['HADOOP_HOME']='@HADOOP_HOME@'
  if '@HADOOP_VERSION@'.include? "cdh"
    env['MAPRED_DIR']='@HADOOP_HOME@/share/hadoop/mapreduce2'
  end
  system(env, "@HIVE_HOME@/bin/hive --service hiveserver &> '@PROJECT_HOME@/logs/hiveserver.log' &")

  file = File.new('@PROJECT_HOME@/logs/sqoop.log', 'w')
  file.truncate(0)
  file.close()
  system(env, "@SQOOP_HOME@/bin/sqoop.sh server start") 
end

def stopAll()
  stopService 'NodeManager', 'node manager'
  stopService 'ResourceManager', 'resource manager'
  stopService 'DataNode', 'data node'
  stopService 'JobTracker', 'job tracker'
  stopService 'TaskTracker', 'task tracker'
  stopService 'NameNode', 'name node'
  stopService 'RunJar', 'hive server'
  stopService 'Bootstrap', 'sqoop server'
end

def shutdown()
  stopAll
  signal='KILL'
  stopAll
end

if ARGV.length == 0
  start
else
  ARGV.each do |arg|
    if arg == 'format'
      @format=true
    end
  end
  
  ARGV.each do |arg|
    if arg == 'shutdown'
      shutdown
    elsif arg == 'start'
      start
    end
  end
end
