package org.apache.hadoop.hive.metastore.tools;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

public class HiveMetaStorePerfTool {
  class Stats {
    long count;
    long succCount;
    long failCount;
    long succTimeInMs;
    synchronized void merge(Stats s) {
      count += s.count;
      succCount += s.succCount;
      failCount += s.failCount;
      succTimeInMs += s.succTimeInMs;
    }
  }
  
  private final Options cmdLineOptions = new Options();
  Map<String, Stats> commandMap = new HashMap<String, Stats>();
  List<String> commands = new ArrayList<String>();
  boolean silent = false;
  public HiveMetaStorePerfTool () {
  }
  public void readCommandFile(String fileName) throws IOException {
    BufferedReader reader = new BufferedReader(new FileReader(fileName));
    String line;
    while ((line = reader.readLine())!=null) {
      if (!line.isEmpty()&&!line.startsWith("--")) {
        commandMap.put(line, new Stats());
        commands.add(line);
      }
    }
    reader.close();
  }
  class Pair<T, U> {
    public T first;
    public U second;
    public Pair(T f, U s) {
      first = f;
      second = s;
    }
  }
  class TestRunner implements Runnable {
    Pattern methodPattern = Pattern.compile("(Partition p=|Table t=|p\\.|t\\.)?(.+)\\((.+)\\)");
    long repeat;
    Random r = new Random();
    public TestRunner(long repeat) {
      this.repeat = repeat;
    }
    Pair<Object, Class> parseArg(String argString, Table t, Partition p) {
      Object o = null;
      Class c = null;
      if (argString.startsWith("{") && argString.endsWith("}")) {
        List<Object> l = new ArrayList<Object>();
        for (String item : argString.substring(1, argString.length()-1).split(":")) {
          l.add(parseArg(item, t, p).first);
        }
        o = l;
        c = List.class;
      } if (argString.startsWith("[") && argString.endsWith("]")) {
        Map<Object, Object> m = new HashMap<Object, Object>();
        for (String arg : argString.substring(1, argString.length()-1).split(":")) {
          String[] keyValue = arg.split("#");
          m.put(parseArg(keyValue[0], t, p), parseArg(keyValue[1], t, p));
        }
        o = m;
        c = Map.class;
      } else if (argString.equals("t")) {
        o = t;
        c = Table.class;
      } else if (argString.equals("p")) {
        o = p;
        c = Partition.class;
      } else if (argString.equals("false")||argString.equals("true")) {
        o = Boolean.parseBoolean(argString);
      } else if (argString.endsWith("s")) {
        o = Short.parseShort(argString.substring(0, argString.length()-2));
        c = short.class;
      } else if (argString.endsWith("i")) {
        o = Integer.parseInt(argString.substring(0, argString.length()-2));
        c = int.class;
      } else if (argString.endsWith("l")) {
        o = Long.parseLong(argString.substring(0, argString.length()-2));
        c = long.class;
      } else if (argString.startsWith("'") && argString.endsWith("'")){
        o = argString.substring(1, argString.length()-1);
        c = String.class;
      }
      return new Pair(o, c);
    }
    Stats runLine(HiveMetaStoreClient client, String line) throws Exception {
      Stats stats = new Stats();
      stats.count = 1;
      String[] statements = line.split(";");
      Partition part = null;
      Table table = null;
      long startTime = 0;
      long endTime = 0;
      for (String statement : statements) {
        Class c = null;
        Object o = null;
        Matcher m;
        if ((m=methodPattern.matcher(statement)).matches()) {
          if (m.group(1) != null) {
            if (m.group(1).equals("p.")) {
              c = Partition.class;
              if (part!=null) {
                o = part;
              } else {
                stats.failCount = 1;
                return stats;
              }
            } else if (m.group(1).equals("t.")) {
              c = Table.class;
              if (table!=null) {
                o = table;
              } else {
                stats.failCount = 1;
                return stats;
              }
            }
          }
          if (c == null) {
            c = HiveMetaStoreClient.class;
            o = client;
          }
          String methodName = m.group(2);
          String arg = m.group(3);
          String[] argStrings = arg.split(",");
          Object[] args = new Object[argStrings.length];
          Class[] argTypes = new Class[argStrings.length];
          for (int i=0;i<args.length;i++) {
            Pair<Object, Class> pair = parseArg(argStrings[i], table, part);
            args[i] = pair.first;
            argTypes[i] = pair.second;
          }
          Method method = c.getMethod(methodName, argTypes);
          startTime = System.currentTimeMillis();
          Object result = method.invoke(o, args);
          endTime = System.currentTimeMillis();
          if (m.group(1) != null) {
            if (m.group(1).equals("Table t=")) {
              table = (Table)result;
            } else if (m.group(1).equals("Partition p=")) {
              part = (Partition)result;
            }
          }
        } else {
          throw new RuntimeException("invalid statement " + statement);
        }
      }
      stats.succCount = 1;
      stats.succTimeInMs = (endTime-startTime);
      return stats;
    }
    @Override
    public void run() {
      try {
        HiveMetaStoreClient client = new HiveMetaStoreClient(new HiveConf());
        // initialize
        client.getDatabase("default");

        for (int i=0;i<repeat;i++) {
          int seq = r.nextInt(commands.size());
          String command = commands.get(seq);
          Stats stats;
          try {
            stats = runLine(client, command);
          } catch (Exception e) {
            if (!silent) {
              e.printStackTrace();
            }
            stats = new Stats();
            stats.failCount++;
          }
          commandMap.get(command).merge(stats);
        }
      } catch (Exception e) {
        if (!silent) {
          e.printStackTrace();
        }
      }
    }
  }

  public void run(String[] args) throws Exception {
    Option helpOption = new Option("help", "print this message");
    Option hiveserver2Option = new Option("hiveserver2", "Use hiveserver2 config");
    Option metastoreOption = new Option("metastore", "Use metastore config");
    Option silentOption = new Option("s", "silent");
    Option repeatOption =
        OptionBuilder
            .hasArgs()
            .withDescription(
                "number of api calls per thread").create("r");
    Option threadOption =
        OptionBuilder
            .hasArgs()
            .withDescription(
                "threads to use").create("t");
    Option commandsOption =
        OptionBuilder
            .hasArgs()
            .withDescription(
                "command file").create("f");
    cmdLineOptions.addOption(helpOption);
    cmdLineOptions.addOption(hiveserver2Option);
    cmdLineOptions.addOption(metastoreOption);
    cmdLineOptions.addOption(silentOption);
    cmdLineOptions.addOption(repeatOption);
    cmdLineOptions.addOption(threadOption);
    cmdLineOptions.addOption(commandsOption);

    CommandLineParser parser = new GnuParser();
    CommandLine line = null;
    try {
      line = parser.parse(cmdLineOptions, args);
    } catch (ParseException e) {
      System.err.println("HiveMetaStorePerfTool:Parsing failed.  Reason: " + e.getLocalizedMessage());
    }
    if (line.hasOption("help")) {
      HelpFormatter formatter = new HelpFormatter();
      formatter.printHelp("metastoreperftool", cmdLineOptions);
      System.exit(0);
    }
    if (line.hasOption("hiveserver2")) {
      HiveConf.setLoadHiveServer2Config(true);
    }
    if (line.hasOption("metastore")) {
      HiveConf.setLoadMetastoreConfig(true);
    }
    if (line.hasOption("s")) {
      silent = true;
    }
    long repeat = 1;
    if (line.hasOption("r")) {
      repeat = Long.parseLong(line.getOptionValue("r"));
    }
    int thread = 1;
    if (line.hasOption("t")) {
      thread = Integer.parseInt(line.getOptionValue("t"));
    }
    if (line.hasOption("f")) {
      readCommandFile(line.getOptionValue("f"));
    }

    Thread[] threads= new Thread[thread];
    for (int i=0;i<thread;i++) {
      threads[i] = new Thread(new TestRunner(repeat));
      threads[i].start();
    }
    for (int i=0;i<thread;i++) {
      threads[i].join();
    }

    for (String command : commands) {
      Stats stats = commandMap.get(command);
      if (command.length()<100) {
        System.out.println(command);
      } else {
        System.out.println(command.substring(0, 97) + "...");
      }
      String message = "count: " + stats.count;
      if (stats.failCount!=0) {
        message = message + "(" + stats.failCount + " fail)";
      }
      if (stats.succCount!=0) {
        message = message + "\t" + "\tavg:"+ stats.succTimeInMs/(double)stats.succCount;
      }
      System.out.println("\t\t" + message);
    }
  }

  public static void main(String args[]) throws Exception {
    HiveMetaStorePerfTool perfTool = new HiveMetaStorePerfTool();
    perfTool.run(args);
  }
}
