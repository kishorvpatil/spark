package org.apache.spark.examples;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkLauncher;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import static java.lang.Thread.sleep;

public class MyLauncherSample {

  public static void main(String[] args) throws ParseException {
    // create the parser
    final CommandLineParser cmdLineGnuParser = new GnuParser();

    final Options gnuOptions = constructGnuOptions();
    Properties props = System.getProperties();
    props.list(System.out);
    CommandLine commandLine;
    String jarLocation = null;
    String propertiesFile = null;
    String className = null;
    boolean shouldLaunch = false;
    long waitTime = 0;
    String deployMode = "client";
    try
    {
      commandLine = cmdLineGnuParser.parse(gnuOptions, args);
      if ( commandLine.hasOption("j") )
      {
        jarLocation = commandLine.getOptionValue("j");
      }
      if ( commandLine.hasOption("p") )
      {
        propertiesFile = commandLine.getOptionValue("p");
      }

      if ( commandLine.hasOption("d") )
      {
        deployMode = commandLine.getOptionValue("d");
      }
      if ( commandLine.hasOption("l") )
      {
        shouldLaunch = true;
      }
      if ( commandLine.hasOption("c") )
      {
        className = commandLine.getOptionValue("c");
      }
      if ( commandLine.hasOption("w") )
      {
        waitTime = Long.parseLong(commandLine.getOptionValue('w', "0"));
      }
    }
    catch (ParseException parseException)  // checked exception
    {
      System.err.println(
        "Encountered exception while parsing using GnuParser:\n"
          + parseException.getMessage() );
    }

    try {
/*    if(shouldLaunch) {
      System.out.println("NOTE::Launching spark as a child process");
      SparkLauncher spark = new SparkLauncher()
        .setAppResource(jarLocation)
        .setMainClass(className)
        .setMaster("yarn")
        .setDeployMode(deployMode)
        .setPropertiesFile(propertiesFile)
        .setConf("spark.authenticate", "true")
        //.setConf("spark.eventLog.dir", "hdfs:///mapred/sparkhistory")
        .setConf(SparkLauncher.EXECUTOR_MEMORY, "2g")
        .setConf(SparkLauncher.DRIVER_MEMORY, "2g");

      spark.launch();
      System.out.println("NOTE::Waiting for launched process to complete....");
    } else {
*/
      System.out.println("NOTE::Launching spark application as independent process...");
        SparkLauncher launcher = new SparkLauncher()
          .setAppResource(jarLocation)
          .setMainClass(className)
          .setMaster("yarn")
          .setDeployMode(deployMode)
          .setVerbose(true);

        if (propertiesFile != null) {
          launcher = launcher.setPropertiesFile(propertiesFile);
        }
        launcher = launcher.setConf("spark.authenticate", "true")
          .setConf(SparkLauncher.EXECUTOR_MEMORY, "2g")
          .setConf(SparkLauncher.DRIVER_MEMORY, "2g");

      SparkAppHandle handle = null;
      if(shouldLaunch) {
        handle = launcher.startApplication();
      } else {
        handle = launcher.startApplicationAsync();
      }

      System.out.println("NOTE::Waiting for Application to finish...." + handle.getAppId() + " state is:" +handle.getState());

      while (handle.getAppId() == null) {
        System.out.println("NOTE::Waiting for Application to be Launched..");
        sleep(1000);
      }
      System.out.println("NOTE::Waiting for Application to finish...." + handle.getAppId() + " state is:" +handle.getState());

      while (handle.getState() == SparkAppHandle.State.CONNECTED || handle.getState() == SparkAppHandle.State.UNKNOWN || handle.getState() == SparkAppHandle.State.SUBMITTED) {
        System.out.println("NOTE::Waiting for Application to finish...." + handle.getAppId() + " state is:" +handle.getState());
        sleep(1000);
      }

      while (handle.getState() != SparkAppHandle.State.FINISHED && handle.getState() != SparkAppHandle.State.KILLED && handle.getState() != SparkAppHandle.State.FAILED) {
        System.out.println("NOTE::Waiting for Application to finish...." + handle.getAppId() + " state is:" +handle.getState());
        sleep(1000);
      }
      System.out.println("NOTE::Finished......" + handle.getAppId() + " state is:" +handle.getState());
    } catch (ClassNotFoundException e)  {
      System.out.println(e.getMessage());
      e.printStackTrace();
    } catch (NoSuchMethodException e)  {
      System.out.println(e.getMessage());
      e.printStackTrace();
    } catch (InvocationTargetException e)  {
      System.out.println(e.getMessage());
      e.printStackTrace();
    } catch (IllegalAccessException e)  {
      System.out.println(e.getMessage());
      e.printStackTrace();
    } catch (IOException e) {
      System.out.println(e.getMessage());
      e.printStackTrace();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // Use handle API to monitor / control application.
  }


  public static Options constructGnuOptions()
  {
    final Options gnuOptions = new Options();
    gnuOptions.addOption("p", "print", false, "Option for launching spark job")
      .addOption("j", "jar", true, "jar Location")
      .addOption("c", "class", true, "className")
      .addOption("d", "deployMode", true, "deployMode")
      .addOption("p", "propertiesFile", true, "propertiesFile")
      .addOption("l", "launch", true, "Launch application")
      .addOption("w", "wait", false, "wait in milliseconds");
    return gnuOptions;
  }

  /**
   * Print usage information to provided OutputStream.
   *
   * @param applicationName Name of application to list in usage.
   * @param options Command-line options to be part of usage.
   * @param out OutputStream to which to write the usage information.
   */
  public static void printUsage( final String applicationName, final Options options, final OutputStream out) {
    final PrintWriter writer = new PrintWriter(out);
    final HelpFormatter usageFormatter = new HelpFormatter();
    usageFormatter.printUsage(writer, 80, applicationName, options);
    writer.close();
  }

}

