package com.purbon.kafka.topology;

import static java.lang.System.exit;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.cli.*;

public class BuilderCLI {

  public static final String TOPOLOGY_OPTION = "topology";
  public static final String TOPOLOGY_DESC = "Topology config file.";

  public static final String BROKERS_OPTION = "brokers";
  public static final String BROKERS_DESC = "The Apache Kafka server(s) to connect to.";

  public static final String ADMIN_CLIENT_CONFIG_OPTION = "clientConfig";
  public static final String ADMIN_CLIENT_CONFIG_DESC = "The AdminClient configuration file.";

  public static final String ALLOW_DELETE_OPTION = "allowDelete";
  public static final String ALLOW_DELETE_DESC =
      "Permits delete operations for topics and configs.";

  public static final String DRY_RUN_OPTION = "dryRun";
  public static final String DRY_RUN_DESC = "Print the execution plan without altering anything.";

  public static final String QUIET_OPTION = "quiet";
  public static final String QUIET_DESC = "Print minimum status update";

  public static final String HELP_OPTION = "help";
  public static final String HELP_DESC = "Prints usage information.";

  public static final String VERSION_OPTION = "version";
  public static final String VERSION_DESC = "Prints useful version information.";

  public static final String APP_NAME = "kafka-topology-builder";

  public static final String IMPORT_OPTION = "import";
  public static final String IMPORT_DESC = "Import the topology into the cluster.";

  public static final String EXPORT_OPTION = "export";
  public static final String EXPORT_DESC = "Export the topology from the cluster.";

  private HelpFormatter formatter;
  private CommandLineParser parser;
  private Options options;

  public BuilderCLI() {
    formatter = new HelpFormatter();
    parser = new DefaultParser();
    options = buildOptions();
  }

  private Options buildOptions() {

    final Option topologyFileOption =
        Option.builder().longOpt(TOPOLOGY_OPTION).hasArg().desc(TOPOLOGY_DESC).required().build();

    final Option brokersListOption =
        Option.builder().longOpt(BROKERS_OPTION).hasArg().desc(BROKERS_DESC).required().build();

    final Option adminClientConfigFileOption =
        Option.builder()
            .longOpt(ADMIN_CLIENT_CONFIG_OPTION)
            .hasArg()
            .desc(ADMIN_CLIENT_CONFIG_DESC)
            .required()
            .build();

    final Option allowDeleteOption =
        Option.builder()
            .longOpt(ALLOW_DELETE_OPTION)
            .hasArg(false)
            .desc(ALLOW_DELETE_DESC)
            .required(false)
            .build();

    final Option dryRunOption =
        Option.builder()
            .longOpt(DRY_RUN_OPTION)
            .hasArg(false)
            .desc(DRY_RUN_DESC)
            .required(false)
            .build();

    final Option quietOption =
        Option.builder()
            .longOpt(QUIET_OPTION)
            .hasArg(false)
            .desc(QUIET_DESC)
            .required(false)
            .build();

    final Option versionOption =
        Option.builder()
            .longOpt(VERSION_OPTION)
            .hasArg(false)
            .desc(VERSION_DESC)
            .required(false)
            .build();

    final Option importOption =
        Option.builder()
            .longOpt(IMPORT_OPTION)
            .hasArg(false)
            .desc(IMPORT_DESC)
            .required(false)
            .build();

    final Option exportOption =
        Option.builder()
            .longOpt(EXPORT_OPTION)
            .hasArg(false)
            .desc(EXPORT_DESC)
            .required(false)
            .build();

    final Option helpOption =
        Option.builder().longOpt(HELP_OPTION).hasArg(false).desc(HELP_DESC).required(false).build();

    final Options options = new Options();

    options.addOption(topologyFileOption);
    options.addOption(brokersListOption);
    options.addOption(adminClientConfigFileOption);

    options.addOption(allowDeleteOption);
    options.addOption(dryRunOption);
    options.addOption(quietOption);
    options.addOption(versionOption);
    options.addOption(importOption);
    options.addOption(exportOption);
    options.addOption(helpOption);

    return options;
  }

  public static void main(String[] args) throws IOException {

    BuilderCLI cli = new BuilderCLI();
    cli.run(args);
    exit(0);
  }

  public void run(String[] args) throws IOException {
    printHelpOrVersion(args);
    CommandLine cmd = parseArgsOrExit(args);

    String topology = cmd.getOptionValue(TOPOLOGY_OPTION);
    Map<String, String> config = parseConfig(cmd);

    if (cmd.hasOption("export")) {
      processExport(topology, config);
      System.out.println("Kafka Topology exported");
    } else {
      processImport(topology, config);
      System.out.println("Kafka Topology imported");
    }
  }

  public void processImport(String topology, Map<String, String> config) {
    KafkaTopologyBuilder.verifyRequiredParameters(topology, config);
    try (KafkaTopologyBuilder builder = KafkaTopologyBuilder.build(config)) {
      builder.importTopology(topology);
    }
  }

  public void processExport(String topology, Map<String, String> config) {
    try (KafkaTopologyBuilder builder = KafkaTopologyBuilder.build(config)) {
      builder.exportTopology(topology);
    }
  }

  public Map<String, String> parseConfig(CommandLine cmd) {
    String brokersList = cmd.getOptionValue(BROKERS_OPTION);
    boolean allowDelete = cmd.hasOption(ALLOW_DELETE_OPTION);
    boolean dryRun = cmd.hasOption(DRY_RUN_OPTION);
    boolean quiet = cmd.hasOption(QUIET_OPTION);
    String adminClientConfigFile = cmd.getOptionValue(ADMIN_CLIENT_CONFIG_OPTION);

    Map<String, String> config = new HashMap<>();
    config.put(BROKERS_OPTION, brokersList);
    config.put(ALLOW_DELETE_OPTION, String.valueOf(allowDelete));
    config.put(DRY_RUN_OPTION, String.valueOf(dryRun));
    config.put(QUIET_OPTION, String.valueOf(quiet));
    config.put(ADMIN_CLIENT_CONFIG_OPTION, adminClientConfigFile);
    return config;
  }

  public void printHelpOrVersion(String[] args) {

    List<String> listOfArgs = Arrays.asList(args);

    if (listOfArgs.contains("--" + HELP_OPTION)) {
      formatter.printHelp(APP_NAME, options);
      exit(0);
    } else if (listOfArgs.contains("--" + VERSION_OPTION)) {
      System.out.println(KafkaTopologyBuilder.getVersion());
      exit(0);
    }
  }

  public CommandLine parseArgsOrExit(String[] args) {
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
      if (cmd.hasOption("export")) {
        if (cmd.hasOption("import")) {
          throw new ParseException("--import and --export options are mutually exclusive");
        }
        // TODO: checks for options irrelevant for export
      } else {
        // TODO: checks for options irrelevant for import
      }
    } catch (ParseException e) {
      System.out.println("Parsing failed cause of " + e.getMessage());
      formatter.printHelp("cli", options);
      exit(1);
    }
    return cmd;
  }
}
