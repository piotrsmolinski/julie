package com.purbon.kafka.topology.clusterstate;

import com.purbon.kafka.topology.TopicManager;
import com.purbon.kafka.topology.model.ACLEntry;
import com.purbon.kafka.topology.model.Impl.ProjectImpl;
import com.purbon.kafka.topology.model.Impl.TopicImpl;
import com.purbon.kafka.topology.model.Impl.TopologyImpl;
import com.purbon.kafka.topology.model.Project;
import com.purbon.kafka.topology.model.Topic;
import com.purbon.kafka.topology.model.Topology;
import com.purbon.kafka.topology.model.users.Connector;
import com.purbon.kafka.topology.model.users.Consumer;
import com.purbon.kafka.topology.model.users.KStream;
import com.purbon.kafka.topology.model.users.Producer;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class TopologyGenerator {

  public Topology generateTopology(ClusterState clusterState) {

    Topology topology = new TopologyImpl();

    topology.setProjects(Collections.singletonList(generateProject(clusterState)));

    return topology;
  }

  public Project generateProject(ClusterState clusterState) {

    Project project = new ProjectImpl();

    List<String> topicNames = extractTopicNames(clusterState.getTopics());

    project.setZookeepers(Collections.emptyList());

    project.setTopics(extractTopics(clusterState.getTopics()));
    //        project.setProducers(extractProducers(topicNames, clusterState.getKafkaAcls()));
    //        project.setConsumers(extractConsumers(topicNames, clusterState.getKafkaAcls()));
    //        project.setStreams(extractStreams(topicNames, clusterState.getKafkaAcls()));

    project.setKafkaRawACLs(extractKafkaAcls(topicNames, clusterState.getKafkaAcls()));

    return project;
  }

  private List<Topic> extractTopics(Map<String, TopicState> topics) {
    return topics.values().stream()
        .sorted(Comparator.comparing(TopicState::getName))
        .map(this::extractTopic)
        .collect(Collectors.toList());
  }

  private List<String> extractTopicNames(Map<String, TopicState> topics) {
    return extractTopics(topics).stream().map(Topic::getName).collect(Collectors.toList());
  }

  private Topic extractTopic(TopicState t) {
    HashMap<String, String> config = new LinkedHashMap<>();
    config.put(TopicManager.NUM_PARTITIONS, String.valueOf(t.getNumPartitions()));
    config.put(TopicManager.REPLICATION_FACTOR, String.valueOf(t.getReplicationFactor()));
    Topic topic = new TopicImpl();
    topic.setName(t.getName());
    topic.setConfig(config);
    //            topic.setProjectPrefix();
    //            topic.setSchemas();
    return topic;
  }

  /** A producer user has permissions to DESCRIBE and WRITE for each topic in the project */
  private List<Producer> extractProducers(
      Collection<String> topicNames, Map<String, Set<ACLEntryState>> acls) {
    return acls.entrySet().stream()
        .filter(
            e ->
                topicNames.stream()
                    .allMatch(t -> hasProducerPermissions(t, e.getKey(), e.getValue())))
        .map(Map.Entry::getKey)
        .sorted()
        .map(p -> buildProducer(p, acls.get(p)))
        .collect(Collectors.toList());
  }

  /**
   * A consumer user has permissions to DESCRIBE and READ for each topic in the project and at least
   * one READ permission for a group.
   */
  private List<Consumer> extractConsumers(
      Collection<String> topicNames, Map<String, Set<ACLEntryState>> acls) {
    return acls.entrySet().stream()
        .filter(
            e ->
                topicNames.stream()
                    .allMatch(t -> hasConsumerPermissions(t, e.getKey(), e.getValue())))
        .map(Map.Entry::getKey)
        .sorted()
        .map(p -> buildConsumer(p, acls.get(p)))
        .collect(Collectors.toList());
  }

  /**
   * A streams user has WRITE and READ permissions to specific topics and ALL permission to prefixed
   * topics. The prefix is different story.
   *
   * @param topicNames
   * @param acls
   * @return
   */
  private List<KStream> extractStreams(
      Collection<String> topicNames, Map<String, Set<ACLEntryState>> acls) {
    return acls.values().stream()
        .flatMap(a -> a.stream())
        .filter(this::isPrefixedTopicAll)
        .map(a -> buildStreams(a.getPrincipal(), acls.get(a.getPrincipal())))
        .collect(Collectors.toList());
  }

  private Map<String, List<ACLEntry>> extractKafkaAcls(
      Collection<String> topicNames, Map<String, Set<ACLEntryState>> acls) {
    return acls.entrySet().stream()
        .sorted(Comparator.comparing(Map.Entry::getKey))
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    e.getValue().stream()
                        .map(
                            x ->
                                ACLEntry.builder()
                                    .host(x.getHost())
                                    .resource(x.getResource())
                                    .resourceType(x.getResourceType())
                                    .patternType(x.getPatternType())
                                    .operation(x.getOperation())
                                    .permissionType(x.getPermissionType())
                                    .build())
                        .collect(Collectors.toList()),
                (x, y) -> {
                  throw new RuntimeException("Duplicate entries");
                },
                LinkedHashMap::new));
  }

  /**
   * A connect user has READ and WRITE permissions to connect offset, status and configs topics
   * (defined). It has CREATE privilege on cluster and READ privilege on connect group (defined).
   * For each read topic it has READ permission and for each write topic it has WRITE permission.
   *
   * @param topicNames
   * @param acls
   * @return
   */
  private List<Connector> extractConnect(
      Collection<String> topicNames, Map<String, Set<ACLEntryState>> acls) {
    return acls.entrySet().stream()
        .filter(isConnectorUser())
        .map(buildConnector())
        .collect(Collectors.toList());
  }

  private Predicate<Map.Entry<String, Set<ACLEntryState>>> isConnectorUser() {
    return e -> {
      String principal = e.getKey();
      Set<ACLEntryState> acls = e.getValue();
      if (!acls.contains(
          ACLEntryState.builder()
              .principal(principal)
              .resource("kafka-cluster")
              .resourceType("CLUSTER")
              .permissionType("ALLOW")
              .operation("CREATE")
              .build())) return false;

      if (acls.stream()
          .noneMatch(ACLEntryState.filter().group("connect-cluster").allow().operation("READ"))) {
        return false;
      }
      if (acls.stream()
          .noneMatch(ACLEntryState.filter().topic("connect-status").allow().operation("READ"))) {
        return false;
      }
      if (acls.stream()
          .noneMatch(ACLEntryState.filter().topic("connect-status").allow().operation("WRITE"))) {
        return false;
      }
      if (acls.stream()
          .noneMatch(ACLEntryState.filter().topic("connect-config").allow().operation("READ"))) {
        return false;
      }
      if (acls.stream()
          .noneMatch(ACLEntryState.filter().topic("connect-config").allow().operation("WRITE"))) {
        return false;
      }
      if (acls.stream()
          .noneMatch(ACLEntryState.filter().topic("connect-offsets").allow().operation("READ"))) {
        return false;
      }
      if (acls.stream()
          .noneMatch(ACLEntryState.filter().topic("connect-offsets").allow().operation("WRITE"))) {
        return false;
      }

      return true;
    };
  }

  private Function<Map.Entry<String, Set<ACLEntryState>>, Connector> buildConnector() {
    return e -> {
      Connector connector = new Connector();
      connector.setPrincipal(e.getKey());

      connector.setOffset_topic(Optional.of("connect-offsets"));
      connector.setConfigs_topic(Optional.of("connect-configs"));
      connector.setStatus_topic(Optional.of("connect-status"));

      connector.setGroup(Optional.of("connect-cluster"));

      HashMap<String, List<String>> topics = new HashMap<>();
      connector.setTopics(topics);
      topics.put("read", getReadTopics(e.getValue()));
      topics.put("write", getWriteTopics(e.getValue()));
      return connector;
    };
  }

  private Producer buildProducer(String principal, Set<ACLEntryState> acls) {
    Producer producer = new Producer(principal);
    return producer;
  }

  private Consumer buildConsumer(String principal, Set<ACLEntryState> acls) {
    Consumer consumer = new Consumer(principal);
    consumer.setGroup(Optional.ofNullable(getGroup(acls)));
    return consumer;
  }

  private KStream buildStreams(String principal, Set<ACLEntryState> acls) {
    KStream streams = new KStream();
    streams.setPrincipal(principal);
    HashMap<String, List<String>> topics = new HashMap<>();
    streams.setTopics(topics);
    topics.put("read", getReadTopics(acls));
    topics.put("write", getWriteTopics(acls));
    return streams;
  }

  /**
   * Verifies if the given principal has {@link
   * com.purbon.kafka.topology.TopologyBuilderAdminClient#setAclsForProducer(String, String)
   * producer} privileges.
   *
   * @param topicName
   * @param principal
   * @param acls
   * @return
   */
  private boolean hasProducerPermissions(
      String topicName, String principal, Set<ACLEntryState> acls) {
    if (!acls.contains(
        ACLEntryState.builder()
            .principal(principal)
            .resource(topicName)
            .resourceType("TOPIC")
            .permissionType("ALLOW")
            .operation("DESCRIBE")
            .build())) return false;
    if (!acls.contains(
        ACLEntryState.builder()
            .principal(principal)
            .resource(topicName)
            .resourceType("TOPIC")
            .permissionType("ALLOW")
            .operation("WRITE")
            .build())) return false;
    return true;
  }

  /**
   * Verifies if the given principal has {@link
   * com.purbon.kafka.topology.TopologyBuilderAdminClient#setAclsForConsumer(Consumer, String)
   * producer} privileges.
   *
   * @param topicName
   * @param principal
   * @param acls
   * @return
   */
  private boolean hasConsumerPermissions(
      String topicName, String principal, Set<ACLEntryState> acls) {
    if (!acls.contains(
        ACLEntryState.builder()
            .principal(principal)
            .host("*")
            .resource(topicName)
            .resourceType("TOPIC")
            .permissionType("ALLOW")
            .operation("DESCRIBE")
            .build())) return false;
    if (!acls.contains(
        ACLEntryState.builder()
            .principal(principal)
            .host("*")
            .resource(topicName)
            .resourceType("TOPIC")
            .permissionType("ALLOW")
            .operation("READ")
            .build())) return false;
    if (!acls.stream().anyMatch(this::isGroupRead)) return false;
    return true;
  }

  private boolean isPrefixedTopicAll(ACLEntryState a) {
    return a.getPermissionType().equals("ALLOW")
        && a.getHost().equals("*")
        && a.getOperation().equals("ALL")
        && a.getResourceType().equals("TOPIC")
        && "PREFIXED".equals(a.getPatternType());
  }

  private boolean isGroupRead(ACLEntryState a) {
    return a.getPermissionType().equals("ALLOW")
        && a.getHost().equals("*")
        && a.getOperation().equals("READ")
        && a.getResourceType().equals("GROUP")
        && ("*".equals(a.getResource()) && "PREFIXED".equals(a.getPatternType())
            || "LITERAL".equals(a.getPatternType()));
  }

  private boolean isTopicRead(ACLEntryState a) {
    return a.getPermissionType().equals("ALLOW")
        && a.getHost().equals("*")
        && a.getOperation().equals("READ")
        && a.getResourceType().equals("TOPIC")
        && "LITERAL".equals(a.getPatternType());
  }

  private boolean isTopicWrite(ACLEntryState a) {
    return a.getPermissionType().equals("ALLOW")
        && a.getHost().equals("*")
        && a.getOperation().equals("WRITE")
        && a.getResourceType().equals("TOPIC")
        && "LITERAL".equals(a.getPatternType());
  }

  private String getGroup(Collection<ACLEntryState> acls) {
    return acls.stream().filter(this::isGroupRead).map(a -> a.getResource()).findAny().orElse(null);
  }

  private List<String> getReadTopics(Collection<ACLEntryState> acls) {
    return acls.stream()
        .filter(this::isTopicRead)
        .map(a -> a.getResource())
        .collect(Collectors.toList());
  }

  private List<String> getWriteTopics(Collection<ACLEntryState> acls) {
    return acls.stream()
        .filter(this::isTopicWrite)
        .map(a -> a.getResource())
        .collect(Collectors.toList());
  }
}
