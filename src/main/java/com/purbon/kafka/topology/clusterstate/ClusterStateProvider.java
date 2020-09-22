package com.purbon.kafka.topology.clusterstate;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.config.ConfigResource;

public class ClusterStateProvider {

  private final Map<String, Object> config;

  public ClusterStateProvider(Map<String, Object> config) {
    this.config = config;
  }

  public ClusterState readClusterState() {

    try (Admin admin = Admin.create(config)) {

      Collection<String> topicNames = getTopicNames(admin);

      Map<String, TopicDescription> descriptions = getTopicDescriptions(admin, topicNames);

      Map<String, Config> topicConfigs = getTopicConfigs(admin, topicNames);

      Config clusterConfig = getClusterConfig(admin);

      Collection<AclBinding> kafkaAcls = getKafkaAcls(admin);

      return ClusterState.builder()
          .withConfig(config(clusterConfig))
          .withTopics(topicStates(topicNames, descriptions, topicConfigs))
          .withKafkaAcls(acls(kafkaAcls))
          .build();
    }
  }

  private Collection<String> getTopicNames(Admin admin) {
    try {
      return admin.listTopics().listings().get().stream()
          .filter(x -> !x.isInternal())
          .map(x -> x.name())
          .collect(Collectors.toSet());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, TopicDescription> getTopicDescriptions(
      Admin admin, Collection<String> topicNames) {
    try {
      return admin.describeTopics(topicNames).all().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Config getClusterConfig(Admin admin) {
    try {
      return admin
          .describeConfigs(
              Collections.singleton(new ConfigResource(ConfigResource.Type.BROKER, "")))
          .all()
          .get()
          .get(new ConfigResource(ConfigResource.Type.BROKER, ""));
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, Config> getTopicConfigs(Admin admin, Collection<String> topicNames) {
    try {
      return admin
          .describeConfigs(
              topicNames.stream()
                  .map(t -> new ConfigResource(ConfigResource.Type.TOPIC, t))
                  .collect(Collectors.toSet()))
          .all().get().entrySet().stream()
          .collect(Collectors.toMap(p -> p.getKey().name(), p -> p.getValue()));
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, TopicState> topicStates(
      Collection<String> topicNames,
      Map<String, TopicDescription> topicDescriptions,
      Map<String, Config> topicConfigs) {

    return topicNames.stream()
        .collect(
            Collectors.toMap(
                Function.identity(),
                topicName ->
                    TopicState.builder()
                        .withName(topicName)
                        .withNumPartitions(numPartitions(topicDescriptions.get(topicName)))
                        .withReplicationFactor(replicationFactor(topicDescriptions.get(topicName)))
                        .withReplicas(replicas(topicDescriptions.get(topicName)))
                        .withConfig(config(topicConfigs.get(topicName)))
                        .build()));
  }

  private Collection<AclBinding> getKafkaAcls(Admin admin) {
    try {
      return admin.describeAcls(AclBindingFilter.ANY).values().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, Set<ACLEntryState>> acls(Collection<AclBinding> bindings) {
    return bindings.stream()
        .map(
            x ->
                ACLEntryState.builder()
                    .principal(x.entry().principal())
                    .host(x.entry().host())
                    .resource(x.pattern().name())
                    .resourceType(x.pattern().resourceType().name())
                    .patternType(x.pattern().patternType().name())
                    .operation(x.entry().operation().name())
                    .permissionType(x.entry().permissionType().name())
                    .build())
        .collect(Collectors.groupingBy(x -> x.getPrincipal(), Collectors.toSet()));
  }

  private Map<String, String> config(Config config) {
    return config.entries().stream()
        .filter(e -> !e.isDefault())
        .collect(Collectors.toMap(e -> e.name(), e -> e.value()));
  }

  private Integer numPartitions(TopicDescription description) {
    return description.partitions().size();
  }

  private Integer replicationFactor(TopicDescription description) {
    return description.partitions().stream().mapToInt(p -> p.replicas().size()).max().getAsInt();
  }

  private List<List<Integer>> replicas(TopicDescription description) {
    Map<Integer, List<Integer>> m =
        description.partitions().stream()
            .collect(
                Collectors.toMap(
                    p -> p.partition(),
                    p -> p.replicas().stream().map(n -> n.id()).collect(Collectors.toList())));
    return IntStream.range(0, numPartitions(description))
        .mapToObj(x -> m.get(x))
        .collect(Collectors.toList());
  }
}
