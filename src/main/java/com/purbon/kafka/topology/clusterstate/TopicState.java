package com.purbon.kafka.topology.clusterstate;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;

/**
 * Topic state represents the information we can collect on topic with Kafka admin API using {@link
 * org.apache.kafka.clients.admin.Admin#describeTopics(Collection, DescribeTopicsOptions)
 * describeTopics} and {@link org.apache.kafka.clients.admin.Admin#describeConfigs(Collection,
 * DescribeConfigsOptions) describeConfigs} methods.
 */
public class TopicState {

  /** Actual topic name as known to the Kafka cluster. */
  private String name;

  /**
   * Replication factor as maximum replication for each partition. <br>
   * Important: replication factor does not exist in Kafka on topic level. Each partition may have
   * different replication factor.
   */
  private Integer replicationFactor;

  /** Number of partitions. */
  private Integer numPartitions;

  /** List of replica placement for each partition */
  private List<List<Integer>> replicas;

  /** Configuration entries explicitly specified for the topic. */
  private Map<String, String> config;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public Integer getReplicationFactor() {
    return replicationFactor;
  }

  public void setReplicationFactor(Integer replicationFactor) {
    this.replicationFactor = replicationFactor;
  }

  public Integer getNumPartitions() {
    return numPartitions;
  }

  public void setNumPartitions(Integer numPartitions) {
    this.numPartitions = numPartitions;
  }

  public List<List<Integer>> getReplicas() {
    return replicas;
  }

  public void setReplicas(List<List<Integer>> replicas) {
    this.replicas = replicas;
  }

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TopicState that = (TopicState) o;
    return Objects.equals(this.name, that.name)
        && Objects.equals(this.replicationFactor, that.replicationFactor)
        && Objects.equals(this.numPartitions, that.numPartitions)
        && Objects.equals(this.replicas, that.replicas)
        && Objects.equals(this.config, that.config);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, replicationFactor, numPartitions, replicas, config);
  }

  @Override
  public String toString() {
    return "TopicState{"
        + "name='"
        + name
        + "'"
        + ", replicationFactor="
        + replicationFactor
        + ", numPartitions="
        + numPartitions
        + ", replicas="
        + replicas
        + ", config="
        + config
        + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private String name;

    private Integer replicationFactor;

    private Integer numPartitions;

    private List<List<Integer>> replicas;

    private Map<String, String> config;

    public Builder withName(String name) {
      this.name = name;
      return this;
    }

    public Builder withReplicationFactor(Integer replicationFactor) {
      this.replicationFactor = replicationFactor;
      return this;
    }

    public Builder withNumPartitions(Integer numPartitions) {
      this.numPartitions = numPartitions;
      return this;
    }

    public Builder withReplicas(List<List<Integer>> replicas) {
      this.replicas = replicas;
      return this;
    }

    public Builder withConfig(Map<String, String> config) {
      this.config = config;
      return this;
    }

    public TopicState build() {
      TopicState state = new TopicState();
      state.setName(name);
      state.setReplicationFactor(replicationFactor);
      state.setNumPartitions(numPartitions);
      state.setReplicas(replicas);
      state.setConfig(config);
      return state;
    }
  }
}
