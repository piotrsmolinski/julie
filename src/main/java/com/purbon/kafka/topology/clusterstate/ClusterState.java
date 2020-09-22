package com.purbon.kafka.topology.clusterstate;

import java.util.*;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.clients.admin.DescribeConfigsOptions;
import org.apache.kafka.clients.admin.DescribeTopicsOptions;
import org.apache.kafka.clients.admin.ListTopicsOptions;
import org.apache.kafka.common.acl.AclBindingFilter;

/** Cluster state represents the view of the managed objects. */
public class ClusterState {

  /**
   * Cluster config {@link org.apache.kafka.clients.admin.Admin#describeConfigs(Collection,
   * DescribeConfigsOptions)}
   */
  private Map<String, String> config;

  /**
   * List of all topics in the cluster. See: {@link
   * org.apache.kafka.clients.admin.Admin#listTopics(ListTopicsOptions)} {@link
   * org.apache.kafka.clients.admin.Admin#describeTopics(Collection, DescribeTopicsOptions)} {@link
   * org.apache.kafka.clients.admin.Admin#describeConfigs(Collection, DescribeConfigsOptions)}
   */
  private Map<String, TopicState> topics;

  /**
   * List of principal ACL entries defined in Kafka. {@link
   * org.apache.kafka.clients.admin.Admin#describeAcls(AclBindingFilter, DescribeAclsOptions)}
   */
  private Map<String, Set<ACLEntryState>> kafkaAcls;

  /** List of principal ACL entries defined in RBAC. */
  private Map<String, Set<ACLEntryState>> rbacAcls;

  /** RBAC roles defined in the metadata service. */
  private Map<String, RBACRoleState> rbacRoles;

  /** List of principal role bindings */
  private Map<String, Set<RBACBindingState>> rbacBindings;

  public Map<String, String> getConfig() {
    return config;
  }

  public void setConfig(Map<String, String> config) {
    this.config = config;
  }

  public Map<String, TopicState> getTopics() {
    return topics;
  }

  public void setTopics(Map<String, TopicState> topics) {
    this.topics = topics;
  }

  public Map<String, Set<ACLEntryState>> getKafkaAcls() {
    return kafkaAcls;
  }

  public void setKafkaAcls(Map<String, Set<ACLEntryState>> kafkaAcls) {
    this.kafkaAcls = kafkaAcls;
  }

  public Map<String, Set<ACLEntryState>> getRbacAcls() {
    return rbacAcls;
  }

  public void setRbacAcls(Map<String, Set<ACLEntryState>> rbacAcls) {
    this.rbacAcls = rbacAcls;
  }

  public Map<String, RBACRoleState> getRbacRoles() {
    return rbacRoles;
  }

  public void setRbacRoles(Map<String, RBACRoleState> rbacRoles) {
    this.rbacRoles = rbacRoles;
  }

  public Map<String, Set<RBACBindingState>> getRbacBindings() {
    return rbacBindings;
  }

  public void setRbacBindings(Map<String, Set<RBACBindingState>> rbacBindings) {
    this.rbacBindings = rbacBindings;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ClusterState state = (ClusterState) o;
    return Objects.equals(config, state.config)
        && Objects.equals(topics, state.topics)
        && Objects.equals(kafkaAcls, state.kafkaAcls)
        && Objects.equals(rbacAcls, state.rbacAcls)
        && Objects.equals(rbacRoles, state.rbacRoles)
        && Objects.equals(rbacBindings, state.rbacBindings);
  }

  @Override
  public int hashCode() {
    return Objects.hash(config, topics, kafkaAcls, rbacAcls, rbacRoles, rbacBindings);
  }

  @Override
  public String toString() {
    return "ClusterState{"
        + "config="
        + config
        + ", topics="
        + topics
        + ", kafkaAcls="
        + kafkaAcls
        + ", rbacAcls="
        + rbacAcls
        + ", rbacRoles="
        + rbacRoles
        + ", rbacBindings="
        + rbacBindings
        + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Map<String, String> config;

    private Map<String, TopicState> topics;

    private Map<String, Set<ACLEntryState>> kafkaAcls;

    private Map<String, Set<ACLEntryState>> rbacAcls;

    private Map<String, RBACRoleState> rbacRoles;

    private Map<String, Set<RBACBindingState>> rbacBindings;

    private Builder() {}

    public Builder withConfig(Map<String, String> config) {
      this.config = config;
      return this;
    }

    public Builder withTopics(Map<String, TopicState> topics) {
      this.topics = topics;
      return this;
    }

    public Builder withKafkaAcls(Map<String, Set<ACLEntryState>> kafkaAcls) {
      this.kafkaAcls = kafkaAcls;
      return this;
    }

    public Builder withRbacAcls(Map<String, Set<ACLEntryState>> rbacAcls) {
      this.rbacAcls = rbacAcls;
      return this;
    }

    public Builder withRbacRoles(Map<String, RBACRoleState> rbacRoles) {
      this.rbacRoles = rbacRoles;
      return this;
    }

    public Builder withRbacBindings(Map<String, Set<RBACBindingState>> rbacBindings) {
      this.rbacBindings = rbacBindings;
      return this;
    }

    public ClusterState build() {
      ClusterState state = new ClusterState();
      state.setConfig(config);
      state.setTopics(topics);
      state.setKafkaAcls(kafkaAcls);
      state.setRbacAcls(rbacAcls);
      state.setRbacRoles(rbacRoles);
      state.setRbacBindings(rbacBindings);
      return state;
    }
  }
}
