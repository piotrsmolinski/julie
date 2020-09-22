package com.purbon.kafka.topology.clusterstate;

import java.util.List;
import java.util.Map;

/** RBAC binds principals to roles for the given resources. */
public class RBACBindingState {

  /** Principal identity, typically User:username or Group:groupname */
  private String principal;

  /** Role name as defined in cluster roles */
  private String roleName;

  private Scope scope;

  private Resource resource;

  /**
   * The scope represents the resource binding context expressed as clusters. See {@link
   * io.confluent.security.authorizer.Scope}
   */
  private static class Scope {

    private List<String> path;

    private Map<String, String> clusters;
  }

  /** Resource matching pattern. See {@link io.confluent.security.authorizer.ResourcePattern} */
  private static class Resource {

    private String name;

    private String resourceType;

    private String patternType;
  }
}
