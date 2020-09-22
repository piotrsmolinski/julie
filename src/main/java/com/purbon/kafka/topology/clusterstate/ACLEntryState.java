package com.purbon.kafka.topology.clusterstate;

import java.util.Objects;
import java.util.function.Predicate;
import org.apache.kafka.clients.admin.DescribeAclsOptions;
import org.apache.kafka.common.acl.AclBindingFilter;

/**
 * ACL entry in the basic Kafka authorization model. The ACLs describe that given principal (user
 * identity may have more than one) is allowed or denied executing operation on a resource. <br>
 * ACLs can be accessed using Kafka admin API {@link
 * org.apache.kafka.clients.admin.Admin#describeAcls(AclBindingFilter, DescribeAclsOptions)
 * describeAcls}. <br>
 * Some of the fields do correspond to Kafka API enums. In this class, however, using limited number
 * of entries might result in a tight coupling to particular Kafka version.
 */
public class ACLEntryState {

  /** Principal that this ACL entry applies to. */
  private String principal;

  /** Originator host. */
  private String host;

  /** Resource name to match. */
  private String resource;

  /** Resource type. Defined in {@link org.apache.kafka.common.resource.ResourceType}. */
  private String resourceType;

  /**
   * Pattern type to execute matching against {@link #resource}. Defined in {@link
   * org.apache.kafka.common.resource.PatternType}
   */
  private String patternType;

  /** Operation that should be matched. See {@link org.apache.kafka.common.acl.AclOperation}. */
  private String operation;

  /**
   * Whether the permission is positive (ALLOW) or negative (DENY). See {@link
   * org.apache.kafka.common.acl.AclPermissionType}.
   */
  private String permissionType;

  public String getPrincipal() {
    return principal;
  }

  public void setPrincipal(String principal) {
    this.principal = principal;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public String getResource() {
    return resource;
  }

  public void setResource(String resource) {
    this.resource = resource;
  }

  public String getResourceType() {
    return resourceType;
  }

  public void setResourceType(String resourceType) {
    this.resourceType = resourceType;
  }

  public String getPatternType() {
    return patternType;
  }

  public void setPatternType(String patternType) {
    this.patternType = patternType;
  }

  public String getOperation() {
    return operation;
  }

  public void setOperation(String operation) {
    this.operation = operation;
  }

  public String getPermissionType() {
    return permissionType;
  }

  public void setPermissionType(String permissionType) {
    this.permissionType = permissionType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ACLEntryState that = (ACLEntryState) o;
    return Objects.equals(principal, that.principal)
        && Objects.equals(host, that.host)
        && Objects.equals(resource, that.resource)
        && Objects.equals(resourceType, that.resourceType)
        && Objects.equals(patternType, that.patternType)
        && Objects.equals(operation, that.operation)
        && Objects.equals(permissionType, that.permissionType);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        principal, host, resource, resourceType, patternType, operation, permissionType);
  }

  @Override
  public String toString() {
    return "ACLEntryState{"
        + "principal='"
        + principal
        + '\''
        + ", host='"
        + host
        + '\''
        + ", resource='"
        + resource
        + '\''
        + ", resourceType='"
        + resourceType
        + '\''
        + ", patternType='"
        + patternType
        + '\''
        + ", operation='"
        + operation
        + '\''
        + ", permissionType='"
        + permissionType
        + '\''
        + '}';
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Filter filter() {
    return new Filter();
  }

  public static class Builder {

    private String principal;

    private String host = "*";

    private String resource;

    private String resourceType;

    private String patternType = "LITERAL";

    private String operation;

    private String permissionType = "ALLOW";

    private Builder() {}

    public Builder principal(String principal) {
      this.principal = principal;
      return this;
    }

    public Builder host(String host) {
      this.host = host;
      return this;
    }

    public Builder resource(String resource) {
      this.resource = resource;
      return this;
    }

    public Builder resourceType(String resourceType) {
      this.resourceType = resourceType;
      return this;
    }

    public Builder patternType(String patternType) {
      this.patternType = patternType;
      return this;
    }

    public Builder operation(String operation) {
      this.operation = operation;
      return this;
    }

    public Builder permissionType(String permissionType) {
      this.permissionType = permissionType;
      return this;
    }

    public ACLEntryState build() {
      ACLEntryState state = new ACLEntryState();
      state.setPrincipal(principal);
      state.setHost(host);
      state.setResource(resource);
      state.setResourceType(resourceType);
      state.setPatternType(patternType);
      state.setOperation(operation);
      state.setPermissionType(permissionType);
      return state;
    }
  }

  public static class Filter implements Predicate<ACLEntryState> {

    private String principal;

    private String host;

    private String resource;

    private String resourceType;

    private String patternType;

    private String operation;

    private String permissionType;

    private Filter() {}

    public Filter principal(String principal) {
      this.principal = principal;
      return this;
    }

    public Filter host(String host) {
      this.host = host;
      return this;
    }

    public Filter resource(String resource) {
      this.resource = resource;
      return this;
    }

    public Filter resourceType(String resourceType) {
      this.resourceType = resourceType;
      return this;
    }

    public Filter topic(String topic) {
      this.resource = topic;
      this.resourceType = "TOPIC";
      return this;
    }

    public Filter group(String group) {
      this.resource = group;
      this.resourceType = "GROUP";
      return this;
    }

    public Filter cluster() {
      this.resource = "kafka-cluster";
      this.resourceType = "CLUSTER";
      return this;
    }

    public Filter patternType(String patternType) {
      this.patternType = patternType;
      return this;
    }

    public Filter operation(String operation) {
      this.operation = operation;
      return this;
    }

    public Filter permissionType(String permissionType) {
      this.permissionType = permissionType;
      return this;
    }

    public Filter allow() {
      this.permissionType = "ALLOW";
      return this;
    }

    public Filter deny() {
      this.permissionType = "DENY";
      return this;
    }

    @Override
    public boolean test(ACLEntryState aclEntryState) {
      if (aclEntryState == null) return false;
      if (principal != null && principal.equals(aclEntryState.getPrincipal())) return false;
      if (host != null && host.equals(aclEntryState.getHost())) return false;
      if (resource != null && resource.equals(aclEntryState.getResource())) return false;
      if (resourceType != null && resourceType.equals(aclEntryState.getResourceType()))
        return false;
      if (patternType != null && patternType.equals(aclEntryState.getPatternType())) return false;
      if (operation != null && operation.equals(aclEntryState.getOperation())) return false;
      if (permissionType != null && permissionType.equals(aclEntryState.getPermissionType()))
        return false;
      return true;
    }
  }
}
