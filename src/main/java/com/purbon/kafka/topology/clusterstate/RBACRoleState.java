package com.purbon.kafka.topology.clusterstate;

import java.util.List;
import java.util.Objects;

/** RBAC role describes set of operations that may be executed against given resource. */
public class RBACRoleState {

  /** Name of the role. */
  private String name;

  /** Access policy defined for this role. */
  private AccessPolicy accessPolicy;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public AccessPolicy getAccessPolicy() {
    return accessPolicy;
  }

  public void setAccessPolicy(AccessPolicy accessPolicy) {
    this.accessPolicy = accessPolicy;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    RBACRoleState that = (RBACRoleState) o;
    return Objects.equals(name, that.name) && Objects.equals(accessPolicy, that.accessPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, accessPolicy);
  }

  @Override
  public String toString() {
    return "RBACRoleState{" + "name='" + name + '\'' + ", accessPolicy=" + accessPolicy + '}';
  }

  private static class AccessPolicy {

    private String scopeType;

    private List<AllowedOperations> allowedOperations;

    public String getScopeType() {
      return scopeType;
    }

    public void setScopeType(String scopeType) {
      this.scopeType = scopeType;
    }

    public List<AllowedOperations> getAllowedOperations() {
      return allowedOperations;
    }

    public void setAllowedOperations(List<AllowedOperations> allowedOperations) {
      this.allowedOperations = allowedOperations;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AccessPolicy that = (AccessPolicy) o;
      return Objects.equals(scopeType, that.scopeType)
          && Objects.equals(allowedOperations, that.allowedOperations);
    }

    @Override
    public int hashCode() {
      return Objects.hash(scopeType, allowedOperations);
    }

    @Override
    public String toString() {
      return "AccessPolicy{"
          + "scopeType='"
          + scopeType
          + '\''
          + ", allowedOperations="
          + allowedOperations
          + '}';
    }
  }

  private static class AllowedOperations {

    private String resourceType;

    private List<String> operations;

    public String getResourceType() {
      return resourceType;
    }

    public void setResourceType(String resourceType) {
      this.resourceType = resourceType;
    }

    public List<String> getOperations() {
      return operations;
    }

    public void setOperations(List<String> operations) {
      this.operations = operations;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      AllowedOperations that = (AllowedOperations) o;
      return Objects.equals(resourceType, that.resourceType)
          && Objects.equals(operations, that.operations);
    }

    @Override
    public int hashCode() {
      return Objects.hash(resourceType, operations);
    }

    @Override
    public String toString() {
      return "AllowedOperations{"
          + "resourceType='"
          + resourceType
          + '\''
          + ", operations="
          + operations
          + '}';
    }
  }
}
