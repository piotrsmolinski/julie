package com.purbon.kafka.topology.model;

/**
 * Low level Access Control List (ACL) entry. The data corresponds to the content of {@link
 * org.apache.kafka.common.acl.AclBinding}.
 */
public class ACLEntry {

  private String host = "*";

  private String resource;

  private String resourceType;

  private String patternType = "LITERAL";

  private String operation;

  private String permissionType = "ALLOW";

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
  public String toString() {
    return "ACLEntry{"
        + "host='"
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

  public static class Builder {

    private String host = "*";

    private String resource;

    private String resourceType;

    private String patternType = "LITERAL";

    private String operation;

    private String permissionType = "ALLOW";

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

    public ACLEntry build() {
      ACLEntry entry = new ACLEntry();
      entry.setHost(this.host);
      entry.setResource(this.resource);
      entry.setResourceType(this.resourceType);
      entry.setPatternType(this.patternType);
      entry.setOperation(this.operation);
      entry.setPermissionType(this.permissionType);
      return entry;
    }
  }
}
