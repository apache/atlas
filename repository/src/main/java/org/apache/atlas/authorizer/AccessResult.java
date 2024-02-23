package org.apache.atlas.authorizer;

public class AccessResult {
    private boolean isAllowed = false;
    private String policyId = "-1";

    public boolean isAllowed() {
        return isAllowed;
    }

    public void setAllowed(boolean allowed) {
        this.isAllowed = allowed;
    }

    public String getPolicyId() {
        return policyId;
    }

    public void setPolicyId(String policyId) {
        this.policyId = policyId;
    }
}
