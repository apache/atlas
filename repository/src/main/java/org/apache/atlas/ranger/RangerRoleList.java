package org.apache.atlas.ranger;

import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.ranger.plugin.model.RangerRole;

import java.util.ArrayList;
import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class RangerRoleList extends VList {

    private static final long serialVersionUID = 1L;

    List<RangerRole> roles = new ArrayList<RangerRole>();

    public RangerRoleList() {
        super();
    }

    public RangerRoleList(List<RangerRole> objList) {
        this.roles = objList;
    }

    public List<RangerRole> getSecurityRoles() {
        return roles;
    }

    public void setRoleList(List<RangerRole> roles) {
        this.roles = roles;
    }

    public List<RangerRole> getRoles() {
        return roles;
    }

    public void setRoles(List<RangerRole> roles) {
        this.roles = roles;
    }

    public int getListSize() {
        if (roles != null) {
            return roles.size();
        }
        return 0;
    }

    public List<RangerRole> getList() {
        return roles;
    }
}