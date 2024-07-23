package com.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class Vendor {

    @QuerySqlField(index = true)
    private Integer id;
    @QuerySqlField
    private String entityChangeAction;
    @QuerySqlField
    private String active;
    @QuerySqlField
    private Long accountID;
    private String fullName;

    public Vendor() {
    }

    public Vendor(Integer id, String entityChangeAction, String active, Long accountID, String fullName) {
        this.id = id;
        this.entityChangeAction = entityChangeAction;
        this.active = active;
        this.accountID = accountID;
        this.fullName = fullName;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getEntityChangeAction() {
        return entityChangeAction;
    }

    public void setEntityChangeAction(String entityChangeAction) {
        this.entityChangeAction = entityChangeAction;
    }

    public String getActive() {
        return active;
    }

    public void setActive(String active) {
        this.active = active;
    }

    public Long getAccountID() {
        return accountID;
    }

    public void setAccountID(Long accountID) {
        this.accountID = accountID;
    }

    public String getFullName() {
        return fullName;
    }

    public void setFullName(String fullName) {
        this.fullName = fullName;
    }


    @Override
    public String toString() {
        return "Vendor{" +
                "id=" + id +
                ", entityChangeAction='" + entityChangeAction + '\'' +
                ", active='" + active + '\'' +
                ", accountID=" + accountID +
                ", fullName='" + fullName + '\'' +
                '}';
    }


}
