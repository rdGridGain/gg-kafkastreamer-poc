package com.model;

import org.apache.ignite.cache.query.annotations.QuerySqlField;

public class VendorAgg {

    @QuerySqlField(index = true)
    private Long accountID;
    @QuerySqlField
    private String fullName;
    @QuerySqlField
    private Integer counter;

    public VendorAgg(Long accountID, String fullName, Integer counter) {
        this.accountID = accountID;
        this.fullName = fullName;
        this.counter = counter;
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

    public Integer getCounter() {
        return counter;
    }

    public void setCounter(Integer counter) {
        this.counter = counter;
    }

    @Override
    public String toString() {
        return "VendorAgg{" +
                "accountID=" + accountID +
                ", fullName='" + fullName + '\'' +
                ", counter=" + counter +
                '}';
    }
}
