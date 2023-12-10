package com.project.dao;

import java.io.Serializable;

public class AgeAnalysisResult implements Serializable{
//DAO to store the result of age distribution across the dataset
    public static final long serialVersionUID = 1001L;

    private long age;
    private long count;


    /*public GenderAnalysisResult(String gender, double percentage) {
        this.gender = gender;
        this.percentage = percentage;
    }*/

    public long getAge() {
        return age;
    }

    public void setAge(long age) {
        this.age = age;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}

