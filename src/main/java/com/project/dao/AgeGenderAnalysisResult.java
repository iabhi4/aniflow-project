package com.project.dao;

import java.io.Serializable;

public class AgeGenderAnalysisResult implements Serializable {
    //DAO to store the analysis of age gender distribution across the dataset
    public static final long serialVersionUID = 1003L;

    private long age;
    private long male;

    private long female;


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

    public long getMale() {
        return male;
    }

    public void setMale(long male) {
        this.male = male;
    }

    public long getFemale() {
        return female;
    }

    public void setFemale(long female) {
        this.female = female;
    }

}
