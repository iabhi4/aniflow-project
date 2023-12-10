package com.project.dao;

import java.io.Serializable;

public class GenderAnalysisResult implements Serializable{
    //DAO to store the analysis result of gender distribution across the dataset

    public static final long serialVersionUID = 1002L;

    private String gender;
    private double percentage;


    /*public GenderAnalysisResult(String gender, double percentage) {
        this.gender = gender;
        this.percentage = percentage;
    }*/

    public String getGender() {
        return gender;
    }

    public void setGender(String gender) {
        this.gender = gender;
    }

    public double getPercentage() {
        return percentage;
    }

    public void setPercentage(double percentage) {
        this.percentage = percentage;
    }
}
