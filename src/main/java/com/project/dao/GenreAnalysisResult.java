package com.project.dao;

import java.io.Serializable;

public class GenreAnalysisResult implements Serializable {
    //DAO to store the analysis result of all genres and also studio portfolio genres.
    public static final long serialVersionUID = 1006L;

    private String genre_name;

    private long count;

    public String getGenreName() {
        return genre_name;
    }

    public void setGenreName(String genre_name) {
        this.genre_name = genre_name;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }
}
