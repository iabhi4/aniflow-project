package com.project.dao;

import java.io.Serializable;
public class AnimeScoreAnalysisResult implements Serializable{
    //DAO to store the analysis result of average score of different animes
    public static final long serialVersionUID = 1004L;

    private long anime_id;

    private double average_score;

    public long getAnimeId() {
        return anime_id;
    }

    public void setAnimeId(long anime_id) {
        this.anime_id = anime_id;
    }

    public double getAverageScore() {
        return average_score;
    }

    public void setAverageScore(double average_score) {
        this.average_score = average_score;
    }
}
