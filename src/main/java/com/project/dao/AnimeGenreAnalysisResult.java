package com.project.dao;

import java.io.Serializable;

public class AnimeGenreAnalysisResult implements Serializable {
    //DAO to store the analysis result of best anime
    public static final long serialVersionUID = 1005L;
    private long anime_id;


    private String anime_genre;

    public long getAnimeId() {
        return anime_id;
    }

    public void setAnimeId(long anime_id) {
        this.anime_id = anime_id;
    }


    public String getAnimeGenre() {
        return anime_genre;
    }

    public void setAnimeGenre(String anime_genre) {
        this.anime_genre = anime_genre;
    }
}
