package com.pcwk.ehr.cmn;

public class SearchBoardVO extends BoardDTO {

    // 기존 검색구분: 제목(10), 내용(20), 제목+내용(30)
    public static final String SEARCH_TITLE = "10";
    public static final String SEARCH_CONTENT = "20";
    public static final String SEARCH_TITLE_CONTENT = "30";
    
    // 추가: 이메일 검색
    public static final String SEARCH_EMAIL = "EMAIL";
    
    private String searchDiv;
    private String searchWord;

    public SearchBoardVO() {
        super();
    }

    public String getSearchDiv() {
        return searchDiv;
    }

    public void setSearchDiv(String searchDiv) {
        this.searchDiv = searchDiv;
    }

    public String getSearchWord() {
        return searchWord;
    }

    public void setSearchWord(String searchWord) {
        this.searchWord = searchWord;
    }

    @Override
    public String toString() {
        return "SearchBoardVO [searchDiv=" + searchDiv + ", searchWord=" + searchWord + ", toString()=" + super.toString() + "]";
    }
}

