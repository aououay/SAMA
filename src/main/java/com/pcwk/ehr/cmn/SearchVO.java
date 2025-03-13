package com.pcwk.ehr.cmn;

public class SearchVO extends DTO {

    // 검색 구분: 사용자ID(10), 이름(20), email(30)
    private int searchDiv;
    // 검색어:
    private String searchWord;

    public SearchVO() {
        super();
    }

    public SearchVO(int searchDiv, String searchWord) {
        super();
        this.searchDiv = searchDiv;
        this.searchWord = searchWord;
    }

    public int getSearchDiv() {
        return searchDiv;
    }

    public void setSearchDiv(int searchDiv) {
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
        return "SearchVO [searchDiv=" + searchDiv + ", searchWord=" + searchWord + ", getPageSize()=" + getPageSize()
                + ", getPageNo()=" + getPageNo() + "]";
    }

}
