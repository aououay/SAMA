package com.pcwk.ehr.cmn;

public class DTO {

    private int no; // 글 번호
    private int totalCnt; // 총 글 수
    private int pageSize; // 페이지 사이즈
    private int pageNo; // 페이지 번호

    // 페이지 처리 관련 추가
    private int startRow;
    private int endRow;

    public void setPaging() {
        this.startRow = (pageNo - 1) * pageSize;
        this.endRow = startRow + pageSize;
    }
    public DTO() {

    }

    public int getNo() {
        return no;
    }

    public void setNo(int no) {
        this.no = no;
    }

    public int getTotalCnt() {
        return totalCnt;
    }

    public void setTotalCnt(int totalCnt) {
        this.totalCnt = totalCnt;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getPageNo() {
        return pageNo;
    }

    public void setPageNo(int pageNo) {
        this.pageNo = pageNo;
    }

    public int getStartRow() {
        return startRow;
    }

    public void setStartRow(int startRow) {
        this.startRow = startRow;
    }

    public int getEndRow() {
        return endRow;
    }

    public void setEndRow(int endRow) {
        this.endRow = endRow;
    }

    @Override
    public String toString() {
        return "DTO{" +
                "no=" + no +
                ", totalCnt=" + totalCnt +
                ", pageSize=" + pageSize +
                ", pageNo=" + pageNo +
                ", startRow=" + startRow +
                ", endRow=" + endRow +
                '}';
    }
}
