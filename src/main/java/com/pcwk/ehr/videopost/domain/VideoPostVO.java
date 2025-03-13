package com.pcwk.ehr.videopost.domain;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Date;

public class VideoPostVO {
    private String videoUuid;
    private String videoTitle;
    private String videoDescription;
    private String videoThumbnailUrl;
    private String videoOriginalUrl;
    private String videoEncodedUrl; // URL 인코딩 저장
    private Date videoUploaded;
    private String videoCategory;

    // 기본 생성자
    public VideoPostVO() {}

    // 모든 필드를 포함하는 생성자
    public VideoPostVO(String videoUuid, String videoTitle, String videoDescription, String videoThumbnailUrl,
            String videoOriginalUrl, String videoEncodedUrl, Date videoUploaded, String videoCategory) {
        this.videoUuid = videoUuid;
        this.videoTitle = videoTitle;
        this.videoDescription = videoDescription;
        this.videoThumbnailUrl = videoThumbnailUrl;
        this.videoOriginalUrl = videoOriginalUrl;
        this.videoEncodedUrl = videoEncodedUrl != null ? videoEncodedUrl : ""; // NULL 방지
        this.videoUploaded = videoUploaded;
        this.videoCategory = videoCategory;
    }

    // 업로드 생성자
    public VideoPostVO(String videoUuid, String videoTitle, String videoDescription, String videoThumbnailUrl,
            String videoOriginalUrl, String videoCategory) {
        this.videoUuid = videoUuid;
        this.videoTitle = videoTitle;
        this.videoDescription = videoDescription;
        this.videoThumbnailUrl = videoThumbnailUrl;
        this.videoOriginalUrl = videoOriginalUrl;

        // 유튜브 URL 인코딩
        this.videoEncodedUrl = encodeUrl(videoOriginalUrl);

        this.videoUploaded = new Date(); // 업로드 날짜 설정
        this.videoCategory = videoCategory;
    }

    // URL Encoding 메서드 추가
    private String encodeUrl(String url) {
        try {
            return URLEncoder.encode(url, StandardCharsets.UTF_8.toString());
        } catch (Exception e) {
            e.printStackTrace();
            return url; // 인코딩 실패 시 원본 URL 반환
        }
    }

    // Getter & Setter
    public String getVideoUuid() {
        return videoUuid;
    }

    public void setVideoUuid(String videoUuid) {
        this.videoUuid = videoUuid;
    }

    public String getVideoTitle() {
        return videoTitle;
    }

    public void setVideoTitle(String videoTitle) {
        this.videoTitle = videoTitle;
    }

    public String getVideoDescription() {
        return videoDescription;
    }

    public void setVideoDescription(String videoDescription) {
        this.videoDescription = videoDescription;
    }

    public String getVideoThumbnailUrl() {
        return videoThumbnailUrl;
    }

    public void setVideoThumbnailUrl(String videoThumbnailUrl) {
        this.videoThumbnailUrl = videoThumbnailUrl;
    }

    public String getVideoOriginalUrl() {
        return videoOriginalUrl;
    }

    public void setVideoOriginalUrl(String videoOriginalUrl) {
        this.videoOriginalUrl = videoOriginalUrl;
        this.videoEncodedUrl = encodeUrl(videoOriginalUrl); // URL 변경 시 자동 인코딩
    }

    public String getVideoEncodedUrl() {
        return videoEncodedUrl;
    }

    public void setVideoEncodedUrl(String videoEncodedUrl) {
        this.videoEncodedUrl = videoEncodedUrl;
    }

    public Date getVideoUploaded() {
        return videoUploaded;
    }

    public void setVideoUploaded(Date videoUploaded) {
        this.videoUploaded = videoUploaded;
    }

    public String getVideoCategory() {
        return videoCategory;
    }

    public void setVideoCategory(String videoCategory) {
        this.videoCategory = videoCategory;
    }

    @Override
    public String toString() {
        return "VideoPostVO [videoUuid=" + videoUuid + ", videoTitle=" + videoTitle + ", videoDescription="
                + videoDescription + ", videoThumbnailUrl=" + videoThumbnailUrl + ", videoOriginalUrl="
                + videoOriginalUrl + ", videoEncodedUrl=" + videoEncodedUrl + ", videoUploaded=" + videoUploaded
                + ", videoCategory=" + videoCategory + "]";
    }
}
