package com.pcwk.ehr.videopost.service;

import java.util.List;
import com.pcwk.ehr.videopost.domain.VideoPostVO;

public interface VideoPostService {
    void insertVideoPost(VideoPostVO videoPost);
    VideoPostVO getVideoPostById(String videoUuid);
    List<VideoPostVO> getAllVideoPosts();
    List<VideoPostVO> searchVideos(String searchType, String searchKeyword);

    // 🔹 페이징 기능 추가
    int getTotalVideoCount();
    List<VideoPostVO> getVideoPostsByPage(int page, int pageSize);
}
