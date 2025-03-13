package com.pcwk.ehr.videopost.mapper;

import com.pcwk.ehr.videopost.domain.VideoPostVO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface VideoPostMapper {
    void insertVideoPost(VideoPostVO videoPost);
    VideoPostVO getVideoPostById(String videoUuid);
    List<VideoPostVO> getAllVideoPosts();
    
    // 검색 기능 추가
    List<VideoPostVO> searchVideos(@Param("searchType") String searchType, @Param("searchKeyword") String searchKeyword);

    // 🔹 페이징을 위한 메서드 추가
    List<VideoPostVO> getVideosWithPagination(@Param("offset") int offset, @Param("pageSize") int pageSize);

    // 🔹 전체 데이터 개수 조회
    int getTotalVideoCount();
}
