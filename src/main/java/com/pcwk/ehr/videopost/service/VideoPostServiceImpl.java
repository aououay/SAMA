package com.pcwk.ehr.videopost.service;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.pcwk.ehr.videopost.domain.VideoPostVO;
import com.pcwk.ehr.videopost.mapper.VideoPostMapper;

@Service
public class VideoPostServiceImpl implements VideoPostService {

    @Autowired
    private VideoPostMapper videoPostMapper;

    @Override
    public void insertVideoPost(VideoPostVO videoPost) {
        String encodedUrl = encodeUrl(videoPost.getVideoOriginalUrl());
        videoPost.setVideoEncodedUrl(encodedUrl);
        videoPostMapper.insertVideoPost(videoPost);
    }

    @Override
    public VideoPostVO getVideoPostById(String videoUuid) {
        return videoPostMapper.getVideoPostById(videoUuid);
    }

    @Override
    public List<VideoPostVO> getAllVideoPosts() {
        return videoPostMapper.getAllVideoPosts();
    }

    @Override
    public List<VideoPostVO> searchVideos(String searchType, String searchKeyword) {
        return videoPostMapper.searchVideos(searchType, searchKeyword);
    }

    @Override
    public int getTotalVideoCount() {
        return videoPostMapper.getTotalVideoCount();
    }

    @Override
    public List<VideoPostVO> getVideoPostsByPage(int page, int pageSize) {
        int offset = (page - 1) * pageSize;
        return videoPostMapper.getVideosWithPagination(offset, pageSize);
    }

    private String encodeUrl(String url) {
        try {
            return URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
            return url;
        }
    }
}
