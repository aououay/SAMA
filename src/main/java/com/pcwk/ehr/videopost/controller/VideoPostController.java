package com.pcwk.ehr.videopost.controller;

import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

import com.pcwk.ehr.videopost.domain.VideoPostVO;
import com.pcwk.ehr.videopost.service.VideoPostService;

@Controller
@RequestMapping("/videopost")
public class VideoPostController {

    @Autowired
    private VideoPostService videoPostService;

    @GetMapping("/list")
    public String list(
        @RequestParam(value = "searchType", required = false) String searchType,
        @RequestParam(value = "searchKeyword", required = false) String searchKeyword,
        @RequestParam(value = "page", required = false, defaultValue = "1") int page,
        Model model) {

        int pageSize = 10;
        int totalVideos = videoPostService.getTotalVideoCount();
        int totalPages = (int) Math.ceil((double) totalVideos / pageSize);

        List<VideoPostVO> videoList;
        if (searchKeyword != null && !searchKeyword.trim().isEmpty()) {
            videoList = videoPostService.searchVideos(searchType, searchKeyword);
        } else {
            videoList = videoPostService.getVideoPostsByPage(page, pageSize);
        }

        model.addAttribute("videoList", videoList);
        model.addAttribute("page", page);
        model.addAttribute("totalPages", totalPages);
        model.addAttribute("hasNext", page < totalPages);

        return "videopost/list";
    }

    @GetMapping("/detail/{videoUuid}")
    public String videoDetail(@PathVariable("videoUuid") String videoUuid, Model model) {
        VideoPostVO video = videoPostService.getVideoPostById(videoUuid);
        if (video == null) {
            return "redirect:/error/404";
        }
        model.addAttribute("video", video);
        return "videopost/detail";
    }

    @GetMapping("/upload")
    public String showUploadForm() {
        return "videopost/upload";
    }

    @PostMapping("/upload")
    public String uploadVideo(
        @RequestParam("videoTitle") String videoTitle,
        @RequestParam("videoDescription") String videoDescription,
        @RequestParam("videoUrl") String videoUrl,
        @RequestParam(value = "thumbnailUrl", required = false) String thumbnailUrl,
        @RequestParam("videoCategory") String videoCategory,
        Model model) {

        try {
            String videoUuid = UUID.randomUUID().toString();
            String youtubeId = videoUrl.trim();
            VideoPostVO videoPost = new VideoPostVO(videoUuid, videoTitle, videoDescription, thumbnailUrl, youtubeId, videoCategory);
            videoPostService.insertVideoPost(videoPost);
            return "redirect:/videopost/list";
        } catch (Exception e) {
            model.addAttribute("message", "업로드 중 오류가 발생했습니다.");
            e.printStackTrace();
        }
        return "videopost/upload";
    }
}
