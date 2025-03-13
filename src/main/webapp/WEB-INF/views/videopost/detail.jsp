<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>

<html>
<head>
    <title>${video.videoTitle}</title>
        <meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet" >
<link href="/assets/css/header.css" rel="stylesheet" >
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<script src="/assets/js/jquery_3_7_1.js"></script>
    <link rel="stylesheet" type="text/css" href="/assets/css/videodetail.css">
</head>
	
<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

<div class="video-detail-container">
    <h1 class="video-title">${video.videoTitle}</h1>
    
    <p class="video-date">작성일: 
        <fmt:formatDate value="${video.videoUploaded}" pattern="yyyy-MM-dd HH:mm"/>
    </p>

    <!-- 유튜브 영상 ID 가져오기 -->
    <c:set var="videoId" value="${video.videoOriginalUrl}" />

    <!-- 📌 영상 플레이어 -->
    <div class="video-player">
        <iframe width="800" height="450" 
                src="https://www.youtube.com/embed/${videoId}" 
                frameborder="0" 
                allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" 
                referrerpolicy="strict-origin-when-cross-origin" 
                allowfullscreen>
        </iframe>
    </div>

    <!-- 📌 영상 설명 -->
    <div class="video-description">
        <p>${video.videoDescription}</p>
    </div>

    <!-- 🔙 목록으로 돌아가기 버튼 -->
    <div class="back-button">
        <a href="<c:url value='/videopost/list' />">목록으로 돌아가기</a>
    </div>
</div>
	<div class="container-fluid px-0">
	<!-- footer-->
	<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
	<!--// footer-------------------------------------------------->
	</div>
</body>
</html>
