<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>

<html>
<head>
    <title>영상 목록</title>
    <meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet" >
<link href="/assets/css/header.css" rel="stylesheet" >
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<script src="/assets/js/jquery_3_7_1.js"></script>
<link rel="stylesheet" type="text/css" href="/assets/css/videolist.css">
</head>



<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>
	
<div class="container">
<p class="heading-title">재난 훈련 영상</p>
<hr>
<div class="search-bar">
    <form action="<c:url value='/videopost/list' />" method="get">
        <select name="searchType">
            <option value="title">제목</option>
            <option value="description">내용</option>
        </select>
        <input type="text" name="searchKeyword" placeholder="검색어 입력">
        <button type="submit">검색</button>
    </form>
</div>
<div class="video-container">

    <c:forEach var="video" items="${videoList}">
        <div class="video-item">
            <a href="<c:url value='/videopost/detail/${video.videoUuid}' />">

                <!-- 썸네일 우선순위: 1) DB에 저장된 썸네일, 2) 유튜브 자동 생성 썸네일 -->
                <c:choose>
                    <c:when test="${not empty video.videoThumbnailUrl}">
                        <img src="${video.videoThumbnailUrl}" alt="${video.videoTitle}">
                    </c:when>
                    <c:otherwise>
                        <img src="https://img.youtube.com/vi/${video.videoOriginalUrl}/mqdefault.jpg" 
                             onerror="this.onerror=null; this.src='/assets/images/default-thumbnail.jpg';" 
                             alt="유튜브 썸네일">
                    </c:otherwise>
                </c:choose>

                <p>${video.videoTitle}</p>
            </a>
        </div>
    </c:forEach>
</div>

<div class="video-pagination-separator"></div>

<!-- 페이지네이션 -->
<div class="pagination">
    <c:if test="${page > 1}">
        <a href="<c:url value='/videopost/list?page=${page - 1}&searchType=${searchType}&searchKeyword=${searchKeyword}' />">&laquo; 이전</a>
    </c:if>

    <span>${page} / ${totalPages}</span>

    <c:if test="${hasNext}">
        <a href="<c:url value='/videopost/list?page=${page + 1}&searchType=${searchType}&searchKeyword=${searchKeyword}' />">다음 &raquo;</a>
    </c:if>
</div>
</div>

	<div class="container-fluid px-0">
	<!-- footer-->
	<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
	<!--// footer-------------------------------------------------->
	</div>
<script src="/assets/js/bootstrap.min.js"></script>
</body>
</html>
