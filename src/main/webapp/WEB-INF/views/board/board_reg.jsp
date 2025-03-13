<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8" %>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt" %>
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <!-- Bootstrap 및 헤더 CSS -->
  <link href="/assets/css/bootstrap.min.css" rel="stylesheet">
  <link href="/assets/css/header.css" rel="stylesheet">
  <link href="/assets/css/footer.css" rel="stylesheet">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
  <title>게시글 등록</title>
  <script src="/assets/js/jquery_3_7_1.js"></script>
  <link rel="icon" href="data:;base64,iVBORw0KGgo=">
  
</head>
<body>
    <div class="">
        <!-- header-->
        <jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
        <!--// header-------------------------------------------------->
    </div>
  <div id="container" class="container">
    
    <!-- main 영역 -->
    <main class="container my-4">
      <article class="col-md-12">
        <!-- 제목 영역 -->
        <div class="title-section">
          <c:choose>
            <c:when test="${ '10' == board_div }">
              <h2>공지사항</h2>
            </c:when>
            <c:when test="${ '20' == board_div }">
              <h2>자유게시판</h2>
            </c:when>
            <c:when test="${ '30' == board_div }">
              <h2>통계자료실</h2>
            </c:when>
          </c:choose>
        </div>
        <hr>
        
        <!-- form 영역 -->
        <form id="doSaveForm" action="/board/doSave.do" method="post" class="row g-3" enctype="multipart/form-data">
          <!-- 닉네임 (작성자) -->
          <div class="col-12 form-group">
            <label for="regId">닉네임</label>
            <input name="regId" id="regId" value="${sessionScope.user.nickname}" readonly class="form-control">
          </div>
          <!-- board_div 값 전달 -->
          <input name="div" id="div" type="hidden" value="${board_div}">
          
          <!-- 제목 입력 -->
          <div class="col-12 form-group">
            <label for="title">제목</label>
            <input name="title" id="title" class="form-control">
          </div>
          
          <!-- 내용 입력 -->
          <div class="col-12 form-group">
            <label for="contents">내용</label>
            <textarea name="contents" id="contentsTextArea" class="form-control" rows="6"></textarea>
          </div>
          
          <!-- 파일 업로드 -->
          <div class="col-12 form-group">
            <label for="file">파일 업로드</label>
            <input type="file" name="file" id="file" class="form-control">
          </div>
          
          <!-- 등록 버튼: 폼 영역 아래쪽에 오른쪽 정렬로 크게 배치 -->
          <div class="col-12 d-flex justify-content-end mt-3">
            <input type="button" id="doSave" value="등록" class="btn btn-primary btn-lg">
          </div>
        </form>
      </article>
    </main>
    
  </div>
  
  <!-- 사용자 정의 스크립트 -->
  <script src="/assets/js/board_reg.js"></script>
  <script src="/assets/js/common.js"></script>
  <!-- Bootstrap JS -->
  <script src="/assets/js/bootstrap.min.js"></script>
</body>
    <div class="container-fluid px-0">
        <!-- footer-->
        <jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
        <!--// footer-------------------------------------------------->
    </div>
</html>