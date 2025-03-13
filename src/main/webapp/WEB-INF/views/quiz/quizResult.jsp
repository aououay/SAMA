<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>퀴즈 결과</title>
  <!-- Bootstrap 5 CDN -->
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
  <!-- Google Fonts (Roboto) -->
  <link href="https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap" rel="stylesheet">
  <link href="/assets/css/quizResult.css" rel="stylesheet">
</head>
<body>
  <!-- 네비게이션 바 (퀴즈 페이지와 통일감을 주기 위해) -->
  <nav class="navbar navbar-expand-lg navbar-dark" style="background-color: #343a40;">
    <div class="container">
      <a class="navbar-brand" href="#">재난 대처 퀴즈</a>
    </div>
  </nav>

  <div class="container">
    <div class="card result-card">
      <div class="card-header">
        <h2>${userName}님의 성향 분석 결과</h2>
      </div>
      <div class="card-body text-center">
        <!-- 이미지가 카드 헤더 바로 아래에 위치 -->
        <img src="${imageUrl}" alt="${result}" class="img-fluid mb-4" style="max-width: 300px;">
        <h3 class="text-success mb-4">결과: ${result}</h3>
        <p class="mb-4">${description}</p>
        <a href="${pageContext.request.contextPath}/quiz/quiz" class="btn btn-primary">다시 퀴즈 풀기</a>
      </div>
    </div>
  </div>

  <!-- Bootstrap JS (Popper 포함) -->
  <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
