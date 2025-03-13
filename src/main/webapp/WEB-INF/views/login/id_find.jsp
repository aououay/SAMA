<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <title>이메일 인증 및 아이디 찾기</title>
  <link href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css" rel="stylesheet">
  <link href="/assets/css/find.css" rel="stylesheet">
  <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
</head>
<body>

<div class="container mt-5" style="max-width: 500px;">
  <div class="p-4">
    <h2 class="text-center mb-4">이메일 인증 및 아이디 찾기</h2>
    
    <!-- 이메일 인증 섹션 -->
    <div id="emailVerificationSection" class="bg-light p-4 mb-3">
      <h3 class="card-title">1. 이메일 인증</h3>
      <form id="emailVerificationForm">
        <div class="form-group">
          <label for="email">이메일:</label>
          <input type="email" class="form-control" id="email" name="email" required placeholder="이메일 입력">
        </div>
        <button type="button" id="sendAuthCodeButton" class="custom-btn">인증 코드 전송</button>
      </form>
      <p id="authCodeSentMessage" class="mt-3 text-success"></p>
      <div id="verifyAuthCodeSection" style="display:none;">
        <div class="form-group mt-3">
          <label for="authCode">인증 코드:</label>
          <input type="text" class="form-control" id="authCode" name="authCode" required placeholder="인증 코드 입력">
        </div>
        <button type="button" id="verifyAuthCodeButton" class="custom-btn">인증 코드 확인</button>
      </div>
    </div>

    <!-- 아이디 출력 섹션 -->
    <div id="idDisplaySection" class="bg-light p-4" style="display:none;">
      <h3 class="card-title">회원 아이디</h3>
      <p id="idDisplayMessage" class="mt-3 text-info"></p>
    </div>

    <!-- 로그인 페이지로 이동 버튼 -->
    <div class="btn-container text-center mt-4">
      <a href="/login/login.do" class="gray-btn">로그인 페이지로 이동</a>
    </div>
  </div>
</div>

<script src="https://cdn.jsdelivr.net/npm/bootstrap@4.5.2/js/bootstrap.min.js"></script>
<script src="/assets/js/id_find.js"></script>

</body>
</html>
