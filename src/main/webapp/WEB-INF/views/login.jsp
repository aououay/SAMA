<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<c:set var="CP" value="${pageContext.request.contextPath}" />
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>로그인 페이지</title>
    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="/assets/css/login.css" rel="stylesheet">
</head>
<body>
	<div class="logo">
        <img style="cursor: pointer;" src="${CP}/assets/images/logo/3652.png" alt="서울365 안전포털" class="custom-logo-img" onclick="location.href='${CP}/index'">		
    </div>
	
    <div class="login-container text-center">
        <h2>로그인</h2>
        <!-- 로그인 폼 -->
        <form action="login.do" method="POST">
            <input type="text" name="userId" class="form-control" placeholder="아이디를 입력하세요." required />
            <input type="password" name="password" class="form-control" placeholder="비밀번호를 입력하세요." required />
            <button type="submit" class="btn btn-login">로그인</button>
        </form>

        <!-- 추가 링크: 비밀번호 찾기, 아이디 찾기, 회원가입 -->
        <div class="links">
            <a href="/login/changePassword.do" type="button">비밀번호 찾기</a>
            <span class="divider"></span> 
            <a href="/email/id-find" type="button">아이디 찾기</a>
            <span class="divider"></span> 
            <a href="/main_user/user_reg_index.do" type="button" value="등록" >회원가입</a>
        </div>

        <!-- 로그인 오류 메시지 -->
        <div class="message">
            <c:if test="${not empty errorMessage}">
                <p>${errorMessage}</p>
            </c:if>
        </div>
    </div>

    <!-- Bootstrap JS (Optional) -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>