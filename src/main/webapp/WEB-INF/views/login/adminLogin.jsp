<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>

<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>관리자 전용 로그인</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
<link href="/assets/css/adminLogin.css" rel="stylesheet">
</head>
<body>
	<div class="logo">
        <img src="${CP}/assets/images/logo/3652.png" alt="서울365 안전포털" class="custom-logo-img">		
    </div>
	<div class="container admin-login-container">
		<div class="card admin-login-card">
			<div class="card-header admin-login-card-header">관리자 전용 로그인</div>
			<div class="card-body">
				<c:if test="${not empty errorMessage}">
					<div class="error-message">${errorMessage}</div>
				</c:if>
				<form
					action="${pageContext.request.contextPath}/login/adminLogin.do"
					method="post">
					<div class="mb-3">
						<input type="text" name="userId" class="form-control"
							placeholder="아이디" required autofocus>
					</div>
					<div class="mb-3">
						<input type="password" name="password" class="form-control"
							placeholder="비밀번호" required>
					</div>
					<div class="d-grid">
						<button type="submit" class="btn btn-primary btn-login">로그인</button>
					</div>
				</form>
			</div>
		</div>
	</div>
	<!-- Bootstrap JS -->
	<script
		src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
