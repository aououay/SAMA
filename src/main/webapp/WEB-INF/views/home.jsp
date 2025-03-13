<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>

<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>메인 페이지</title>
    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <!-- 네비게이션 바 -->
    <nav class="navbar navbar-expand-lg navbar-light bg-light">
         <div class="container">
             <a class="navbar-brand" href="${pageContext.request.contextPath}/home">홈</a>
             <div class="collapse navbar-collapse">
                 <ul class="navbar-nav ms-auto">
                     <li class="nav-item">
                         <a class="nav-link" href="${pageContext.request.contextPath}/mypage">마이페이지</a>
                     </li>
                     <li class="nav-item">
		    			 <c:choose>
			               <c:when test="${empty sessionScope.user.name }">
			                <li  class="nav-item"><a class="nav-link" href="${pageContext.request.contextPath}/login/login.do">로그인</a></li>
			               </c:when> 
				           <c:otherwise>
				             <li  class="nav-item"><a class="nav-link" href="${pageContext.request.contextPath}/logout">로그아웃</a></li>
				           </c:otherwise>
			            </c:choose>
                     </li>
                 </ul>
             </div>
         </div>
    </nav>
    
    <!-- 메인 콘텐츠 -->
    <div class="container mt-5">
         <h1>메인 페이지</h1>
         <p>로그인 후 접근 가능한 메인 페이지입니다. 여기서 원하는 페이지로 부드럽게 이동할 수 있습니다!</p>
    </div>
    
    <!-- Bootstrap JS (옵션) -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
