<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width. initial-scale=1.0">

<!-- 사용자 정의 CSS -->
<link rel="stylesheet" href="/assets/css/pcwk.css">


<!-- Bootstrap -->
<link rel="stylesheet" href="/assets/css/bootstrap.min.css">

<title>Insert title here</title>
</head>
<body>
	<h2>부트 스트랩</h2>
	<div class="container">

		<p>현재 시각은 : ${dateTime}</p>
		<c:out value="${dateTime}"></c:out>
		<br>
		<br>
		<button type="button" class="btn btn-primary">버튼</button>
		<br>
		<br>
		<img alt="눈사람" src="/assets/images/snowman.jpg">
	</div>


	<!-- 사용자 정의 JS -->
	<script src="/assets/js/pcwk.js"></script>

	<!-- Bootstrap -->
	<script src="/assets/js/bootstrap.min.js"></script>

</body>
</html>