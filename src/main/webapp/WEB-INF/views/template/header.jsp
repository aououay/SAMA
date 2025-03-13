<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<c:set var="CP" value="${pageContext.request.contextPath}" />

<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<title>서울365 안전포털</title>
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/gh/projectnoonnu/noonfonts@1.0.0/NanumSquare.css">
<!-- Bootstrap CSS -->
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css"
	integrity="sha384-rbsA2VBKQ6qn7R0vQ1ty9+T2TzpunpZK+F0bJ7CE2X4zj52ekMz9sJ7l9y19g1Qz"
	crossorigin="anonymous">

<!-- 커스텀 CSS -->
<link rel="stylesheet" href="${CP}/assets/css/custom-header.css">
</head>
<body>

	<!-- 헤더 영역 시작 -->
	<header id="custom-header">
		<div class="top-bar-black">
			<div class="container top-bar-content">
				<div class="right-info">
					<c:if test="${not empty sessionScope.user.name}">
						<span>${sessionScope.user.nickname }님 환영합니다.</span>
					</c:if>
					<a href="${CP}/mypage">마이페이지</a>
					<c:choose>
						<c:when test="${empty sessionScope.user.name }">
							<a href="${CP}/login/login.do">로그인</a>
						</c:when>
						<c:otherwise>
							<a href="${CP}/logout">로그아웃</a>
						</c:otherwise>
					</c:choose>
				</div>
			</div>
		</div>

		<div class="nav-bar-navy">
			<div
				class="container d-flex align-items-center justify-content-between py-2">
				<!-- 로고 -->
				<div class="logo">
					<a class="navbar-brand" href="${CP}/index"> <img
						src="${CP}/assets/images/logo/3652.png" alt="서울365 안전포털"
						class="custom-logo-img">
					</a>
				</div>

				<!-- 네비게이션 메뉴 -->
				<nav class="main-nav">
					<ul class="d-flex mb-0">
						<!-- 재난현황 -->
						<li class="nav-item custom-dropdown" style="cursor: pointer;"><a
							class="nav-link custom-dropdown-toggle">재난현황</a>
							<ul class="custom-dropdown-menu">
								<li><a class="custom-dropdown-item"
									href="${CP}/rainfall/view">호우</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/waterlevel/view">홍수</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/earthquake/view">지진</a></li>
								<li><a class="custom-dropdown-item" href="${CP}/typ/view">태풍</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/disastermessage/view">재난문자</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/accident-stats">주요재난통계</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/disastermessage/safety_Index">재난안전지수</a></li>
							</ul></li>

						<!-- 서울날씨 -->
						<li class="nav-item custom-dropdown" style="cursor: pointer;"><a
							class="nav-link custom-dropdown-toggle">서울날씨</a>
							<ul class="custom-dropdown-menu">
								<li><a class="custom-dropdown-item" href="${CP}/weather">날씨</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/air_quality">대기환경</a></li>
							</ul></li>

						<!-- 재난안전시설 -->
						<li class="nav-item custom-dropdown" style="cursor: pointer;"><a
							class="nav-link custom-dropdown-toggle">재난안전시설</a>
							<ul class="custom-dropdown-menu">
								<li><a class="custom-dropdown-item"
									href="${CP}/acmdfclty/view">지진 옥외 대피 장소</a></li>
								<li><a class="custom-dropdown-item" href="${CP}/hanpa/view">한파
										응급 대피소</a></li>
								<li><a class="custom-dropdown-item" href="${CP}/faclt/view">비상
										대피 시설</a></li>
								<li><a class="custom-dropdown-item" href="${CP}/ewsf/view">급수
										시설</a></li>
							</ul></li>

						<!-- 재난대비 -->
						<li class="nav-item custom-dropdown" style="cursor: pointer;"><a
							class="nav-link custom-dropdown-toggle">재난대비</a>
							<ul class="custom-dropdown-menu">
								<li><a class="custom-dropdown-item" href="${CP}/siren">재난
										경보 소리 테스트</a></li>
								<li><a class="custom-dropdown-item" href="${CP}/checklist">생존
										필수품 추천 및 체크리스트</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/videopost/list">재난 훈련 영상</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/disasterguide/finedust">자연재난 행동 요령 및 예방법</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/disasterguide/fire">사회재난 행동 요령 및 예방법</a></li>
							</ul></li>

						<!-- 시민안전보험 -->
						<li class="nav-item custom-dropdown" style="cursor: pointer;"><a
							class="nav-link custom-dropdown-toggle">시민안전보험</a>
							<ul class="custom-dropdown-menu">
								<li><a class="custom-dropdown-item"
									href="${CP}/ins/intro.do">시민안전보험 소개</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/ins/getIns.do">시민안전보험 조회</a></li>
							</ul></li>

						<!-- 응급의료기관 -->
						<li class="nav-item custom-dropdown" style="cursor: pointer;"><a
							class="nav-link custom-dropdown-toggle">응급의료기관</a>
							<ul class="custom-dropdown-menu">
								<li><a class="custom-dropdown-item"
									href="${CP}/er_medi_inst/list">응급의료기관 정보</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/er_medi_inst/er_bed">응급실 가용병상 현황</a></li>
							</ul></li>

						<!-- 재난안전게시판 -->
						<li class="nav-item custom-dropdown" style="cursor: pointer;"><a
							class="nav-link custom-dropdown-toggle">재난안전게시판</a>
							<ul class="custom-dropdown-menu">
								<li><a class="custom-dropdown-item"
									href="${CP}/board/doRetrieve.do?div=10">공지사항</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/board/doRetrieve.do?div=20">자유게시판</a></li>
								<li><a class="custom-dropdown-item"
									href="${CP}/board/doRetrieve.do?div=30">통계자료실</a></li>
							</ul></li>
					</ul>
				</nav>
			</div>
		</div>
	</header>
	<!-- 헤더 영역 끝 -->

	<!-- JS -->
	<script
		src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.bundle.min.js"
		integrity="sha384-kenU1KFdBIe4zVF0s0G1M5b4hcpxyD9F7jL+AM5uNnE23ZxOfJmQNE2pGf3H1kP4"
		crossorigin="anonymous"></script>

	<!-- 마우스 오버 드롭다운 JS -->
	<script>
		document.addEventListener('DOMContentLoaded', function() {
			let dropdowns = document.querySelectorAll('.custom-dropdown');
			dropdowns.forEach(function(dropdown) {
				let menu = dropdown.querySelector('.custom-dropdown-menu');
				dropdown.addEventListener('mouseenter', function() {
					menu.style.opacity = "1";
					menu.style.transform = "translateY(0)";
					menu.style.visibility = "visible";
				});
				dropdown.addEventListener('mouseleave', function() {
					menu.style.opacity = "0";
					menu.style.transform = "translateY(-10px)";
					menu.style.visibility = "hidden";
				});
			});
		});
	</script>

</body>
</html>
