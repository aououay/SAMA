<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<c:set var="CP" value="${pageContext.request.contextPath}" />

<!DOCTYPE html>
<html>

<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=1200">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet">
<link href="/assets/css/header.css" rel="stylesheet">
<link href="/assets/css/index.css" rel="stylesheet">
<link href="/assets/css/hexagon.css" rel="stylesheet">
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<title>Insert title here</title>
<script src="/assets/js/jquery_3_7_1.js"></script>
</head>
<link rel="icon" href="data:;base64,iVBORw0KGgo=">
<style>
.truncate-text {
	white-space: nowrap;
	overflow: hidden;
	text-overflow: ellipsis;
	max-width: 544.478px; /* 원하는 너비 설정 */
}
</style>

<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

	<!-- main-->
	<!-- 날씨 대시보드 -->
		<div id="container" class="container">
			<div class="mt-5 weather-dashboard">
				<div
					class="header d-flex justify-content-between align-items-center">
					<h2>
						오늘 날씨 <span id="date"></span>
					</h2>
					<div class="d-flex justify-content-end align-items-center gap-3">
						<h3>서울특별시</h3>
						<div class="dropdown mb-1">
							<button class="btn btn-outline-primary dropdown-toggle"
								type="button" id="dropdownMenuButton1" data-bs-toggle="dropdown"
								aria-expanded="false">${wc.guNm}</button>
							<ul class="dropdown-menu" aria-labelledby="dropdownMenuButton1">
								<li><a class="dropdown-item" href="${CP}/index/111123">종로구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111121">중구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111131">용산구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111181">은평구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111191">서대문구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111201">마포구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111142">성동구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111141">광진구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111152">동대문구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111151">중랑구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111161">성북구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111291">강북구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111171">도봉구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111311">노원구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111301">양천구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111212">강서구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111221">구로구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111281">금천구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111231">영등포구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111241">동작구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111251">관악구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111262">서초구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111261">강남구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111273">송파구</a></li>
								<li><a class="dropdown-item" href="${CP}/index/111274">강동구</a></li>
							</ul>
						</div>
					</div>
				</div>

						<!-- 날씨 및 대기지수 카드들 -->
						<div class="row mt-3 justify-content-center">
						    <!-- 날씨 정보 -->
						    <div class="col-md-5">
						        <div class="card p-4 text-end">
						            <p class="text-start">발표일 : ${wc_Dt}</p>
						            <img class="rounded mx-auto d-block" src="https://openweathermap.org/img/wn/${wc.weatherIconId}@2x.png" height="130" width="130">
						            <p class="text-center fs-4 fw-bolder">${wc.weatherDesc}</p>
						            <div class="d-flex justify-content-center align-items-center gap-2">
						                <h1 class="pe-2 display-4">${wc.temp}°</h1>
						                <p class="fs-5 text-danger">${wc.maxTemp}°</p>
						                <p class="fs-5">/</p>
						                <p class="fs-5 text-primary">${wc.minTemp}°</p>
						            </div>
						            <div class="d-flex justify-content-center align-items-center gap-2 mt-2">
						                <small>체감 <span class="fw-bolder">${wc.feelsLikeTemp}°</span></small>
						                <small>습도 <span class="fw-bolder">${wc.humidity}%</span></small>
						                <small>풍속 <span class="fw-bolder">${wc.windSpeed}m/s</span></small>
						            </div>
						        </div>
						    </div>
						
						<!-- 대기질 정보 -->
					    <div class="col-md-6">
					        <div class="card p-4">
					            <p class="text-start mb-0">발표일 : ${aqc_Dt}</p>
					            <div class="row text-center">
					                <div class="col-6 mt-4">
					                    <p class="fw-lighter">통합대기환경지수</p>
					                    <c:choose>
					                        <c:when test='${aqc.caiGrade.equals("좋음")}'><div class="border border-primary border-5 rounded-pill"><p class="mb-0 mt-1 pt-2 text-primary fw-bolder">${aqc.caiGrade}</p><p class="fw-bolder">${aqc.cai}</p></div></c:when>
					                        <c:when test='${aqc.caiGrade.equals("보통")}'><div class="border border-success border-5 rounded-pill"><p class="mb-0 mt-1 pt-2 text-success fw-bolder">${aqc.caiGrade}</p><p class="fw-bolder">${aqc.cai}</p></div></c:when>
					                        <c:when test='${aqc.caiGrade.equals("나쁨")}'><div class="border border-warning border-5 rounded-pill"><p class="mb-0 mt-1 pt-2 text-warning fw-bolder">${aqc.caiGrade}</p><p class="fw-bolder">${aqc.cai}</p></div></c:when>
					                        <c:when test='${aqc.caiGrade.equals("매우나쁨")}'><div class="border border-danger border-5 rounded-pill"><p class="mb-0 mt-1 pt-2 text-danger fw-bolder">${aqc.caiGrade}</p><p class="fw-bolder">${aqc.cai}</p></div></c:when>
					                        <c:otherwise><div class="border border-dark border-5 rounded-pill"><p class="mb-0 mt-1 pt-2 text-dark fw-bolder">측정</p><p class="fw-bolder">불가</p></div></c:otherwise>
					                    </c:choose>
					                </div>
					                <div class="col-6 mt-4">
					                    <p class="fw-lighter">미세먼지</p>
					                    <c:choose>
					                        <c:when test="${0 <= aqc.pm10Conc && aqc.pm10Conc <= 30}"><div class="border border-primary border-5 rounded-pill"><p class="mb-0 pt-2 text-primary fw-bolder">좋음</p><p class="fw-bolder">${aqc.pm10Conc}</p></div></c:when>
					                        <c:when test="${30 < aqc.pm10Conc && aqc.pm10Conc <= 80}"><div class="border border-success border-5 rounded-pill"><p class="mb-0 pt-2 text-success fw-bolder">보통</p><p class="fw-bolder">${aqc.pm10Conc}</p></div></c:when>
					                        <c:when test="${80 < aqc.pm10Conc && aqc.pm10Conc <= 150}"><div class="border border-warning border-5 rounded-pill"><p class="mb-0 pt-2 text-warning fw-bolder">나쁨</p><p class="fw-bolder">${aqc.pm10Conc}</p></div></c:when>
					                        <c:when test="${aqc.pm10Conc > 150}"><div class="border border-danger border-5 rounded-pill"><p class="mb-0 pt-2 text-danger fw-bolder">매우나쁨</p><p class="fw-bolder">${aqc.pm10Conc}</p></div></c:when>
					                        <c:otherwise><div class="border border-dark border-5 rounded-pill"><p class="mb-0 pt-2 text-dark fw-bolder">측정</p><p class="fw-bolder">불가</p></div></c:otherwise>
					                    </c:choose>
					                </div>
					                <div class="col-6 mt-4">
					                    <p class="fw-lighter">초미세먼지</p>
					                    <c:choose>
					                        <c:when test="${0 <= aqc.pm25Conc && aqc.pm25Conc <= 15}"><div class="border border-primary border-5 rounded-pill"><p class="mb-0 pt-2 text-primary fw-bolder">좋음</p><p class="fw-bolder">${aqc.pm25Conc}</p></div></c:when>
					                        <c:when test="${15 < aqc.pm25Conc && aqc.pm25Conc <= 35}"><div class="border border-success border-5 rounded-pill"><p class="mb-0 pt-2 text-success fw-bolder">보통</p><p class="fw-bolder">${aqc.pm25Conc}</p></div></c:when>
					                        <c:when test="${35 < aqc.pm25Conc && aqc.pm25Conc <= 75}"><div class="border border-warning border-5 rounded-pill"><p class="mb-0 pt-2 text-warning fw-bolder">나쁨</p><p class="fw-bolder">${aqc.pm25Conc}</p></div></c:when>
					                        <c:when test="${aqc.pm25Conc > 75}"><div class="border border-danger border-5 rounded-pill"><p class="mb-0 pt-2 text-danger fw-bolder">매우나쁨</p><p class="fw-bolder">${aqc.pm25Conc}</p></div></c:when>
					                        <c:otherwise><div class="border border-dark border-5 rounded-pill"><p class="mb-0 pt-2 text-dark fw-bolder">측정</p><p class="fw-bolder">불가</p></div></c:otherwise>
					                    </c:choose>
					                </div>
					                <div class="col-6 mt-4">
					                    <p class="fw-lighter">오존</p>
					                    <c:choose>
					                        <c:when test="${0 <= aqc.o3Conc && aqc.o3Conc <= 0.030}"><div class="border border-primary border-5 rounded-pill"><p class="mb-0 pt-2 text-primary fw-bolder">좋음</p><p class="fw-bolder">${aqc.o3Conc}</p></div></c:when>
					                        <c:when test="${0.030 < aqc.o3Conc && aqc.o3Conc <= 0.090}"><div class="border border-success border-5 rounded-pill"><p class="mb-0 pt-2 text-success fw-bolder">보통</p><p class="fw-bolder">${aqc.o3Conc}</p></div></c:when>
					                        <c:when test="${0.090 < aqc.o3Conc && aqc.o3Conc <= 0.150}"><div class="border border-warning border-5 rounded-pill"><p class="mb-0 pt-2 text-warning fw-bolder">나쁨</p><p class="fw-bolder">${aqc.o3Conc}</p></div></c:when>
					                        <c:when test="${aqc.o3Conc > 0.150}"><div class="border border-danger border-5 rounded-pill"><p class="mb-0 pt-2 text-danger fw-bolder">매우나쁨</p><p class="fw-bolder">${aqc.o3Conc}</p></div></c:when>
					                        <c:otherwise><div class="border border-dark border-5 rounded-pill"><p class="mb-0 pt-2 text-dark fw-bolder">측정</p><p class="fw-bolder">불가</p></div></c:otherwise>
					                    </c:choose>
					                </div>
					            </div>
					        </div>
					    </div>
					</div>
					</div>

					<!-- 5일간의 날씨 예보 -->
					<div class="card mt-4 p-5 pt-0">
						<div class="row mt-3 justify-content-center">
						<h3 class="text-start forecast-title mt-4 ms-3">5일 예보</h3>
							<div class="row justify-content-center gap-4">
								<c:forEach var="wf" items="${wf_List}">
									<div class="col-md-2 forecast-card">
										<div class="card text-center p-3">
											<p class="mb-0">${wf.week}</p>
											<p class="mt-1 mb-1">${wf.fcstDt}</p>
											<img class="rounded mx-auto d-block"
												src="https://openweathermap.org/img/wn/${wf.weatherIconId}@2x.png"
												height="70" width="70">
											<div class="d-flex justify-content-center mt-0 mb-0 gap-1">
												<p class="fs-5 text-danger">${wf.maxTemp}°</p>
												<p class="fs-5">/</p>
												<p class="fs-5 text-primary">${wf.minTemp}°</p>
											</div>
										</div>
									</div>
								</c:forEach>
							</div>
						</div>
					</div>
					</div>
				</main>

			</main>
			<!--// main---------------------------------------------------->

		</div>
		
		
		<div class="container" style="max-width:1320px;">
		  <div class="card mt-5 mb-5 p-4 text-center shadow-sm">
		    <button class="btn btn-primary btn-weather-mbti" onclick="openQuizPopup()">
		      🌤️ 재난상황 대처로 알아보는 MBTI 하러가기 →
		    </button>
		  </div>
		  </div>


		<div class="container mt-4 mb-5">
			<div class="row">
				<div class="col-8">
					<h1 class="ps-0 pb-0 fs-4 fw-bolder"
						onclick="location.href='${CP}/disastermessage/view'"
						style="cursor: pointer; padding-bottom: 8px; padding-top: 8px;">긴급재난문자
						&#8594;</h1>
					<ul class="list-group">
						<c:forEach var="dm" items="${dmListForseoul}">
							<li class="list-group-item ps-0"
								style="border-top: none; border-left: none; border-right: none;">
								<div class="d-flex flex-column" style="font-size: 14px">
									<p class="text-danger mt-1 mb-1">${dm.disMesDt}</p>
									<p class="mt-1 mb-1">${dm.disMesCon}</p>
								</div>
							</li>
						</c:forEach>
					</ul>
				</div>

				<!-- 재난대비영상 -->
				<div class="col-4">
					<div class="card shadow-none"
						style="width: 23rem; background-color: transparent; border: none;">
						<h1 class="card-header ps-0 fs-4 fw-bolder"
							onclick="location.href='${CP}/videopost/list'"
							style="cursor: pointer; background-color: transparent; border: none;">재난훈련
							영상 &#8594;</h1>
						<!-- 유튜브 영상 ID 가져오기 -->
						<c:set var="videoId" value="${video.videoOriginalUrl}" />
						<!-- 📌 영상 플레이어 -->
						<div class="card-img-top mt-3">
							<iframe width="410px" height="250px"
								src="https://www.youtube.com/embed/${videoId}" frameborder="0"
								allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share"
								referrerpolicy="strict-origin-when-cross-origin" allowfullscreen>
							</iframe>
						</div>
						<div class="card-body ps-0">
							<h5 class="card-title fs-6 fw-bolder">${video.videoTitle}</h5>
						</div>
					</div>
				</div>
			</div>

		</div>

		<div class="container pt-4">
			<div class="row">
				<div class="col-6">
					<h1 class="ps-0 pb-0 fs-4 fw-bolder"
						onclick="location.href='${CP}/board/doRetrieve.do?div=10'"
						style="cursor: pointer; padding-bottom: 8px; padding-top: 8px;">공지사항
						&#8594;</h1>
					<ul class="list-group pt-3">
						<c:forEach var="notice" items="${noticeList}">
							<li class="list-group-item ps-0"
								style="border-top: none; border-left: none; border-right: none;">
								<div class="d-flex">
									<p class="flex-grow-1 fs-5 fw-bolder mt-1 mb-1 truncate-text"
										style="cursor: pointer;"
										onclick="location.href='${CP}/board/doSelectOne.do?div=10&boardId=${notice.boardId}'">${notice.title}</p>
									<p style="font-size: 14px" class="pt-1 mt-1 mb-1">${notice.regDt}</p>
								</div>
							</li>
						</c:forEach>
					</ul>
				</div>

				<div class="col-6">
					<h1 class="ps-0 pb-0 fs-4 fw-bolder"
						onclick="location.href='${CP}/board/doRetrieve.do?div=30'"
						style="cursor: pointer; padding-bottom: 8px; padding-top: 8px;">통계자료실
						&#8594;</h1>
					<ul class="list-group pt-3">
						<c:forEach var="statsPost" items="${statsPostList}">
							<li class="list-group-item ps-0"
								style="border-top: none; border-left: none; border-right: none;">
								<div class="d-flex">
									<p class="flex-grow-1 fs-5 fw-bolder mt-1 mb-1 truncate-text"
										style="cursor: pointer;"
										onclick="location.href='${CP}/board/doSelectOne.do?div=30&boardId=${statsPost.boardId}'">${statsPost.title}</p>
									<p style="font-size: 14px" class="pt-1 mt-1 mb-1">${statsPost.regDt}</p>
								</div>
							</li>
						</c:forEach>
					</ul>
				</div>
			</div>
		</div>


		<div class="container mt-5">
			<div class="col-6">
				<h1 class="ps-0 pb-0 fs-4 fw-bolder"
					onclick="location.href='${CP}/disasterguide/finedust'"
					style="cursor: pointer; padding-bottom: 8px; padding-top: 8px;">재난행동요령
					&#8594;</h1>
			</div>
			<div class="col-6"></div>
		</div>


		<div class="container-fluid mt-4 background-image">
		  <div class="content-wrapper"> <!-- 컨텐츠 너비 제한 & 가운데 정렬 -->
		    <div class="row">
		      <div class="col-4 pb-4" style="margin-left: 30px;">
					<h1 class="fs-3 pt-5 fw-bolder ps-1">자연재난 행동요령 및 예방법</h1>
					<div class="hexagon-container pt-3">
						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/finedust'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/14656/14656067.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">미세먼지</p>
								</div>
							</div>
						</div>

						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/typhoon'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/5782/5782847.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">태풍</p>
								</div>
							</div>
						</div>

						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/heatwave'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/15658/15658550.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">폭염</p>
								</div>
							</div>
						</div>

						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/heavyrain'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/10293/10293810.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">호우</p>
								</div>
							</div>
						</div>

						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/flood'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/16507/16507179.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">홍수</p>
								</div>
							</div>
						</div>

						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/coldwave'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/6236/6236344.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">한파</p>
								</div>
							</div>
						</div>

						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/snowstorm'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/9755/9755287.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">대설</p>
								</div>
							</div>
						</div>
					</div>
				</div>


				<div class="col-4 pb-4" style="margin-left: 200px;">
			        <h1 class="fs-3 pt-5 fw-bolder ps-1">사회재난 행동 요령 및 예방법</h1>
			        <div class="hexagon-container pt-3">
						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/fire'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/3268/3268677.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">화재</p>
								</div>
							</div>
						</div>
						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/forestfires'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src=https://cdn-icons-png.freepik.com/256/11726/11726222.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid
										width="40" height="40">
									<p class="mt-2 mb-0">산불</p>
								</div>
							</div>
						</div>
						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/electricity_gas_accident'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/10213/10213673.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">감전/가스</p>
								</div>
							</div>
						</div>
						<div class="hexagon__outer"
							onclick="location.href='${CP}/disasterguide/car_accident'"
							style="cursor: pointer;">
							<div class="hexagon__inner bg-primary">
								<div class="d-flex flex-column align-items-center">
									<img alt=""
										src="https://cdn-icons-png.freepik.com/256/4785/4785974.png?ga=GA1.1.1120617672.1741509995&semt=ais_hybrid"
										width="40" height="40">
									<p class="mt-2 mb-0">교통사고</p>
								</div>
							</div>
						</div>
					</div>
				</div>
				<div class="col-2"></div>

			</div>
		</div>


	</main>

	<!--// main---------------------------------------------------->

	</div>

	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>


	<script>
		// 오늘 날짜 자동 업데이트

		let day = new Date().toLocaleDateString('ko-KR');

		let dayOfWeek = new Date(day).getDay();
		let dayList = [ '일', '월', '화', '수', '목', '금', '토' ];
		let label = dayList[dayOfWeek];

		document.getElementById("date").innerText = day + "(" + label + ")";
	</script>
	
	<script>
	  function openMbtiPopup() {
	    const url = "${CP}/quiz/quiz"; // 원하는 팝업 URL
	    const name = "mbtiPopup"; // 팝업 이름
	    const specs = "width=600,height=800,top=100,left=100"; // 팝업창 크기와 위치

	    window.open(url, name, specs);
	  }
	  
	  document.querySelector(".btn-weather-mbti").addEventListener("click", function() {
	    openMbtiPopup();
	  });
	</script>

</body>
</html>