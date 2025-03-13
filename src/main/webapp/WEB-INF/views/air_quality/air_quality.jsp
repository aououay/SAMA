<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<c:set var="CP" value="${pageContext.request.contextPath}" />

<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="1200">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet">
<link href="/assets/css/header.css" rel="stylesheet">
<link href="/assets/css/air_quality.css" rel="stylesheet">
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<title>Insert title here</title>
<style>
.wrap {
	position: absolute;
	left: 0;
	bottom: 40px;
	width: 288px;
	height: 132px;
	margin-left: -144px;
	text-align: left;
	overflow: hidden;
	font-size: 12px;
	font-family: 'Malgun Gothic', dotum, '돋움', sans-serif;
	line-height: 1.5;
}

.wrap * {
	padding: 0;
	margin: 0;
}

.wrap .info {
	width: 286px;
	height: 120px;
	border-radius: 5px;
	border-bottom: 2px solid #ccc;
	border-right: 1px solid #ccc;
	overflow: hidden;
	background: #fff;
}

.wrap .info:nth-child(1) {
	border: 0;
	box-shadow: 0px 1px 2px #888;
}

.info .title {
	padding: 5px 0 0 5px;
	height: 30px;
	background: #f8f8ff;
	border-bottom: 1px solid #ddd;
	font-size: 15px;
	font-weight: bold;
	text-indent: 6px
}

.info .body {
	position: relative;
	overflow: hidden;
}

.info .desc {
	position: relative;
	margin: 5px 0 0 5px;
	height: 85px;
}

.desc .ellipsis {
	margin-top: 2px;
	margin-left: 6px;
	overflow: hidden;
	text-overflow: ellipsis;
	white-space: nowrap;
}

.desc .jibun {
	font-size: 11px;
	color: #888;
	margin-top: 2px;
}

.info:after {
	content: '';
	position: absolute;
	margin-left: -12px;
	left: 50%;
	bottom: 0;
	width: 22px;
	height: 12px;
	background:
		url('https://t1.daumcdn.net/localimg/localimages/07/mapapidoc/vertex_white.png')
}

.map-b {
	font-size: 12px
}

.nav-tab {
	margin-bottom: 0px;
}

.pm_w {
	width: 1150px;
}

#pm_prepare {
    position: relative;
    width: 100%;
    overflow: hidden;
}

#pm_prepare::before, 
#pm_prepare::after {
    content: "";
    position: absolute;
    top: 0;
    height: 100%;
    width: 50%;
    z-index: -1;
}

#pm_prepare::before {
    left: 0;
    background-color: #F1F1F1; /* 왼쪽 배경색 (네이비톤) */
}

#pm_prepare::after {
    right: 0;
    background-color: #E3E1DE; /* 오른쪽 배경색 (밝은 네이비톤 또는 원하는 컬러) */
}

#pm_prepare {
    display: flex;
    justify-content: center;
    align-items: center;
    position: relative;
    overflow: hidden;
}

#pm_prepare img {
    max-width: 1320px;
    width: 100%;
    height: auto;
    z-index: 1;
}

</style>

<script src="/assets/js/jquery_3_7_1.js"></script>
</head>
<link rel="icon" href="data:;base64,iVBORw0KGgo=">
		
<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>



	<!-- main-->
	<main>
		<div class="container">
			<div class="row pt-5">
				<div class="col-6">
					<h1 class="fs-4 mb-4 fw-bolder">서울특별시 실시간 대기환경 지도</h1>
					<div class="nav-tab">
						<ul class="nav nav-tabs nav-fill justify-content-center">
							<li class="nav-item" style="cursor: pointer;"><a
								class="nav-link text-dark active"
								onclick="cai_MakerAndOverlays()">통합대기환경</a></li>
							<li class="nav-item" style="cursor: pointer;"><a
								class="nav-link text-dark" onclick="pm10_MakerAndOverlays()">미세먼지</a></li>
							<li class="nav-item" style="cursor: pointer;"><a
								class="nav-link text-dark" onclick="pm25_MakerAndOverlays()">초미세먼지</a></li>
							<li class="nav-item" style="cursor: pointer;"><a
								class="nav-link text-dark" onclick="o3_MakerAndOverlays()">오존</a></li>
						</ul>
					</div>
					<div id="map" class="border-end border-start"
						style="width: 636px; height: 600px;"></div>
					<div
						class="border-end border-start border-bottom ps-3 bg-light text-start">
						<p class="map-b pt-2 mb-1">&#128309;좋음 &#128994;보통 &#128992;나쁨
							&#128308;매우나쁨</p>
						<p class="map-b mb-1">통합대기환경/미세먼지/초미세먼지/오존 : 에어코리아 제공</p>
						<p class="map-b pb-2 mb-2">데이터는 현지 사정 및 수신상태에 따라 미수신 될 수 있습니다.</p>
					</div>
				</div>
				<div class="col-6 ps-4 pe-5">
					<h1 class="fs-4 mb-4 text-end fw-bolder">서울특별시 자치구별 실시간 대기환경</h1>
					<ul class="nav nav-tabs nav-fill pt-1 justify-content-center"
						id="myTab" role="tablist">
						<li class="nav-item" role="presentation">
							<button class="nav-link active" id="cai-tab" data-bs-toggle="tab"
								data-bs-target="#cai" type="button" role="tab"
								aria-controls="cai" aria-selected="true">통합대기환경</button>
						</li>
						<li class="nav-item" role="presentation">
							<button class="nav-link" id="pm10-tab" data-bs-toggle="tab"
								data-bs-target="#pm10" type="button" role="tab"
								aria-controls="pm10" aria-selected="false">미세먼지</button>
						</li>
						<li class="nav-item" role="presentation">
							<button class="nav-link" id="pm25-tab" data-bs-toggle="tab"
								data-bs-target="#pm25" type="button" role="tab"
								aria-controls="pm25" aria-selected="false">초미세먼지</button>
						</li>
						<li class="nav-item" role="presentation">
							<button class="nav-link" id="o3-tab" data-bs-toggle="tab"
								data-bs-target="#o3" type="button" role="tab" aria-controls="o3"
								aria-selected="false">오존</button>
						</li>
					</ul>
					<div class="tab-content border-end border-start border-bottom"
						id="myTabContent">
						<div class="tab-pane fade show active" id="cai" role="tabpanel"
							aria-labelledby="cai-tab">
							<p style="font-size: 14px; color: #888;"
								class="text-end pt-3 me-5">발표일 : ${aq_List[0].msDt}</p>
							<div class="row">
								<div class="col-6 mt-0 ps-5 pe-2">
									<table class="table table-bordered fw-normal text-center mt-0">
										<thead>
											<c:forEach var="aq" items="${aq_List}" begin="0" end="12">
												<tr>
													<th class="bg-light">${aq.guNm}</th>
													<c:choose>
														<c:when test='${aq.caiGrade.equals("좋음")}'>
															<th class="text-primary fw-normal w-50">${aq.cai}</th>
														</c:when>
														<c:when test='${aq.caiGrade.equals("보통")}'>
															<th class="text-success fw-normal w-50">${aq.cai}</th>
														</c:when>
														<c:when test='${aq.caiGrade.equals("나쁨")}'>
															<th class="text-warning fw-normal w-50">${aq.cai}</th>
														</c:when>
														<c:when test='${aq.caiGrade.equals("매우나쁨")}'>
															<th class="text-danger fw-normal w-50">${aq.cai}</th>
														</c:when>
														<c:otherwise>
															<th class="text-dark fw-normal w-50">점검중</th>
														</c:otherwise>
													</c:choose>
												</tr>
											</c:forEach>
										</thead>
									</table>
								</div>
								<div class="col-6 ps-2 pe-5">
									<table class="table table-bordered text-center mt-0">
										<thead>
											<c:forEach var="aq" items="${aq_List}" begin="13">
												<tr>
													<th class="bg-light">${aq.guNm}</th>
													<c:choose>
														<c:when test='${aq.caiGrade.equals("좋음")}'>
															<th class="text-primary fw-normal w-50">${aq.cai}</th>
														</c:when>
														<c:when test='${aq.caiGrade.equals("보통")}'>
															<th class="text-success fw-normal w-50">${aq.cai}</th>
														</c:when>
														<c:when test='${aq.caiGrade.equals("나쁨")}'>
															<th class="text-warning fw-normal w-50">${aq.cai}</th>
														</c:when>
														<c:when test='${aq.caiGrade.equals("매우나쁨")}'>
															<th class="text-danger fw-normal w-50">${aq.cai}</th>
														</c:when>
														<c:otherwise>
															<th class="text-dark fw-normal w-50">점검중</th>
														</c:otherwise>
													</c:choose>
												</tr>
											</c:forEach>
										</thead>
									</table>
								</div>
							</div>

						</div>
						<div class="tab-pane fade" id="pm10" role="tabpanel"
							aria-labelledby="pm10-tab">
							<p style="font-size: 14px; color: #888;"
								class="text-end pt-3 me-5">발표일 : ${aq_List[0].msDt}</p>
							<div class="row">
								<div class="col-6 mt-0 ps-5 pe-2">
									<table class="table table-bordered fw-normal text-center mt-0">
										<thead>
											<c:forEach var="aq" items="${aq_List}" begin="0" end="12">
												<tr>
													<th class="bg-light">${aq.guNm}</th>
													<c:choose>
														<c:when test='${0 <= aq.pm10Conc && aq.pm10Conc <= 30}'>
															<th class="text-primary fw-normal w-50">${aq.pm10Conc}</th>
														</c:when>
														<c:when test='${30 < aq.pm10Conc && aq.pm10Conc <= 80}'>
															<th class="text-success fw-normal w-50">${aq.pm10Conc}</th>
														</c:when>
														<c:when test='${80 < aq.pm10Conc && aq.pm10Conc <= 150}'>
															<th class="text-warning fw-normal w-50">${aq.pm10Conc}</th>
														</c:when>
														<c:when test='${aq.pm10Conc > 150}'>
															<th class="text-danger fw-normal w-50">${aq.pm10Conc}</th>
														</c:when>
														<c:otherwise>
															<th class="text-dark fw-normal w-50">점검중</th>
														</c:otherwise>
													</c:choose>
												</tr>
											</c:forEach>
										</thead>
									</table>
								</div>
								<div class="col-6 ps-2 pe-5">
									<table class="table table-bordered text-center mt-0">
										<thead>
											<c:forEach var="aq" items="${aq_List}" begin="13">
												<tr>
													<th class="bg-light">${aq.guNm}</th>
													<c:choose>
														<c:when test='${0 <= aq.pm10Conc && aq.pm10Conc <= 30}'>
															<th class="text-primary fw-normal w-50">${aq.pm10Conc}</th>
														</c:when>
														<c:when test='${30 < aq.pm10Conc && aq.pm10Conc <= 80}'>
															<th class="text-success fw-normal w-50">${aq.pm10Conc}</th>
														</c:when>
														<c:when test='${80 < aq.pm10Conc && aq.pm10Conc <= 150}'>
															<th class="text-warning fw-normal w-50">${aq.pm10Conc}</th>
														</c:when>
														<c:when test='${aq.pm10Conc > 150}'>
															<th class="text-danger fw-normal w-50">${aq.pm10Conc}</th>
														</c:when>
														<c:otherwise>
															<th class="text-dark fw-normal w-50">점검중</th>
														</c:otherwise>
													</c:choose>
												</tr>
											</c:forEach>
										</thead>
									</table>
								</div>
							</div>
						</div>
						<div class="tab-pane fade" id="pm25" role="tabpanel"
							aria-labelledby="pm25-tab">
							<p style="font-size: 14px; color: #888;"
								class="text-end pt-3 me-5">발표일 : ${aq_List[0].msDt}</p>
							<div class="row">
								<div class="col-6 mt-0 ps-5 pe-2">
									<table class="table table-bordered fw-normal text-center mt-0">
										<thead>
											<c:forEach var="aq" items="${aq_List}" begin="0" end="12">
												<tr>
													<th class="bg-light">${aq.guNm}</th>
													<c:choose>
														<c:when test="${0 <= aq.pm25Conc && aq.pm25Conc <= 15}">
															<th class="text-primary fw-normal w-50">${aq.pm25Conc}</th>
														</c:when>
														<c:when test="${15 < aq.pm25Conc && aq.pm25Conc <= 35}">
															<th class="text-success fw-normal w-50">${aq.pm25Conc}</th>
														</c:when>
														<c:when test="${35 < aq.pm25Conc && aq.pm25Conc <= 75}">
															<th class="text-warning fw-normal w-50">${aq.pm25Conc}</th>
														</c:when>
														<c:when test="${aq.pm25Conc > 75}">
															<th class="text-danger fw-normal w-50">${aq.pm25Conc}</th>
														</c:when>
														<c:otherwise>
															<th class="text-dark fw-normal w-50">점검중</th>
														</c:otherwise>
													</c:choose>
												</tr>
											</c:forEach>
										</thead>
									</table>
								</div>
								<div class="col-6 ps-2 pe-5">
									<table class="table table-bordered text-center mt-0">
										<thead>
											<c:forEach var="aq" items="${aq_List}" begin="13">
												<tr>
													<th class="bg-light">${aq.guNm}</th>
													<c:choose>
														<c:when test="${0 <= aq.pm25Conc && aq.pm25Conc <= 15}">
															<th class="text-primary fw-normal w-50">${aq.pm25Conc}</th>
														</c:when>
														<c:when test="${15 < aq.pm25Conc && aq.pm25Conc <= 35}">
															<th class="text-success fw-normal w-50">${aq.pm25Conc}</th>
														</c:when>
														<c:when test="${35 < aq.pm25Conc && aq.pm25Conc <= 75}">
															<th class="text-warning fw-normal w-50">${aq.pm25Conc}</th>
														</c:when>
														<c:when test="${aq.pm25Conc > 75}">
															<th class="text-danger fw-normal w-50">${aq.pm25Conc}</th>
														</c:when>
														<c:otherwise>
															<th class="text-dark fw-normal w-50">점검중</th>
														</c:otherwise>
													</c:choose>
												</tr>
											</c:forEach>
										</thead>
									</table>
								</div>
							</div>
						</div>
						<div class="tab-pane fade" id="o3" role="tabpanel"
							aria-labelledby="o3-tab">
							<p style="font-size: 14px; color: #888;"
								class="text-end pt-3 me-5">발표일 : ${aq_List[0].msDt}</p>
							<div class="row">
								<div class="col-6 mt-0 ps-5 pe-2">
									<table class="table table-bordered fw-normal text-center mt-0">
										<thead>
											<c:forEach var="aq" items="${aq_List}" begin="0" end="12">
												<tr>
													<th class="bg-light">${aq.guNm}</th>
													<c:choose>
														<c:when test='${0 <= aq.o3Conc && aq.o3Conc <= 0.030}'>
															<th class="text-primary fw-normal w-50">${aq.o3Conc}</th>
														</c:when>
														<c:when
															test='${0.030 < aq.o3Conc && aq.o3Conc <= 0.090}'>
															<th class="text-success fw-normal w-50">${aq.o3Conc}</th>
														</c:when>
														<c:when
															test='${0.090 < aq.o3Conc && aq.o3Conc <= 0.150}'>
															<th class="text-warning fw-normal w-50">${aq.o3Conc}</th>
														</c:when>
														<c:when test='${aq.o3Conc > 0.150}'>
															<th class="text-danger fw-normal w-50">${aq.o3Conc}</th>
														</c:when>
														<c:otherwise>
															<th class="text-dark fw-normal w-50">점검중</th>
														</c:otherwise>
													</c:choose>
												</tr>
											</c:forEach>
										</thead>
									</table>
								</div>
								<div class="col-6 ps-2 pe-5">
									<table class="table table-bordered text-center mt-0">
										<thead>
											<c:forEach var="aq" items="${aq_List}" begin="13">
												<tr>
													<th class="bg-light">${aq.guNm}</th>
													<c:choose>
														<c:when test='${0 <= aq.o3Conc && aq.o3Conc <= 0.030}'>
															<th class="text-primary fw-normal w-50">${aq.o3Conc}</th>
														</c:when>
														<c:when
															test='${0.030 < aq.o3Conc && aq.o3Conc <= 0.090}'>
															<th class="text-success fw-normal w-50">${aq.o3Conc}</th>
														</c:when>
														<c:when
															test='${0.090 < aq.o3Conc && aq.o3Conc <= 0.150}'>
															<th class="text-warning fw-normal w-50">${aq.o3Conc}</th>
														</c:when>
														<c:when test='${aq.o3Conc > 0.150}'>
															<th class="text-danger fw-normal w-50">${aq.o3Conc}</th>
														</c:when>
														<c:otherwise>
															<th class="text-dark fw-normal w-50">점검중</th>
														</c:otherwise>
													</c:choose>
												</tr>
											</c:forEach>
										</thead>
									</table>
								</div>
							</div>
						</div>
					</div>
				</div>
			</div>
		</div>

		<div class="">
			<h1 class="fs-4 fw-bolder mt-4 mb-4 text-center">서울 미세먼지 경보 발령 내역</h1>
		</div>
		<div class="container-fluid d-flex justify-content-center">
			<table class="pm_w table table-bordered border-dark text-center">
				<thead>
					<tr class="text-center table-success">
						<th class="pb-2" scope="col">번호</th>
						<th class="pb-2" scope="col">지역</th>
						<th class="pb-2" scope="col">권역</th>
						<th class="pb-2" scope="col">항목</th>
						<th class="pb-2" scope="col">경보단계</th>
						<th class="pb-2" scope="col">발령농도</th>
						<th class="pb-2" scope="col">발령시간</th>
						<th class="pb-2" scope="col">해제시간</th>
					</tr>
				</thead>
				<tbody>
					<c:set var="count" value="0"></c:set>
					<c:forEach var="pm_Warning" items="${pm_Warning_List}">
						<tr>
							<th class="table-success" scope="row">${count+1}</th>
							<td>${pm_Warning.distNm}</td>
							<td>${pm_Warning.moveNm}</td>
							<c:choose>
								<c:when test='${pm_Warning.issueItem.equals("PM25")}'>
									<td>PM2.5</td>
								</c:when>
								<c:otherwise>
									<td>${pm_Warning.issueItem}</td>
								</c:otherwise>
							</c:choose>
							<td>${pm_Warning.issueGbn}</td>
							<td>${pm_Warning.issueConc}</td>
							<td>${pm_Warning.issueDt}</td>
							<td>${pm_Warning.clearDt}</td>
						</tr>
						<c:set var="count" value="${count + 1}" />
					</c:forEach>

				</tbody>
			</table>
		</div>
		
		<div id="pm_prepare" class="container-fluid mt-5 px-0">
		<img alt="" src="/assets/images/young_min/pm_prepare.png" width="100%" height="453px" class="">
		</div>


	</main>


	<!--// main---------------------------------------------------->

	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>

	<script type="text/javascript"
		src="//dapi.kakao.com/v2/maps/sdk.js?appkey=69ec97985c73a732909bbbdfc73cdfd3"></script>

	<script>
		var container = document.getElementById('map');
		var options = {
			center : new kakao.maps.LatLng(37.5642135, 127.0016985),
			level : 9
		};

		var map = new kakao.maps.Map(container, options);
		
		let markers = [];
		let overlays = [];

		// 지도 확대 축소를 제어할 수 있는  줌 컨트롤을 생성합니다
		var zoomControl = new kakao.maps.ZoomControl();
		map.addControl(zoomControl, kakao.maps.ControlPosition.RIGHT);
		
		function panTo() {
		    // 이동할 위도 경도 위치를 생성합니다 
		    var moveLatLon = new kakao.maps.LatLng(37.5642135, 127.0016985);
		    
		    // 지도 중심을 부드럽게 이동시킵니다
		    // 만약 이동할 거리가 지도 화면보다 크면 부드러운 효과 없이 이동합니다
		    map.panTo(moveLatLon);            
		}       
		 
		function clean_MarkersAndOverlays() {
			
			markers.forEach(marker => marker.setMap(null));
				
			overlays.forEach(overlay => overlay.setMap(null));
			
			markers = [];
			overlays = [];
			
		}

		function make_MarkersAndOverlays(positions) {
			
			clean_MarkersAndOverlays();

			for (var i = 0; i < positions.length; i++) {
				(function(position){
				
				// 마커 이미지의 이미지 크기 입니다
			    var imageSize = new kakao.maps.Size(20, 24); 
				
			    // 마커 이미지를 생성합니다    
			    var markerImage = new kakao.maps.MarkerImage(positions[i].imageSrc, imageSize); 
	
				// 마커를 생성합니다
				var marker = new kakao.maps.Marker({
					map: map, // 마커를 표시할 지도
					position : positions[i].latlng, // 마커를 표시할 위치
					image : markerImage // 마커 이미지 
				});
				
				markers.push(marker);
				
				// 마커 위에 커스텀오버레이를 표시합니다
				// 마커를 중심으로 커스텀 오버레이를 표시하기위해 CSS를 이용해 위치를 설정했습니다
				var overlay = new kakao.maps.CustomOverlay({
				    content: positions[i].content,
				    map: map,
				    position: marker.getPosition()
				});
				
				overlays.push(overlay);
				overlay.setMap(null);
				
				// 마커를 마우스 오버했을 때 커스텀 오버레이를 표시합니다
				kakao.maps.event.addListener(marker, 'mouseover', function() {
				    overlay.setMap(map);
				});
				
				// 커스텀 오버레이를 닫기 위해 호출되는 함수입니다 
				function closeOverlay() {
				    overlay.setMap(null);     
				}
				
				// 마커를 마우스 아웃했을 때 커스텀 오버레이를 제거합니다
				kakao.maps.event.addListener(marker, 'mouseout', function() {
					closeOverlay();
				});
				
				})(positions[i]);
			}
		}
		
		function cai_MakerAndOverlays(){
			
			panTo();
			
		    const tabs = document.querySelectorAll(".nav-link");

	        tabs.forEach(tab => {
	            tab.addEventListener("click", function() {
	                // 모든 탭의 active 클래스 제거
	                tabs.forEach(t => t.classList.remove("active"));
	                
	                // 클릭한 탭에 active 클래스 추가
	                this.classList.add("active");
	            });
	        });
			
			let positions = [
				<c:forEach var="aq" items="${aq_List}" varStatus="status">
				{
			        imageSrc:
			       	<c:choose>
				    	<c:when test='${aq.caiGrade.equals("좋음")}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level1.png"
				    	</c:when>
				    	<c:when test='${aq.caiGrade.equals("보통")}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level2.png"
				    	</c:when>
				    	<c:when test='${aq.caiGrade.equals("나쁨")}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level3.png"
				    	</c:when>
				    	<c:when test='${aq.caiGrade.equals("매우나쁨")}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level4.png"
				    	</c:when>
				    	<c:otherwise>
				    		"/assets/images/young_min/ico_air_level0.png"
				    	</c:otherwise>
			    	</c:choose>, 
			        latlng: new kakao.maps.LatLng(${aq.msLat}, ${aq.msLon}),
					content: '<div class="wrap">' + 
		            '    <div class="info">' + 
		            '        <div class="title">' + 
		            '             ${aq.guNm}' + 
		            '        </div>' + 
		            '        <div class="body">' + 
		            '            <div class="img">' +
		            '           </div>' + 
		            '            <div class="desc">' + 
		            '                <div class="ellipsis">위치</div>' + 
		            '                <div class="jibun ellipsis">${aq.msAddr}</div>' +
		            '        	   <div class="table-wrap mt-1 px-1">' + 
		            '				  <table class="table table-bordered text-center">'+
		            '           	 	<thead>'+
		            '               	 	<tr>'+
		            '                   		<th class="w-25 fw-bolder table-secondary border">통합대기지수</th>'+
									            <c:choose>
										        	<c:when test="${aq.caiGrade.equals(\"좋음\")}">
										        		'<th class="w-25 fw-normal text-primary">좋음</th>' +
										        	</c:when>
										        	<c:when test="${aq.caiGrade.equals(\"보통\")}">
										        		'<th class="w-25 fw-normal text-success">보통</th>' +
										        	</c:when>
										        	<c:when test="${aq.caiGrade.equals(\"나쁨\")}">
										        		'<th class="w-25 fw-normal text-warning">나쁨</th>' +
										        	</c:when>
										        	<c:when test="${aq.caiGrade.equals(\"매우나쁨\")}">
										        		'<th class="w-25 fw-normal text-danger">매우나쁨</th>' +
										        	</c:when>
										        	<c:otherwise>
										        		'<th class="w-25 fw-normal text-dark">-</th>' +
										        	</c:otherwise>
									        	</c:choose>
										        	<c:choose>
										        	<c:when test="${aq.cai != -1}">
										        		'<th class="w-25 fw-normal">${aq.cai}</th>' +
										        	</c:when>
										        	<c:otherwise>
										        		'<th class="w-25 fw-normal">점검중</th>' +
										        	</c:otherwise>
									        	</c:choose>
		            '                		</tr>'+
		            '                	</thead>'+
		            '           	  </table>'+
		            '              </div>' + 
		            '            </div>' + 
		            '        </div>' + 
		            '    </div>' +    
		            '</div>',				
		            lat: ${aq.msLat},
		            lon: ${aq.msLon}
			        
			    }<c:if test="${!status.last}">,</c:if>
				</c:forEach>
			];	
			
			make_MarkersAndOverlays(positions);
		}
		
		function pm10_MakerAndOverlays(){
		
			panTo();
			
		    const tabs = document.querySelectorAll(".nav-link");

	        tabs.forEach(tab => {
	            tab.addEventListener("click", function() {
	                // 모든 탭의 active 클래스 제거
	                tabs.forEach(t => t.classList.remove("active"));
	                
	                // 클릭한 탭에 active 클래스 추가
	                this.classList.add("active");
	            });
	        });
			
			let positions = [
				<c:forEach var="aq" items="${aq_List}" varStatus="status">
				{
			        imageSrc:
			       	<c:choose>
				    	<c:when test='${0 <= aq.pm10Conc && aq.pm10Conc <= 30}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level1.png"
				    	</c:when>
				    	<c:when test='${30 < aq.pm10Conc && aq.pm10Conc <= 80}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level2.png"
				    	</c:when>
				    	<c:when test='${80 < aq.pm10Conc && aq.pm10Conc <= 150}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level3.png"
				    	</c:when>
				    	<c:when test='${aq.pm10Conc > 150}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level4.png"
				    	</c:when>
				    	<c:otherwise>
				    		"/assets/images/young_min/ico_air_level0.png"
				    	</c:otherwise>
			    	</c:choose>, 
			        latlng: new kakao.maps.LatLng(${aq.msLat}, ${aq.msLon}),
					content: '<div class="wrap">' + 
		            '    <div class="info">' + 
		            '        <div class="title">' + 
		            '             ${aq.guNm}' + 
		            '        </div>' + 
		            '        <div class="body">' + 
		            '            <div class="img">' +
		            '           </div>' + 
		            '            <div class="desc">' + 
		            '                <div class="ellipsis">위치</div>' + 
		            '                <div class="jibun ellipsis">${aq.msAddr}</div>' +
		            '        	   <div class="table-wrap mt-1 px-1">' + 
		            '				  <table class="table table-bordered text-center">'+
		            '           	 	<thead>'+
		            '               	 	<tr>'+
		            '                   		<th class="w-25 fw-bolder table-secondary border">미세먼지</th>'+
									            <c:choose>
										        	<c:when test="${0 <= aq.pm10Conc && aq.pm10Conc <= 30}">
										        		'<th class="w-25 fw-normal text-primary">좋음</th>' +
										        	</c:when>
										        	<c:when test="${30 < aq.pm10Conc && aq.pm10Conc <= 80}">
										        		'<th class="w-25 fw-normal text-success">보통</th>' +
										        	</c:when>
										        	<c:when test="${80 < aq.pm10Conc && aq.pm10Conc <= 150}">
										        		'<th class="w-25 fw-normal text-warning">나쁨</th>' +
										        	</c:when>
										        	<c:when test="${aq.pm10Conc > 150}">
										        		'<th class="w-25 fw-normal text-danger">매우나쁨</th>' +
										        	</c:when>
										        	<c:otherwise>
										        		'<th class="w-25 fw-normal text-dark">-</th>' +
										        	</c:otherwise>
									        	</c:choose>
										        	<c:choose>
										        	<c:when test="${aq.pm10Conc != -1}">
										        		'<th class="w-25 fw-normal">${aq.pm10Conc}㎍/m³</th>' +
										        	</c:when>
										        	<c:otherwise>
										        		'<th class="w-25 fw-normal">점검중</th>' +
										        	</c:otherwise>
									        	</c:choose>
		            '                		</tr>'+
		            '                	</thead>'+
		            '           	  </table>'+
		            '              </div>' + 
		            '            </div>' + 
		            '        </div>' + 
		            '    </div>' +    
		            '</div>',				
		            lat: ${aq.msLat},
		            lon: ${aq.msLon}
			        
			    }<c:if test="${!status.last}">,</c:if>
				</c:forEach>
			];	
			
			make_MarkersAndOverlays(positions);
		}
		
		function pm25_MakerAndOverlays(){
			
			panTo();
			
		    const tabs = document.querySelectorAll(".nav-link");

	        tabs.forEach(tab => {
	            tab.addEventListener("click", function() {
	                // 모든 탭의 active 클래스 제거
	                tabs.forEach(t => t.classList.remove("active"));
	                
	                // 클릭한 탭에 active 클래스 추가
	                this.classList.add("active");
	            });
	        });
			
			let positions = [
				<c:forEach var="aq" items="${aq_List}" varStatus="status">
				{
			        imageSrc:
			       	<c:choose>
				    	<c:when test='${0 <= aq.pm25Conc && aq.pm25Conc <= 15}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level1.png"
				    	</c:when>
				    	<c:when test='${15 < aq.pm25Conc && aq.pm25Conc <= 35}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level2.png"
				    	</c:when>
				    	<c:when test='${35 < aq.pm25Conc && aq.pm25Conc <= 75}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level3.png"
				    	</c:when>
				    	<c:when test='${aq.pm25Conc > 75}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level4.png"
				    	</c:when>
				    	<c:otherwise>
				    		"/assets/images/young_min/ico_air_level0.png"
				    	</c:otherwise>
			    	</c:choose>, 
			        latlng: new kakao.maps.LatLng(${aq.msLat}, ${aq.msLon}),
					content: '<div class="wrap">' + 
		            '    <div class="info">' + 
		            '        <div class="title">' + 
		            '             ${aq.guNm}' + 
		            '        </div>' + 
		            '        <div class="body">' + 
		            '            <div class="img">' +
		            '           </div>' + 
		            '            <div class="desc">' + 
		            '                <div class="ellipsis">위치</div>' + 
		            '                <div class="jibun ellipsis">${aq.msAddr}</div>' +
		            '        	   <div class="table-wrap mt-1 px-1">' + 
		            '				  <table class="table table-bordered text-center">'+
		            '           	 	<thead>'+
		            '               	 	<tr>'+
		            '                   		<th class="w-25 fw-bolder table-secondary border">초미세먼지</th>'+
									            <c:choose>
										        	<c:when test="${0 <= aq.pm25Conc && aq.pm25Conc <= 15}">
										        		'<th class="w-25 fw-normal text-primary">좋음</th>' +
										        	</c:when>
										        	<c:when test="${15 < aq.pm25Conc && aq.pm25Conc <= 35}">
										        		'<th class="w-25 fw-normal text-success">보통</th>' +
										        	</c:when>
										        	<c:when test="${35 < aq.pm25Conc && aq.pm25Conc <= 75}">
										        		'<th class="w-25 fw-normal text-warning">나쁨</th>' +
										        	</c:when>
										        	<c:when test="${aq.pm25Conc > 75}">
										        		'<th class="w-25 fw-normal text-danger">매우나쁨</th>' +
										        	</c:when>
										        	<c:otherwise>
										        		'<th class="w-25 fw-normal text-dark">-</th>' +
										        	</c:otherwise>
									        	</c:choose>
										        	<c:choose>
										        	<c:when test="${aq.pm25Conc != -1}">
										        		'<th class="w-25 fw-normal">${aq.pm25Conc}㎍/m³</th>' +
										        	</c:when>
										        	<c:otherwise>
										        		'<th class="w-25 fw-normal">점검중</th>' +
										        	</c:otherwise>
									        	</c:choose>
		            '                		</tr>'+
		            '                	</thead>'+
		            '           	  </table>'+
		            '              </div>' + 
		            '            </div>' + 
		            '        </div>' + 
		            '    </div>' +    
		            '</div>',				
		            lat: ${aq.msLat},
		            lon: ${aq.msLon}
			        
			    }<c:if test="${!status.last}">,</c:if>
				</c:forEach>
			];	
			
			make_MarkersAndOverlays(positions);
		}
		
		function o3_MakerAndOverlays(){
			
			panTo();
			
		    const tabs = document.querySelectorAll(".nav-link");

	        tabs.forEach(tab => {
	            tab.addEventListener("click", function() {
	                // 모든 탭의 active 클래스 제거
	                tabs.forEach(t => t.classList.remove("active"));
	                
	                // 클릭한 탭에 active 클래스 추가
	                this.classList.add("active");
	            });
	        });
			
			let positions = [
				<c:forEach var="aq" items="${aq_List}" varStatus="status">
				{
			        imageSrc:
			       	<c:choose>
				    	<c:when test='${0 <= aq.o3Conc && aq.o3Conc <= 0.030}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level1.png"
				    	</c:when>
				    	<c:when test='${0.030 < aq.o3Conc && aq.o3Conc <= 0.090}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level2.png"
				    	</c:when>
				    	<c:when test='${0.090 < aq.o3Conc && aq.o3Conc <= 0.150}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level3.png"
				    	</c:when>
				    	<c:when test='${aq.o3Conc > 0.150}'>
				    		"https://d.kbs.co.kr/static/images/now/ico_air_level4.png"
				    	</c:when>
				    	<c:otherwise>
				    		"/assets/images/young_min/ico_air_level0.png"
				    	</c:otherwise>
			    	</c:choose>, 
			        latlng: new kakao.maps.LatLng(${aq.msLat}, ${aq.msLon}),
					content: '<div class="wrap">' + 
		            '    <div class="info">' + 
		            '        <div class="title">' + 
		            '             ${aq.guNm}' + 
		            '        </div>' + 
		            '        <div class="body">' + 
		            '            <div class="img">' +
		            '           </div>' + 
		            '            <div class="desc">' + 
		            '                <div class="ellipsis">위치</div>' + 
		            '                <div class="jibun ellipsis">${aq.msAddr}</div>' +
		            '        	   <div class="table-wrap mt-1 px-1">' + 
		            '				  <table class="table table-bordered text-center">'+
		            '           	 	<thead>'+
		            '               	 	<tr>'+
		            '                   		<th class="w-25 fw-bolder table-secondary border">오존</th>'+
									            <c:choose>
										        	<c:when test="${0 <= aq.o3Conc && aq.o3Conc <= 0.030}">
										        		'<th class="w-25 fw-normal text-primary">좋음</th>' +
										        	</c:when>
										        	<c:when test="${0.030 < aq.o3Conc && aq.o3Conc <= 0.090}">
										        		'<th class="w-25 fw-normal text-success">보통</th>' +
										        	</c:when>
										        	<c:when test="${0.090 < aq.o3Conc && aq.o3Conc <= 0.150}">
										        		'<th class="w-25 fw-normal text-warning">나쁨</th>' +
										        	</c:when>
										        	<c:when test="${aq.o3Conc > 0.150}">
										        		'<th class="w-25 fw-normal text-danger">매우나쁨</th>' +
										        	</c:when>
										        	<c:otherwise>
										        		'<th class="w-25 fw-normal text-dark">-</th>' +
										        	</c:otherwise>
									        	</c:choose>
										        	<c:choose>
										        	<c:when test="${aq.o3Conc != -1}">
										        		'<th class="w-25 fw-normal">${aq.o3Conc}㎍/m³</th>' +
										        	</c:when>
										        	<c:otherwise>
										        		'<th class="w-25 fw-normal">점검중</th>' +
										        	</c:otherwise>
									        	</c:choose>
		            '                		</tr>'+
		            '                	</thead>'+
		            '           	  </table>'+
		            '              </div>' + 
		            '            </div>' + 
		            '        </div>' + 
		            '    </div>' +    
		            '</div>',				
		            lat: ${aq.msLat},
		            lon: ${aq.msLon}
			        
			    }<c:if test="${!status.last}">,</c:if>
				</c:forEach>
			];	
			
			make_MarkersAndOverlays(positions);
		}
		
		document.addEventListener("DOMContentLoaded", function () {
			cai_MakerAndOverlays()
		});
		
		
		
		
	</script>

</body>
</html>