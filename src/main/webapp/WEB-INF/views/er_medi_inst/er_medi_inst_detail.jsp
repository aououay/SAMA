<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<c:set var="CP" value="${pageContext.request.contextPath}" />

<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=1200">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet">
<link href="/assets/css/header.css" rel="stylesheet">
<link href="/assets/css/er_medi_inst_detail.css" rel="stylesheet">
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<title>Insert title here</title>
<style>
.customoverlay {
	position: relative;
	bottom: 85px;
	border-radius: 6px;
	border: 1px solid #ccc;
	border-bottom: 2px solid #ddd;
	float: left;
}

.customoverlay:nth-of-type(n) {
	border: 0;
	box-shadow: 0px 1px 2px #888;
}

.customoverlay a {
	display: block;
	text-decoration: none;
	color: #000;
	text-align: center;
	border-radius: 6px;
	font-size: 14px;
	font-weight: bold;
	overflow: hidden;
	background: #d95050;
	background: #d95050
		url(https://t1.daumcdn.net/localimg/localimages/07/mapapidoc/arrow_white.png)
		no-repeat right 14px center;
}

.customoverlay .title {
	display: block;
	text-align: center;
	background: #fff;
	margin-right: 35px;
	padding: 10px 15px;
	font-size: 14px;
	font-weight: bold;
}

.customoverlay:after {
	content: '';
	position: absolute;
	margin-left: -12px;
	left: 50%;
	bottom: -12px;
	width: 22px;
	height: 12px;
	background:
		url('https://t1.daumcdn.net/localimg/localimages/07/mapapidoc/vertex_white.png')
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
				<div class="col-1"></div>
				<div class="col-5 ps-4">
					<div class="card border-secondary h-100">
						<img style="cursor: pointer;"
							src="/assets/images/er_medi_inst/${emi_Info.hpid}.jpg"
							class="card-img-top" alt="${emi_Info.dutyNm} 이미지"
							height="539.125.px"
							onclick="window.open('https://map.kakao.com/link/search/${emi_Info.dutyNm}')">
						<div class="card-body">
							<h5 style="font-size: 24px; cursor: pointer;"
								class="card-title fw-bolder"
								onclick="window.open('https://map.kakao.com/link/search/${emi_Info.dutyNm}')">${emi_Info.dutyNm}</h5>
							<p style="font-size: 17px; color: #616161;"
								class="card-text mb-2">${emi_Info.dutyAddr}</p>
							<div class="d-flex">
								<p style="font-size: 17px; color: #616161;"
									class="card-text mb-0">대표전화&nbsp;:&nbsp;</p>
								<p style="font-size: 17px;"
									class="card-text mb-0 text-decoration-underline text-primary">${emi_Info.dutyTel}</p>
							</div>
							<div class="d-flex">
								<p style="font-size: 17px; color: #616161;"
									class="card-text mb-0">응급실전화&nbsp;:&nbsp;</p>
								<p style="font-size: 17px;"
									class="card-text mb-0 text-decoration-underline text-primary">${emi_Info.erTel}</p>
							</div>
						</div>
					</div>
				</div>
				<div class="col-6 ps-4">
					<div id="map" class="border rounded border-secondary"
						style="width: 590px; height: 650px;"></div>

					<p class="btn-group pt-1 mb-0" role="group"
						aria-label="Basic outlined example">
						<button type="button" class="btn btn-outline-primary"
							onclick="setOverlayMapTypeId('traffic')">교통정보 보기</button>
						<button type="button" class="btn btn-outline-primary"
							onclick="setOverlayMapTypeId('terrain')">지형정보 보기</button>
						<button type="button" class="btn btn-outline-primary"
							onclick="setOverlayMapTypeId('ROADMAP')">초기화</button>
					</p>
				</div>

			</div>

		</div>

		<div class="container pe-5 pt-5" style="padding-left: 132px">
			<ul class="nav nav-tabs pt-1 justify-content-start" id="myTab"
				role="tablist">
				<li class="nav-item" role="presentation">
					<button class="nav-link active" id="er_Bed-tab"
						data-bs-toggle="tab" data-bs-target="#er_Bed" type="button"
						role="tab" aria-controls="er_Bed" aria-selected="true">응급실
						가용병상 정보</button>
				</li>
			</ul>

			<!-- Tab panes -->
			<div class="tab-content border-start border-end border-bottom">
				<div class="tab-pane active" id="er_Bed" role="tabpanel"
					aria-labelledby="er_Bed-tab">
					<div class="d-flex justify-content-center pt-5 px-5 mb-5">
						<table
							class="table table-bordered table-striped table-light table-hover">
							<thead>
								<tr>
									<th class="text-center w-25" scope="col">항목</th>
									<th class="text-center" scope="col">상태</th>
								</tr>
							</thead>
							<tbody>
								<tr>
									<th class="ps-3" scope="row">입력일자</th>
									<td class="ps-3">${emi_Info.hviDt}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">응급실</th>
									<td class="ps-3">${emi_Info.hvec}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">수술실</th>
									<td class="ps-3">${emi_Info.hvoc}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">신경중환자실</th>
									<td class="ps-3">${emi_Info.hvcc}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">신생중환자실</th>
									<td class="ps-3">${emi_Info.hvncc}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">흉부중환자실</th>
									<td class="ps-3">${emi_Info.hvccc}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">일반중환자실</th>
									<td class="ps-3">${emi_Info.hvicc}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">입원실</th>
									<td class="ps-3">${emi_Info.hvgc}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">내과중환자실</th>
									<td class="ps-3">${emi_Info.hv2}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">외과중환자실</th>
									<td class="ps-3">${emi_Info.hv3}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">외과입원실(정형외과)</th>
									<td class="ps-3">${emi_Info.hv4}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">신경과입원실</th>
									<td class="ps-3">${emi_Info.hv5}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">신경외과중환자실</th>
									<td class="ps-3">${emi_Info.hv6}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">약물중환자실</th>
									<td class="ps-3">${emi_Info.hv7}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">화상중환자실</th>
									<td class="ps-3">${emi_Info.hv8}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">외상중환자실</th>
									<td class="ps-3">${emi_Info.hv9}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">VENTI(소아)</th>
									<td class="ps-3">${emi_Info.hv10}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">인큐베이터(보육기)</th>
									<td class="ps-3">${emi_Info.hv11}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">CT가용(가/부)</th>
									<td class="ps-3">${emi_Info.hvctYn}</td>
								</tr>
								<tr>
									<th class="ps-3" scope="row">MRI가용(가/부)</th>
									<td class="ps-3">${emi_Info.hvmriYn}</td>
								</tr>

							</tbody>
						</table>
					</div>
				</div>
			</div>

		</div>

		<div class="container mt-4">
			<div class="d-flex justify-content-end">
				<button type="button" class="btn btn-outline-primary" style="margin-right: 33px;"
					onclick="window.location.href='${CP}/er_medi_inst/list'">목록</button>
			</div>
		</div>

	</main>


	<!--// main---------------------------------------------------->





	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>

	<script type="text/javascript"
		src="//dapi.kakao.com/v2/maps/sdk.js?appkey=69ec97985c73a732909bbbdfc73cdfd3&libraries=services"></script>
	<script>
		var container = document.getElementById('map');
		var options = {
			center : new kakao.maps.LatLng(37.5642135, 127.0016985),
			level : 3
		};

		var map = new kakao.maps.Map(container, options);

		/* 		// 지도에 교통정보를 표시하도록 지도타입을 추가합니다
		 map.addOverlayMapTypeId(kakao.maps.MapTypeId.TRAFFIC); */

		// 일반 지도와 스카이뷰로 지도 타입을 전환할 수 있는 지도타입 컨트롤을 생성합니다
		var mapTypeControl = new kakao.maps.MapTypeControl();

		// 지도에 컨트롤을 추가해야 지도위에 표시됩니다
		// kakao.maps.ControlPosition은 컨트롤이 표시될 위치를 정의하는데 TOPRIGHT는 오른쪽 위를 의미합니다
		map.addControl(mapTypeControl, kakao.maps.ControlPosition.TOPRIGHT);

		// 지도 확대 축소를 제어할 수 있는  줌 컨트롤을 생성합니다
		var zoomControl = new kakao.maps.ZoomControl();
		map.addControl(zoomControl, kakao.maps.ControlPosition.RIGHT);
		
		// 지도에 추가된 지도타입정보를 가지고 있을 변수입니다
		var currentTypeId;

		// 버튼이 클릭되면 호출되는 함수입니다
		function setOverlayMapTypeId(maptype) {
		    var changeMaptype;
		    
		    // maptype에 따라 지도에 추가할 지도타입을 결정합니다
		    if (maptype === 'traffic') {            
		        
		        // 교통정보 지도타입
		        changeMaptype = kakao.maps.MapTypeId.TRAFFIC;     
		        
		    } else if (maptype === 'terrain') {
		        
		        // 지형정보 지도타입
		        changeMaptype = kakao.maps.MapTypeId.TERRAIN;    

		    } else if (maptype === 'ROADMAP') {
		        
		    	changeMaptype = kakao.maps.MapTypeId.ROADMAP;     
		    }
		    
		    // 이미 등록된 지도 타입이 있으면 제거합니다
		    if (currentTypeId) {
		        map.removeOverlayMapTypeId(currentTypeId);    
		    }
		    
		    // maptype에 해당하는 지도타입을 지도에 추가합니다
		    map.addOverlayMapTypeId(changeMaptype);
		    
		    // 지도에 추가된 타입정보를 갱신합니다
		    currentTypeId = changeMaptype;        
		}


		// 주소-좌표 변환 객체를 생성합니다
		var geocoder = new kakao.maps.services.Geocoder();

		// 주소로 좌표를 검색합니다
		geocoder
				.addressSearch(
						"${emi_Info.dutyAddr}",
						async function(result, status) {
							// 정상적으로 검색이 완료됐으면 
							if (status === kakao.maps.services.Status.OK) {
								var coords = new kakao.maps.LatLng(result[0].y,
										result[0].x);

								// 지도의 중심을 결과값으로 받은 위치로 이동시킵니다
								map.setCenter(coords);
								

								var imageSrc = '/assets/images/young_min/hospital_marker.png', // 마커이미지의 주소입니다    
								imageSize = new kakao.maps.Size(34, 36) // 마커이미지의 크기입니다

								// 마커의 이미지정보를 가지고 있는 마커이미지를 생성합니다
								var markerImage = new kakao.maps.MarkerImage(
										imageSrc, imageSize)

								// 마커를 생성합니다
								var marker = new kakao.maps.Marker({
									position : coords,
									image : markerImage
								// 마커이미지 설정 
								});

								// 마커가 지도 위에 표시되도록 설정합니다
								marker.setMap(map);

								// 마커에 클릭이벤트를 등록합니다
								kakao.maps.event.addListener(marker, 'click',
										function() {

											map.setLevel(3);
											map.panTo(coords);

										});
								
								// 커스텀 오버레이에 표출될 내용으로 HTML 문자열이나 document element가 가능합니다
								const content =`
								    <div class="customoverlay">
							        <a href="https://map.kakao.com/link/search/${emi_Info.dutyNm}" target="_blank">
							            <span class="title">${emi_Info.dutyNm}</span>
							        </a>
							    </div>
							`;

							
								// 커스텀 오버레이를 생성합니다
								var customOverlay = new kakao.maps.CustomOverlay(
										{
											map : map,
											position : coords,
											content : content,
											yAnchor : 0.1
										});

							}
						});
	</script>

</body>
</html>