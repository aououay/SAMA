<%@ page contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html>
<head>
<title>대피소 정보</title>
<script type="text/javascript"
	src="//dapi.kakao.com/v2/maps/sdk.js?appkey=15c1760c31c6bd80ba0ef8f34c4fa29a"></script>
<script>
function initMap() {
    var lat = parseFloat("${lat}");
    var lng = parseFloat("${lng}");
    var address = "${address}";

    if (isNaN(lat) || isNaN(lng)) {
        lat = 37.5665;
        lng = 126.9780;
    }

    var mapContainer = document.getElementById("map");
    var mapOption = {
        center: new kakao.maps.LatLng(lat, lng),
        level: 3
    };

    var map = new kakao.maps.Map(mapContainer, mapOption);
    var marker = new kakao.maps.Marker({
        position: new kakao.maps.LatLng(lat, lng)
    });
    marker.setMap(map);

    // 🔹 커스텀 오버레이 추가
    var content = `
        <div class="customoverlay" data-address="${address}" data-lat="${lat}" data-lng="${lng}">
            <div class="title">${address}</div>
            <button class="btn" onclick="navigateToShelter(this)">길찾기</button>
        </div>
    `;

    var overlay = new kakao.maps.CustomOverlay({
        content: content,
        position: marker.getPosition(), // marker.position -> marker.getPosition()
        yAnchor: 1.5
    });

    overlay.setMap(map);
}

// 🔹 길찾기 버튼 클릭 시 동작
function navigateToShelter(button) {
    var parent = button.closest(".customoverlay");
    var address = parent.getAttribute("data-address");
    var lat = parent.getAttribute("data-lat");
    var lng = parent.getAttribute("data-lng");
    
    if (!address || address.trim() === "") {
        alert("대피소 주소가 올바르게 로드되지 않았습니다.");
        return;
    }

    var url = `https://map.kakao.com/link/to/${address},${lat},${lng}`;
    window.open(url, "_blank");
}

window.onload = initMap;

</script>
<style>
.container {
	width: 400px;
	margin: 0 auto;
	text-align: center;
	font-family: Arial, sans-serif;
	border: 1px solid #ccc;
	padding: 10px;
	border-radius: 8px;
}

.header {
	font-size: 20px;
	font-weight: bold;
	margin-bottom: 10px;
}

.info-table {
	width: 100%;
	border-collapse: collapse;
	margin-top: 10px;
}

.info-table th, .info-table td {
	border: 1px solid #ccc;
	padding: 8px;
	text-align: left;
}

.info-table th {
	background-color: #f8f8f8;
}

#map {
	width: 100%;
	height: 300px;
	margin-top: 10px;
}

/* 🔹 커스텀 오버레이 스타일 */
.customoverlay {
	background: white;
	border: 1px solid #ccc;
	padding: 10px;
	border-radius: 8px;
	text-align: center;
	box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.2);
}

.customoverlay .title {
	font-size: 14px;
	font-weight: bold;
	margin-bottom: 5px;
}

.btn {
	display: inline-block;
	padding: 5px 10px;
	font-size: 14px;
	color: white;
	background-color: #007bff;
	border: none;
	border-radius: 5px;
	cursor: pointer;
}

.btn:hover {
	background-color: #0056b3;
}
</style>
</head>
<body>
	<div class="container">
		<div class="header">
			지진옥외대피시설명: <b>${name}</b>
		</div>

		<p>주소: ${address}</p>
		<div id="map"></div>

		<table class="info-table">
			<tr>
				<th>최대 수용 가능 인원</th>
				<td>${capacity}명</td>
			</tr>
			<tr>
				<th>시설 면적</th>
				<td>${facilityArea}㎡</td>
			</tr>
			<tr>
				<th>사용 구분</th>
				<td>${useSeCd}</td>
			</tr>
			<tr>
				<th>관리 기관</th>
				<td>${managementOffice}</td>
			</tr>
		</table>
	</div>
</body>
</html>
