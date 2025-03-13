<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<html>
<head>
    <title>과거지진</title>
    <link rel="stylesheet" type="text/css" href="/assets/css/eqk.css">
    <link rel="stylesheet" type="text/css" href="/assets/css/bootstrap.min.css">
    <link href="/assets/css/header.css" rel="stylesheet">
    <style>
		#container {
			max-width: 1320px;
		    margin: 0 auto;
		    padding: 0 15px;
		    margin-top: 36px;
		}
        #earthquakeChart {
            width: 100%;
            height: 700px !important;
            margin: auto;
            margin-top: 50px !important;
        }
    </style>


</head>



<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

<div id="container" class="earthquakeList">
    <div class="leftArea">
        <div class="titArea">
            <div class="selectArea">
                <%--controller에서 지정한 searchDiv와 searchWord를 js 파일로 전달함
                js에서 전달받은 searchDiv를 cotroller로 전달하고 맞는 searchWord의 바인드변수를 myBatis에서 사용--%>
                <select class="selectTypeA" style="width:170px;" name="year" id="year" onchange="changeYear()">
                    <option value="">선택</option>
                    <option value="2025" ${searchWord == '2025' ? 'selected' : ''}>2025</option>
                    <option value="2024" ${searchWord == '2024' ? 'selected' : ''}>2024</option>
                    <option value="2023" ${searchWord == '2023' ? 'selected' : ''}>2023</option>
                    <option value="2022" ${searchWord == '2022' ? 'selected' : ''}>2022</option>
                    <option value="2021" ${searchWord == '2021' ? 'selected' : ''}>2021</option>
                    <option value="2020" ${searchWord == '2020' ? 'selected' : ''}>2020</option>
                    <option value="2019" ${searchWord == '2019' ? 'selected' : ''}>2019</option>
                    <option value="2018" ${searchWord == '2018' ? 'selected' : ''}>2018</option>
                    <option value="2017" ${searchWord == '2017' ? 'selected' : ''}>2017</option>
                    <option value="2016" ${searchWord == '2016' ? 'selected' : ''}>2016</option>
                </select>
            </div>
        </div>
        <div class="tableArea" style="margin-top: 35px">
            <div id="map" class="mapArea" id="tmpMapArea">
                <!-- 여기서 지진 발생지도를 표시할 수 있습니다. -->
                <script type="text/javascript" src="//dapi.kakao.com/v2/maps/sdk.js?appkey=f3175384932ff101a4e21ba1cf4377df"></script>
                <script type="text/javascript">
                    var container = document.getElementById('map'); //지도를 담을 영역의 DOM 레퍼런스
                    var options = { //지도를 생성할 때 필요한 기본 옵션
                        center: new kakao.maps.LatLng(37.5665, 126.9780), // 한국의 대략적인 중앙 좌표(서울)
                        level: 14 // 지도 레벨(확대, 축소 정도)
                    };

                    var map = new kakao.maps.Map(container, options); //지도 생성 및 객체 리턴

                    // 지도 레벨 변경 후 중심이 이상해지지 않도록 설정
                    function changeLevel(newLevel) {
                        // 지도 레벨 변경
                        map.setLevel(newLevel);

                        // 레벨 변경 후 중심 좌표를 다시 설정하여 이상한 위치로 이동하지 않도록 함
                        map.setCenter(new kakao.maps.LatLng(37.5665, 126.9780)); // 서울 중심으로 다시 설정
                    }

                        // 선택한 지역의 위도 경도로 이동함
                    function panTo() {
                        // 이동할 위도 경도 위치를 생성함
                        var moveLatLon = new kakao.maps.LatLng(37.5665, 126.9780); // 예시로 제주도 방향으로 이동함
                        // 지도 중심을 부드럽게 이동함
                        // 만약 이동할 거리가 지도 화면보다 크면 부드러운 효과 없이 이동함
                        map.panTo(moveLatLon);
                    }
                        //마커 이미지를 불러옴
                    var imageSrc = '/assets/images/yeong-chang/dropBlueBold.png',
                        imageSize = new kakao.maps.Size(24, 29), // 마커이미지의 크기 지정
                        imageOption = {offset: new kakao.maps.Point(17, 29)}; // 마커이미지의 옵션 마커의 좌표와 일치시킬 이미지 안에서의 좌표를 설정

                    // 마커의 이미지정보를 가지고 있는 마커이미지를 생성
                    var markerImage = new kakao.maps.MarkerImage(imageSrc, imageSize, imageOption),
                        markerPosition = new kakao.maps.LatLng(37.54699, 127.09598); // 마커가 표시될 위치입니다


                    // 마커를 생성합니다
                    var marker = new kakao.maps.Marker({
                        position: markerPosition,
                        image: markerImage // 마커이미지 설정
                    });


                    // 마커가 지도 위에 표시되도록 설정합니다
                    marker.setMap(map);


                    // 마커 위치 업데이트 함수
                    function updateMarkerPosition(lat, lon) {

                        // 새로운 마커 위치 설정
                        var newPosition = new kakao.maps.LatLng(lat, lon);

                        // 마커 위치 변경
                        marker.setPosition(newPosition);

                        // 지도 중심을 해당 위치로 이동
                        map.panTo(newPosition);
                    }
                </script>
            </div>
        </div>
        <button onclick="changeLevel(12)">레벨 12로 변경</button>
        <button onclick="changeLevel(13)">레벨 13으로 변경</button>
        <button onclick="panTo()">지도 중심좌표 이동시키기</button>
    </div>


    <div class="rightArea">
        <div class="topArea">
            <ul id="sortList">
                <%--controller에서 지정한 searchDiv와 searchWord를 js 파일로 전달함
                js에서 전달받은 searchDiv를 cotroller로 전달하고 맞는 searchDiv의 공용sql을 myBatis에서 사용--%>
                <li id="listDate"><a href="#" onclick="changeSelection('10')">최신순</a></li>
                <li id="listMng"><a href="#" onclick="changeSelection('20')">규모순</a></li>
            </ul>
        </div>
        <div class="tableArea" id="content">
            <div class="titArea">
                <div class="tableSet">
                    <span>발생시각</span>
                    <span>규모</span>
                    <span>위/경도</span>
                    <span>위치</span>
                </div>
            </div>
            <ul class="tableList scr1" id="getEarthquakeList">
                    <%--controller에서 가져온 earthquakeVO타입의 데이터를 뿌려줌--%>
                <c:forEach var="earthquake" items="${list}">
                    <li onclick="updateMarkerPosition(${earthquake.eqkLat}, ${earthquake.eqkLon})">
                        <span id="formattedTime">${earthquake.eqkReportTime}</span>
                        <span>${earthquake.eqkMt}</span>
                        <span>${earthquake.eqkLat}, ${earthquake.eqkLon}</span>
                        <span>${earthquake.eqkLoc}</span>
                    </li>
                </c:forEach>
            </ul>
        </div>
    </div>
</div>




<div id="container" class="earthquakeList">
    <div class="leftArea" style="margin-top: 30px">
        <div class="titArea">

        </div>


        <div class="tableArea">
            <div id="map1" class="mapArea" >
                <!-- 여기서 지진 발생지도를 표시할 수 있습니다. -->
                <script type="text/javascript" src="//dapi.kakao.com/v2/maps/sdk.js?appkey=f3175384932ff101a4e21ba1cf4377df"></script>
                <script type="text/javascript">
                    var container1 = document.getElementById('map1'); // 지도를 담을 영역의 DOM 레퍼런스
                    var options1 = {
                        center: new kakao.maps.LatLng(37.5665, 126.9780), // 초기 지도 중심
                        level: 14 // 지도의 확대 수준
                    };
                    var map1 = new kakao.maps.Map(container1, options1); // 지도 생성 및 객체 리턴

                    // 마커를 표시할 위치와 title 객체 배열
                    var positions = [
                        // 서버에서 받은 데이터를 바탕으로 마커 위치를 설정
                        <c:forEach var="earthquake" items="${eqkOccurrence}">
                        {
                            title: '지진 발생 (' + '${earthquake.earthquakeYear}' + '년 ' + '${earthquake.magnitudeRange}' + ')',
                            latlng: new kakao.maps.LatLng(${earthquake.eqkLat}, ${earthquake.eqkLon}),
                            count: ${earthquake.eqkCount},
                            eqkMt: '${earthquake.magnitudeRange}'
                        },
                        </c:forEach>
                    ];

                    var currentContent = null;
                    // 마커 이미지의 URL을 지진 규모에 따라 동적으로 설정
                    function getMarkerImage(eqkMt) {
                        // eqkMt 비교하여 해당 마커로 표시
                        // 쿼리문에서 해당값을 만들어 그값교 비교함
                        var eqkMtValue = eqkMt;
                            console.log(eqkMtValue);
                        if (eqkMtValue >= '5.0~') {
                            return "/assets/images/yeong-chang/red.png"; // 5.0 이상 마커 (빨간색)
                        } else if (eqkMtValue >= '4.0 - 4.9') {
                            return "/assets/images/yeong-chang/orange.png"; // 4.0 ~ 4.9 마커 (주황색)
                        } else if (eqkMtValue >= '3.0 - 3.9') {
                            return "/assets/images/yeong-chang/blue.png"; // 3.0 ~ 3.9 마커 (파란색)
                        } else {
                            return "/assets/images/yeong-chang/green.png"; // 2.9 이하 마커 (초록색)
                        }
                    }

                    var infowindow = new kakao.maps.InfoWindow(); // 하나의 정보 창을 전역 변수로 생성

                    // 마커를 생성하여 지도에 표시합니다
                    for (var i = 0; i < positions.length; i++) {
                        var imageSize1 = new kakao.maps.Size(24, 35);
                        var markerImage1 = new kakao.maps.MarkerImage(getMarkerImage(positions[i].eqkMt), imageSize1);
                        var marker1 = new kakao.maps.Marker({
                            map: map1,
                            position: positions[i].latlng,
                            title: positions[i].title,
                            image: markerImage1
                        });

                        // 마커에 클릭 이벤트를 추가하여 정보 창을 띄웁니다
                        kakao.maps.event.addListener(marker1, 'click', (function(marker, i) {
                            return function() {
                                // 기존 정보 창 닫기
                                infowindow.close();

                                // 모달창에 보여줄 정보 표시
                                var content = '<div style="padding:5px;">' +
                                    '지진 발생 연도: ' + positions[i].title + '<br>' +
                                    '지진 횟수: ' + positions[i].count + '</div>';

                                // 정보 창 내용 업데이트
                                infowindow.setContent(content);
                                infowindow.open(map1, marker);
                            };
                        })(marker1, i));
                    }

                    // 지도 클릭 시 정보 창 닫기
                    kakao.maps.event.addListener(map1, 'click', function() {
                        infowindow.close();
                    });

                </script>

                <!-- 연도별 및 규모별 지진 횟수를 지도안에 표형태로 표시 -->
                <div id="eqkStats" class="eqkStats">
                    <h3>연도별 규모별 지진 횟수</h3>
                    <table>
                        <thead>
                        <tr>
                            <th>규모</th>
                            <th><img src="/assets/images/yeong-chang/green.png" style="width: 20px"></th>
                            <th><img src="/assets/images/yeong-chang/blue.png" style="width: 20px"></th>
                            <th><img src="/assets/images/yeong-chang/orange.png" style="width: 20px"></th>
                            <th><img src="/assets/images/yeong-chang/red.png" style="width: 20px"></th>
                        </tr>
                        </thead>
                        <tbody>
                        <c:forEach var="earthquake" items="${eqkByYearAll}">
                            <tr>
                                <td>${earthquake.magnitudeRange}</td>
                                <%--earthquake.magnitudeRange 값이 비교값과 동일하다면 earthquake.eqkCount를 출력하고 그값과 동일하지 않다면 0을 출력--%>
                                <td>${earthquake.magnitudeRange == '2.0 - 2.9' ? earthquake.eqkCount : 0}</td>
                                <td>${earthquake.magnitudeRange == '3.0 - 3.9' ? earthquake.eqkCount : 0}</td>
                                <td>${earthquake.magnitudeRange == '4.0 - 4.9' ? earthquake.eqkCount : 0}</td>
                                <td>${earthquake.magnitudeRange == '5.0~' ? earthquake.eqkCount : 0}</td>
                            </tr>
                        </c:forEach>
                        </tbody>
                    </table>
                </div>

            </div>

        </div>
    </div>


    <div class="rightArea" style="margin-top: 45px">
        <div class="tableArea">
            <!-- 차트가 그려질 캔버스 -->
            <canvas id="earthquakeChart"></canvas>
            <div class="titArea">
                <div class="tableSet">
                </div>
            </div>
            <ul class="tableList scr1" id="">
            </ul>
        </div>
    </div>
</div>

<!-- Chart.js -->
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script src="/assets/js/eqk/eqk_page.js"></script>
<script>
    // JSP에서 eqkByYear 데이터를 JSON 형태로 JavaScript로 전달하기
    var earthquakeData = [];
    <c:forEach var="eqkByYear" items="${eqkByYear}">
    earthquakeData.push({
        year: '${eqkByYear.earthquakeYear}',
        magnitudeRange: '${eqkByYear.magnitudeRange}',
        count: ${eqkByYear.eqkCount}
    });
    </c:forEach>

    /// 고유 연도 리스트
    var years = [...new Set(earthquakeData.map(item => item.year))];

    // 규모 범위 설정
    var magnitudeRanges = ['3.0 - 3.9', '4.0 - 4.9', '5.0~'];

    // 각 연도별 규모 범위별 지진 수량을 합산
    var dataset = magnitudeRanges.map(range => {
        return {
            label: range,
            data: years.map(year => {
                return earthquakeData.filter(item => item.year === year && item.magnitudeRange === range)
                    .reduce((sum, item) => sum + item.count, 0);
            })
            ,
            backgroundColor:
                range === '3.0 - 3.9' ? 'rgb(69,100,237)' :
                    range === '4.0 - 4.9' ? 'rgb(241,189,58)' : 'rgb(239,43,43)',
            borderColor: 'black',
            borderWidth: 1,
            borderRadius: 8,  // 여기서 radius를 설정
        };
    });
    // 차트 그리기
    var ctx = document.getElementById('earthquakeChart').getContext('2d');
    var earthquakeChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: years, // X축 레이블: 연도
            datasets: dataset // Y축 데이터: 규모별 데이터셋
        },
        options: {
            maxBarThickness: 20,
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    title: {
                        display: true
                    },
                    stacked: true,  // 막대가 쌓이는 차트
                    grid: {
                        offset: true
                    }
                },
                y: {
                    beginAtZero: true,
                    stacked: true, // 막대가 쌓이는 차트
                    title: {
                        display: true
                    }
                }
            },
            plugins: {
                legend: {
                    position: 'top',
                }
            },
        },
    });
    document.addEventListener("DOMContentLoaded", function() {
        // 모든 "formattedTime" span 요소 가져오기
        let timeElements = document.querySelectorAll("#formattedTime");

        timeElements.forEach(function(element) {
            let timeStr = element.innerText.trim();
            if (timeStr.length === 12) {
                let formattedTime = timeStr.substring(0, 4) + "-" +
                    timeStr.substring(4, 6) + "-" +
                    timeStr.substring(6, 8) + " " +
                    timeStr.substring(8, 10) + ":" +
                    timeStr.substring(10, 12);
                element.innerText = formattedTime;
            }
        });
    });

</script>
<div class="container-fluid px-0">
	<!-- footer-->
	<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
	<!--// footer-------------------------------------------------->
</div>
    </body>
    </html>
