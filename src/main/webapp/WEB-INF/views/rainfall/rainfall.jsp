<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>

<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="/assets/css/bootstrap.min.css" rel="stylesheet" >
	<link href="/assets/css/header.css" rel="stylesheet" >
	<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
    
    <title>강우량 정보</title>
    <script src="//dapi.kakao.com/v2/maps/sdk.js?appkey=feb2e034645bde27f5e8d4d1e4bfc50c&libraries=services"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <script src="/assets/js/jquery_3_7_1.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/chartjs-plugin-datalabels"></script>

    <style>
    #rf-container { 
    	display: flex;
    	}
    #map { 
    	width: 50%; 
    	height: 600px; 
    	margin-top: 30px;
    	padding-right: 20px;
    	}
	#legend {
	    position: absolute;
	    bottom: 20px; /* 아래쪽 여백 */
	    right: 20px;  /* 오른쪽 여백 */
	    background: rgba(255, 255, 255, 0.9);
	    padding: 10px;
	    border-radius: 8px;
	    box-shadow: 0px 0px 5px rgba(0, 0, 0, 0.3);
	    z-index: 10;
		}
		
	#legend img {
	    width: 380px;
	    height: auto;
		}    	
    #infoPanel { 
    	width: 50%; 
    	padding: 30px;
    	padding-right: 0px;
    	}
	.non-wl {
	    text-align: center; 
        padding: 40px;
        color: gray; 
        border: 1px solid #ddd; 
        border-top: 3px solid black; 
        font-size: 20px;
        font-weight: bold;
        height: 400px;
        display: flex;
        align-items: center;
        justify-content: center;
        background-color: #f9f9f9;
		}
    .table-container { 
        width: 100%; 
        overflow-x: auto; 
        white-space: nowrap;
    }

    .table-scroll { 
        display: block; 
        width: 100%; /* 가로 길이 제한 */
        max-width: 100%; /* 화면을 넘어가지 않도록 설정 */
    }

    table { 
        width: 100%; 
        border-collapse: collapse; 
        margin-top: 20px; 
    }

    th{ 
        border: 1px solid #ddd; 
        padding: 10px; 
        text-align: center; 
        background: #f4f4f4;
    }
    
    td { 
        border: 1px solid #ddd; 
        padding: 10px; 
        text-align: center; 
    }
    /* 차트 크기 고정 */
    #rainChart {
        margin-top: 20px;
        max-width: 100%;
        height: 300px !important; /* 차트가 너무 커지는 문제 해결 */
    }
    .sourceText {
	    font-size: 14px;
	    color: darkgray ;
    }
    
    #guRainfallContainer {
	    height: 250px;
	    width: 1200px;
    	margin:0 auto;
	}
	
	#guRainfallContainer h2{
    	margin-top: 80px;
    }
	
	#lastUpdated{
	    width: 100%; 
	    text-align: center;
	}
	
	#lastUpdated table {
	    width: 80%;
	    margin: 0 auto;
	}

	
	.tabs {
	    display: flex;
	    margin-bottom: 10px;
	    border-bottom: 2px solid navy;
	}
	
	.tab-button {
	    padding: 10px 100px;
	    border: 1px solid white;
	    background: #ddd;
	    cursor: pointer;
	    font-size: 16px;
	   
	}
	
	.tab-button.active {
	    background: navy;
	    color: white;
	}
	
	#rainfallImage {
    display: flex;
    justify-content: center; /* 수평 중앙 정렬 */
    align-items: center; /* 수직 중앙 정렬 (필요할 경우) */
}

	#rainfallImage img{
		max-width: 1320px;
	}
	.textbox{
		margin-top: 50px;
	}
	.textbox h3{
		margin-top: 20px;
	}
	.line{
		border-top: 1px solid gray;
		padding: 20px 0px;
		margin-top: 20px;
	}
	
	#rainfallImage {
	    position: relative;
	    width: 100%;
	    overflow: hidden;
	}

	#rainfallImage::before, 
	#rainfallImage::after {
	    content: "";
	    position: absolute;
	    top: 0;
	    height: 100%;
	    width: 50%;
	    z-index: -1;
	}

	#rainfallImage::before {
	    left: 0;
	    background-color: #F1F1F1; /* 왼쪽 배경색 (네이비톤) */
	}

	#rainfallImage::after {
	    right: 0;
	    background-color: #E3E1DE; /* 오른쪽 배경색 (밝은 네이비톤 또는 원하는 컬러) */
	}

	#rainfallImage {
	    display: flex;
	    justify-content: center;
	    align-items: center;
	    position: relative;
	    overflow: hidden;
	}

	#rainfallImage img {
	    max-width: 1320px;
	    width: 100%;
	    height: auto;
	    z-index: 1;
	}

    </style>
</head>
<!--  header -->
<div id="" class="">
	<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
</div>
<!--// header-------------------------------------------------->
<body>


<div id="container" class="container" style="margin-bottom: 200px;">
	<div id="rf-container">
	    <!-- 지도 영역 -->
	    <div id="map">
		    <div id="legend">
				<img src="/assets/images/eun_bi/rainfallLegend.png" alt="범례">
			</div>
	    </div>
	
	    <!-- 관측소 정보 영역 -->
	    <div id="infoPanel">
	    	<h2>호우 특보현황</h2>	  
				<c:choose>
					<c:when test="${empty warnings}">
				         <!-- 데이터가 없을 때 -->
				         <div class="non-wl">					         
				             <p><strong>⚠ 현재 발령중인 특보 현황이 없습니다.</strong></p>
				         </div>
					</c:when>
					<c:otherwise>
						 <div id="floodWarningTable">
					     	<table border="1">
								<thead>
						            <tr>
						                <th>관측지점</th>
						                <th>예보번호</th>
						                <th>발령일시</th>
						                <th>예보종류</th>
						                <th>예보상태</th>
						            </tr>
						        </thead>
						        <tbody id="floodWarningBody">				        
					                <c:forEach var="rain" items="${warnings}" varStatus="status">
					                    <tr>
					                        <td>${rain.obsNm}</td>
					                        <td>2025-${rain.rfObsCd}호</td>
					                        <td>${rain.meaDt}</td>
					                        <td>${rain.state}</td>
					                        <td>${rain.forecastState}</td>
						                </tr>
						            </c:forEach>			 
						        </tbody>
						    </table>
						</div>	
					</c:otherwise>
				</c:choose>
			<div id="stationInfo" style="display: none;">
		        <h2 id="stationName"></h2>
		        <p style="color:gray;">주소: <span id="stationAddr"></span></p>
	
		        <!-- 강우량 테이블 -->
		        <div class="table-container">
		            <div class="table-scroll">
		                <table class="rainfall-table">
		                    <thead>
		                        <tr id="rainfallHeader">
		                            <th>시간</th>
		                            <c:forEach var="rf" items="${rainfallData}">
					                	<td>${rf.meaDt}</td>
					                </c:forEach>
		                        </tr>
		                    </thead>
		                    <tbody>
		                        <tr id="rainfallData">
		                            <th>강우량(mm)</th>
		                            <c:forEach var="rf" items="${rainfallData}">
			                        	<td>${rf.rf}</td>
			                    	</c:forEach>
		                        </tr>
		                    </tbody>
		                </table>
		            </div>
		        </div>
	
	        	<canvas id="rainChart"></canvas>
			</div>
		</div>
	</div>
	   
	<div class = "sourceText">    
		<span>기상청 제공</span>
	</div>
	
	<div id="guRainfallContainer">
	    <h2>자치구별 강수량
		    <c:if test="${not empty guRainfall}">
		       <span style="color: gray; font-size: 16px; margin-left: 20px; padding-left: 15px;">
		            <c:out value="${guRainfall[0].regDt}" /> 기준
		       </span>
		   </c:if>		    
	    </h2>
	    <p id="lastUpdated"></p>
	    <div class="tabs">
	        <button class="tab-button active" data-type="1h">1시간 강수량</button>
	        <button class="tab-button" data-type="24h">24시간 강수량</button>
	        <button class="tab-button" data-type="48h">48시간 강수량</button>
	    </div>
	    <canvas id="rainfallChart"></canvas>
	
	</div>

</div>
<div id="rainfallImage">
	<img src="/assets/images/eun_bi/rainfallBanner.png" alt="호우발생시행동요령">
</div>

<div id="container" class="container">
	<div class="textbox">
	<h3>호우주의보  ·  호우경보 기준</h3>
		<div class = "line">
	           <span>· 호우주의보 : 3시간 누적강우량이 60mm 이상 예상되거나 12시간 누적강우량이 110mm 이상 예상될 때</span></br>
	           <span>· 호우경보 : 3시간 누적강우량이 90mm 이상 예상되거나 12시간 누적강우량이 180mm 이상 예상될 때</span>
		</div>
	</div>
</div>

	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>


<script>
//자치구 시간 타입별 강수량
let rainfallChart;

document.addEventListener("DOMContentLoaded", function() {
    fetchRainfallData("1h"); // 기본적으로 1시간 강수량 표시
 	//모든 탭 버튼에 클릭 이벤트 추가
    document.querySelectorAll(".tab-button").forEach(button => {
        button.addEventListener("click", function() {
        	// 클릭한 버튼만 활성화 스타일 적용
            document.querySelectorAll(".tab-button").forEach(btn => btn.classList.remove("active"));
            this.classList.add("active");
            // 버튼의 data-type 속성을 가져와 해당 시간 데이터 요청
            const type = this.dataset.type;
            fetchRainfallData(type);
        });
    });
});
//자치구 시간 타입별 강수량
function fetchRainfallData(type) {
    fetch("/rainfall/gurainfall") // 모든 자치구의 강수량을 불러오는 API
    .then(response => response.json())
    .then(data => {
        console.log("받은 데이터:", data);

        let labels = [];
        let values = [];
        
        data.forEach(row => {
            labels.push(row.guNm); //x축에 자치구명 추가
            if (type === "1h") values.push(row.rf);
            else if (type === "24h") values.push(row.rf_24h);
            else if (type === "48h") values.push(row.rf_48h);
        });
        console.log(` ${type} 강수량 데이터`, values);
        updateChart(labels, values, type);
    })
    .catch(error => console.error("데이터 불러오기 오류:", error));
}


//자치구별 강수량 bar차트
function updateChart(labels, values, type) {
    const ctx = document.getElementById("rainfallChart").getContext("2d");

    if (rainfallChart) {
        rainfallChart.destroy();
    }

    const chartTitle = type === "1h" ? "1시간 강수량(mm)"
                      : type === "24h" ? "24시간 강수량(mm)"
                      : "48시간 강수량(mm)";

    rainfallChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: labels,
            datasets: [{
                label: chartTitle,
                data: values,
                backgroundColor: "royalblue",
                borderColor: "blue",
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: { display: false },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            return context.raw;
                        }
                    }
                },
                datalabels: {  // 막대기 값 표시 설정 추가
                    anchor: 'center',   // 값 위치 (start, center, end)
                    align: 'top',   // 정렬 위치 (top, middle, bottom)
                    formatter: function(value) {
                    	return value === 0 ? "0.0" : value.toFixed(1) ;    // 소수점 1자리까지 표시
                    },
                    color: 'skyblue',  // 값 색상
                    font: {
                        weight: 'bold',
                        size: 12
                    }
                }
                               
            },
            scales: {
                x: {
                	grid: {
                    display: false // 기준선 제거
                	},               	
                    ticks: {
                        autoSkip: false, // 자치구명 생략 방지
                        maxRotation:0, // 기울여서 표시
                        minRotation: 0
                    }
                },
                y: { beginAtZero: true,
                	grid: {
                        display: false // 기준선 제거
                    },
                	ticks: {               		
                        stepSize: 0.5 // 눈금 간격을 0.5mm 단위로 설정
                    } 
                }
            }
        },
        plugins: [ChartDataLabels]
    });
} 


// 카카오 맵
var map;

function initMap() {
    map = new kakao.maps.Map(document.getElementById('map'), {
        center: new kakao.maps.LatLng(37.5665, 126.9780),
        level: 9
    });
    var markerIcons = {
    		normal: "/assets/images/eun_bi/dropBlueBold.png", // 1mm미만
    		five: "/assets/images/eun_bi/dropPurpleBold.png", // 5mm 미만
    		ten: "/assets/images/eun_bi/dropGreenBold.png", // 10mm미만
    		warning: "/assets/images/eun_bi/dropYellowBold.png", // 30mm미만 주의보
    		alert: "/assets/images/eun_bi/dropRedBold.png" // 30mm이상 경보
    };
    var locations = [
        <c:forEach var="station" items="${allRainfallStations}">
        <c:set var="markerState" value="normal"/>
        <c:choose>
	        <c:when test="${station.hourState}">
	        	<c:set var="markerState" value="five"/>
	    	</c:when>
		    <c:when test="${station.hourState}">
		    	<c:set var="markerState" value="ten"/>
			</c:when>
	        <c:when test="${station.hourState}">
	            <c:set var="markerState" value="warning"/>
	        </c:when>
	        <c:when test="${station.hourState}">
	            <c:set var="markerState" value="alert"/>
	        </c:when>
	    </c:choose>
        { 
            name: "<c:out value='${station.obsNm}'/>", 
            lat: ${station.lat}, 
            lon: ${station.lon}, 
            id: ${station.rfObsCd},
            state: "<c:out value='${markerState}'/>" // 강수량에 따른 상태 추가
        },
    </c:forEach>
    ];
    console.log("마커 데이터 확인:", locations);
    locations.forEach(function(station) {
    	console.log("마커 상태 확인:", station.state); 
    	var imageSrc = markerIcons[station.state] || markerIcons["normal"];
        
        var markerImage = new kakao.maps.MarkerImage(imageSrc, new kakao.maps.Size(15, 22));
    	
        var marker = new kakao.maps.Marker({
            map: map,
            position: new kakao.maps.LatLng(station.lat, station.lon),
            title: station.name,
            image: markerImage
        });

        var infoWindow = new kakao.maps.InfoWindow({
            content: '<div style="padding:5px;font-size:14px;white-space:nowrap;">' + station.name + '</div>',
            removable: false
        });

        kakao.maps.event.addListener(marker, 'mouseover', function () {
            console.log("마커에 마우스를 올림:", station.name);
            infoWindow.open(map, marker);
        });

        kakao.maps.event.addListener(marker, 'mouseout', function () {
            infoWindow.close();
        });

        kakao.maps.event.addListener(marker, 'click', function() {
       	   console.log("마커 클릭됨:", station.name);

       	    // 테이블, 페이징, 제목 등의 요소 가져오기
       	    var floodTable = document.getElementById("floodWarningTable"); // 홍수 발생 현황 테이블
       	    var pagination = document.querySelector(".pagination-container"); // 페이징
       	    var stationInfo = document.getElementById("stationInfo"); // 관측소 정보 영역
       	    var floodTitle = document.querySelector("h2"); // "홍수 발생현황" 제목
       	    var noWarning = document.querySelector(".non-wl"); // "현재 발령중인 특보 현황 없음" 메시지

       	    // 기존 홍수 발생 현황 관련 UI 숨김
       	    if (floodTable) floodTable.style.display = "none"; // 홍수 테이블 숨김
       	    if (pagination) pagination.style.display = "none"; // 페이징 숨김
       	    if (floodTitle) floodTitle.style.display = "none"; // 제목 숨김
       	    if (noWarning) noWarning.style.display = "none"; // 특보 없음 메시지 숨김

       	    // 선택한 관측소 정보 표시
       	    if (stationInfo) stationInfo.style.display = "block";
            loadRainfallData(station.id);
        });
    });

}
//호우 특보현황
function loadRainfallWarnings() {
    fetch('/rainfall/warnings')
        .then(response => response.json())
        .then(data => {
        	console.log("호우 특보 데이터:", data); 
            let tableBody = document.getElementById("floodWarningBody"); // tbody의 ID 사용
            let floodTable = document.getElementById("floodWarningTable");
            let noWarningMessage = document.querySelector(".non-wl");

            if (!tableBody) {
                console.error("floodWarningTable의 tbody를 찾을 수 없음");
                return;
            }

            tableBody.innerHTML = ""; // 기존 데이터 삭제           

            if (data.length === 0) {
                noWarningMessage.style.display = "block";  // 특보 없음 메시지 표시
                document.getElementById("floodWarningTable").style.display = "none"; // 테이블 숨김
            } else {
                noWarningMessage.style.display = "none"; // 특보 없음 메시지 숨김
                document.getElementById("floodWarningTable").style.display = "block"; // 테이블 표시

                data.forEach(row => {
                    let tr = document.createElement("tr");
                    tr.innerHTML = `
                        <td>${row.obsNm}</td>
                        <td>2025-${row.rfObsCd}호</td>
                        <td>${row.meaDt}</td>
                        <td>${row.state}</td>
                        <td>${row.forecastState}</td>
                    `;
                    tableBody.appendChild(tr);
                });
            }
        })
        .catch(error => console.error("호우 특보 데이터 로드 실패:", error));
} 
window.onload = function() {
	loadRainfallWarnings();
};


// 특정 관측소의 강우량 데이터 로드
function loadRainfallData(rfObsCd) {
    fetch('/rainfall/recent/' + rfObsCd)
    .then(response => response.json())
    .then(data => {
        console.log("받은 데이터:", data); 

        if (!data || data.length === 0) {
            console.warn("강우량 데이터가 없음.");
            document.getElementById("infoPanel").style.display = "none";
            return;
        }

        document.getElementById("infoPanel").style.display = "block";
        document.getElementById("stationName").innerText = data[0].obsNm || "관측소 정보 없음";
        document.getElementById("stationAddr").innerText = (data[0].addr || "") + " " + (data[0].etcAddr || "");

        var tableHead = document.getElementById("rainfallHeader");
        var tableBody = document.getElementById("rainfallData");

        if (!tableHead || !tableBody) {
            console.error("테이블 요소를 찾을 수 없습니다. 테이블 ID를 확인하세요.");
            return;
        }

        // 테이블 초기화
        tableHead.innerHTML = "<th>시간</th>";
        tableBody.innerHTML = "<th>강우량 (mm)</th>";

        var labels = [];
        var levels = [];

        data.slice(0, 12).forEach(row => {
            labels.unshift(row.meaDt);
            levels.unshift(row.rf);

            var th = document.createElement("td");
            th.textContent = row.meaDt;
            tableHead.appendChild(th);

            var td = document.createElement("td");
            td.textContent = row.rf;
            tableBody.appendChild(td);
        });

        drawChart(labels, levels); 
    })
    .catch(error => {
        console.error("데이터 가져오는 중 오류 발생:", error);
    });
}
window.onload = function() {
	loadRainfallData();
};

//관측소별 강수량
function drawChart(labels, rainfallData) {
    var ctx = document.getElementById("rainChart").getContext("2d");

    if (!labels.length || !rainfallData.length) {
        console.warn("차트 데이터 없음, 기본값 사용");
        labels = ["데이터 없음"];
        rainfallData = [0];
    }

    // 기존 차트 삭제 (Chart.js 인스턴스인지 확인)
    if (window.rainChart instanceof Chart) {
        window.rainChart.destroy();
    }

    window.rainChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: labels,
            datasets: [{
                label: "강우량(mm)",
                data: rainfallData,
                borderColor: "navy",
                borderWidth: 2,
                pointRadius: 3,
                pointBackgroundColor: "navy",
                fill: false
            }]
        },
        options: {
            responsive: true,
            scales: {
                y: { beginAtZero: true 
                }
            }
        }
    });
}

// 지도 로딩
window.onload = function() {
    initMap();
};

</script>

</body>
</html>
