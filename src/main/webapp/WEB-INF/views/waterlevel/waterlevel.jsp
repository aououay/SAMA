<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<%@ page import="java.util.Calendar" %>
<%
    int currentYear = Calendar.getInstance().get(Calendar.YEAR);
%>

<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="/assets/css/bootstrap.min.css" rel="stylesheet" >
	<link href="/assets/css/header.css" rel="stylesheet" >
	<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
    
    <title>홍수 정보</title>
    <script src="/assets/js/jquery_3_7_1.js"></script>
    <script type="text/javascript" 
    		src="//dapi.kakao.com/v2/maps/sdk.js?appkey=feb2e034645bde27f5e8d4d1e4bfc50c&libraries=services"></script>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        #wl-container {
            display: flex;
        }
        #map {
            width: 50%;
            height: 650px;
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
		    width: 130px;
		    height: auto;
		}
        
        #infoPanel{
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
		
        table {
            width: 100%;
            border-collapse: collapse;
        }
        th {
            border: 1px solid #ddd;
            padding: 10px;
            text-align: center;
            background-color: #f4f4f4;
        }
        td {
            border: 1px solid #ddd;
            padding: 10px;
            text-align: center;
        }
        
		.table-container {
		    width: 100%;
		    overflow-x: auto; /* 가로 스크롤 허용 */
		    white-space: nowrap; /* 텍스트 줄 바꿈 방지 */
		}
		
		.table-scroll {
		    display: block;
		    width: max-content; /* 내용 길이에 맞게 테이블 확장 */
		}
		
		#waterLevelChart {
			margin-top: 50px;
		}
		.displayArea {
		    text-align: center;
		    height: 40px;
		    margin-bottom: 25px;
		    border: 1px solid #d2d2d2;
		    box-sizing: border-box;
		    margin-left: 0px auto;
		    padding-inline-start: 0px;
		 }
		 
		 ul, li {
		 	list-style: none;
		 	
		 }
		 
		 .displayArea li {
		    float: left;
		    position: relative;
		    width: 50%;
		    height: 38px;
		    line-height: 38px;
		    font-size: 15px;
		    font-weight: 200;
   		 }
   		 .displayArea li:after {
		    content: "";
		    position: absolute;
		    right: 0;
		    top: 50%;
		    width: 1px;
		    height: 16px;
		    transform: translateY(-50%);
		    background: #ddd;
		}
		.strong-style {
			font-weight:bold;
			margin-right: 5px;
			padding: 10px;
		}
		.pagination-container {
		    display: flex;
		    justify-content: center; /* 가로 중앙 정렬 */
		    margin-top: 20px;
		}
		
		.pagination {
		    display: flex;
		    justify-content: center;
		    align-items: center;
		    margin-top: 20px;
		    gap: 8px; /* 버튼 간격 */
		}
		
		.pagination a {
		    display: flex;
		    justify-content: center;
		    align-items: center;
		    width: 40px;
		    height: 40px;
		    border: 1px solid #ccc;
		    border-radius: 8px;
		    text-decoration: none;
		    font-size: 16px;
		    color: #555;
		    font-weight: bold;
		    transition: background-color 0.3s, color 0.3s;
		}
		
		.pagination .arrow {
		    font-size: 18px;
		    font-weight: bold;
		    background-color: white;
		}
		
		.pagination a:hover {
		    background-color: #f0f0f0;
		}
		
		.pagination .active {
		    color: #0d6efd;
		    border-bottom: 3px solid #0d6efd;
		    pointer-events: none;
		}
		#latestWaterLevels{
		    width: 1200px;
	    	margin:0 auto;
		}
		
		#latestWaterLevels h2 {
			margin-top: 50px;
		}
		
		.sourceText {
		    font-size: 14px;
		    color: darkgray ;
		    margin-left: 10px;
    	}
		
		#lastUpdated{
			margin-top: 40px; 
		    width: 100%; 
		    text-align: center;
		}
		
		#lastUpdated table {
		    width: 80%;
		    margin: 0 auto;
		}			
		
		#waterlevelImage {
   		 display: flex;
   		 justify-content: center; /* 수평 중앙 정렬 */
   		 align-items: center; /* 수직 중앙 정렬 (필요할 경우) */
		}
		
		#waterlevelImage img{
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
		
		#waterlevelImage {
		    position: relative;
		    width: 100%;
		    overflow: hidden;
		}

		#waterlevelImage::before, 
		#waterlevelImage::after {
		    content: "";
		    position: absolute;
		    top: 0;
		    height: 100%;
		    width: 50%;
		    z-index: -1;
		}
		
		#waterlevelImage::before {
		    left: 0;
		    background-color: #F1F1F1; /* 왼쪽 배경색 (네이비톤) */
		}

		#waterlevelImage::after {
		    right: 0;
		    background-color: #E3E1DE; /* 오른쪽 배경색 (밝은 네이비톤 또는 원하는 컬러) */
		}

		#waterlevelImage {
		    display: flex;
		    justify-content: center;
		    align-items: center;
		    position: relative;
		    overflow: hidden;
		}

		#waterlevelImage img {
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

<div id="container" class="container">
	<div id="wl-container" class="container">
		<div id="map">
			<div id="legend">
		    	<img src="/assets/images/eun_bi/legend.png" alt="범례">
			</div>
		</div>
	    <div id="infoPanel"> 
	    	<h2>홍수 발생현황</h2>
		    <c:choose>
			     <c:when test="${empty waterLevels}">
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
						        <tbody>				        
						            <c:forEach var="water" items="${waterLevels}" varStatus="status">
						                <tr>
						                    <td>${water.obsNm}</td>
						                    <td>2025-${water.wlObsCd}호</td>
						                    <td>${water.meaDt}</td>
						                    <td>${water.state}</td>
						                    <td>${water.forecastState}</td>
						                </tr>
						            </c:forEach>			 
						        </tbody>
						    </table>
					</div>
				</c:otherwise>
			</c:choose>
	
<%-- 	 		<div class="pagination-container">
			    <c:if test="${totalPages > 1}">
				    <div class="pagination">
				        <!-- 이전 버튼 -->
				        <c:if test="${showPrev}">
				            <a href="?page=${currentPage - 1}" class="arrow">&lt;</a>
				        </c:if>
				
				        <!-- 페이지 번호 -->
				        <c:forEach var="i" begin="${startPage}" end="${endPage}">
				            <c:choose>
				                <c:when test="${i == currentPage}">
				                    <strong class="active">${i}</strong>
				                </c:when>
				                <c:otherwise>
				                    <a href="?page=${i}" class="page">${i}</a>
				                </c:otherwise>
				            </c:choose>
				        </c:forEach>
				
				        <!-- 다음 버튼 -->
				        <c:if test="${showNext}">
				            <a href="?page=${currentPage + 1}" class="arrow">&gt;</a>
				        </c:if>
				    </div>
				</c:if>
			</div>  --%>
	
		     <div id="stationInfo" style="display: none;">
	         	<h2 id="obsName">관측소 정보</h2>
	         	<p style="color:gray;">주소: <span id="obsAddr"></span></p>
		        <ul class="displayArea">
		         	<li>
		         		<strong class="strong-style" style="color:orange;">주의보 수위(m)  </strong>   
		         		<span id="wrnWl"></span> 
		         	</li>
		         	<li>
		         		<strong class="strong-style" style="color:red;">경보 수위(m)  </strong>   
		         		<span id="almWl"></span> 
		         	</li>           
		        </ul>
		      	<div class="table-container">           	
			  		<div class="table-scroll">    		
			          	 <table class="water-level-table">               
			             	<thead >                
			                	<tr>
			                    	<th>시간</th>
			                    	<c:forEach var="wl" items="${waterLevels}">
			                        	<td>${wl.meaDt}</td>
			                    	</c:forEach>
				           		</tr>
				        	</thead>
			              	<tbody id="waterLevelTable">         
			              		<tr>
			                       <th>현재 수위 (m)</th>
			                    	<c:forEach var="wl" items="${waterLevels}">
			                        	<td>${wl.wl}</td>
			                    	</c:forEach>
			                   </tr>                      
			           		</tbody>            	
			           	</table>            
					</div>               
				</div>
				
	        <canvas id="waterLevelChart"></canvas>
	        </div>
	
	    </div>
	     
	</div>
	
	<div class = "sourceText">
		<span>한강 홍수통제소 제공</span>
	</div>
	
	<div id="latestWaterLevels" style="margin-bottom: 50px;">
	   <h2>서울 권역 한강 관측소별 수위정보
		   <c:if test="${not empty latestWaterLevels}">
		       <span style="color: gray; font-size: 16px; margin-left: 20px; padding-left: 15px;">
		            <c:out value="${latestWaterLevels[0].regDt}" /> 기준
		       </span>
		   </c:if>
	   </h2>
	   <p id="lastUpdated"></p>
	   <table border="1">
	       <thead>
	           <tr>
	               <th>관측소</th>
	               <th>주의보 수위(m)</th>
	               <th>경보 수위(m)</th>
	               <th>현재수위(m)</th>
	           </tr>
	       </thead>
	       <tbody>				        
	           <c:forEach var="water" items="${latestWaterLevels}">
	            <tr>
	                <td>${water.obsNm}</td>
	                <td>${water.wrnWl}</td>
	                <td>${water.almWl}</td>
	                <td>${water.wl}</td>             
	            </tr>
	        </c:forEach>			 
	        </tbody>
	    </table>
	</div>
	
</div>

<div id="waterlevelImage">
	<img src="/assets/images/eun_bi/waterlevelBanner.png" alt="홍수발생시행동요령">
</div>
	
<div id="container" class="textbox container">
	<div class="textbox">
	<h3>홍수주의보  ·  홍수경보 기준</h3>
		<div class = "line">
		          <span>· 홍수주의보 : 홍수특보를 발령하는 지점의 수위가 계속 상승하여 계획홍수량의 50%가 흐를 때의 수위를 초과할 것이 예상되는 경우. 제방·수문·교량 등에 대한 경계가 필요한 수위</span></br>
		          <span>· 홍수경보 : 홍수특보를 발령하는 지점의 수위가 계속 상승하여 계획홍수량의 70%가 흐를 때의 수위를 초과할 것이 예상되는 경우. 제방·수문·교량 등의 붕괴의 위험이 예상되는 수위</span></br>
		          <span>· 홍수주의보 · 경보의 기준이 되는 수위는 각 지점별로 별도 지정</span>		
		</div>
	</div>
</div>

	    

	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>
 
	
    <script>
   	// 서울 권역 한강 관측소별 수위
    function loadLatestWaterLevels() {
        fetch('/waterlevel/latest')
            .then(response => response.json())
            .then(data => {
                let tableBody = document.getElementById("latestWaterLevels");
                let lastUpdated = document.getElementById("lastUpdated");

                tableBody.innerHTML = "";
                lastUpdated.innerText = new Date().toLocaleString() + " 기준";

                data.forEach(row => {
                    let tr = document.createElement("tr");
                    tr.innerHTML = `
                        <td>${row.obsNm}</td>
                        <td>${row.wrnWl}</td>
                        <td>${row.almWl}</td>
                        <td>${row.wl}</td>
                    `;
                    tableBody.appendChild(tr);
                });
            })
            .catch(error => console.error("수위 데이터 로드 실패:", error));
    }

    // 페이지 로딩 시 자동 실행
    window.onload = function() {
        loadLatestWaterLevels();
    };
	    
    function loadKakaoMap() {
        if (window.kakao && kakao.maps) {
            initMap();  
        } else {
            setTimeout(loadKakaoMap, 500);  // API 로드될 때까지 재시도
        }
    }
       window.onload = function() {      
           initMap();

       };
	//카카오맵
	function initMap() {

        var map = new kakao.maps.Map(document.getElementById('map'), {
            center: new kakao.maps.LatLng(37.5665, 126.9780),
            level: 9
        });
  
        var markerIcons = {
        		normal: "/assets/images/eun_bi/dropBlueBold.png", // 기본 마커
        		warning: "/assets/images/eun_bi/dropYellowBold.png", // 주의보
        		alert: "/assets/images/eun_bi/dropRedBold.png" // 경보
        };
		
        var locations = [
        	<c:forEach var="wl" items="${allWaterLevels}">
            <c:set var="markerState" value="normal"/>
            <c:choose>
                <c:when test="${wl.state eq '주의보'}">
                    <c:set var="markerState" value="warning"/>
                </c:when>
                <c:when test="${wl.state eq '경보'}">
                    <c:set var="markerState" value="alert"/>
                </c:when>
            </c:choose>
	            {name: '<c:out value="${wl.obsNm}" />', lat: ${wl.lat}, lon: ${wl.lon}, id: ${wl.wlObsCd}, level: '<c:out value="${markerState}" />'},
	        </c:forEach>
        ];
        //console.log("마커 데이터 확인:", locations);

        locations.forEach(function(loc) {	  
        	//console.log("마커 상태 확인:", loc.level);
        	//console.log("마커 상태 확인:", loc.name, loc.level);
        	var imageSrc = markerIcons[loc.level] || markerIcons.normal; // 기본값 normal
            
            var markerImage = new kakao.maps.MarkerImage(imageSrc, new kakao.maps.Size(15, 22));
        	
            var marker = new kakao.maps.Marker({
                position: new kakao.maps.LatLng(loc.lat, loc.lon),
                map: map,
                image: markerImage
            });
            
            var infoWindow = new kakao.maps.InfoWindow({
                content: '<div style="padding: 5px; align-items: center; text-align: center; font-size: 12px; background-color: white;  border-radius: 5px;">' + 
                		(loc.name ? loc.name : "이름 없음") + '</div>'
            });

            // 마커에 마우스를 올리면 (`mouseover`) 이름 표시
            kakao.maps.event.addListener(marker, 'mouseover', function() {
            	//console.log("마커에 마우스를 올림:", loc.name);
                infoWindow.open(map, marker);
            });

            // 마커에서 마우스를 떼면 (`mouseout`) 이름 숨김
            kakao.maps.event.addListener(marker, 'mouseout', function() {
                infoWindow.close();
            });

            kakao.maps.event.addListener(marker, 'click', function() {
                //console.log("마커 클릭됨:", loc.name);

                var floodTable = document.getElementById("floodWarningTable");
                var pagination = document.querySelector(".pagination-container");
                var stationInfo = document.getElementById("stationInfo");
                var floodTitle = document.querySelector("h2"); // 홍수 발생현황 제목
                var noWarning = document.querySelector(".non-wl"); 

                if (floodTable) floodTable.style.display = "none"; // 홍수 발생 현황 숨김
                if (pagination) pagination.style.display = "none"; // 페이징 숨김
                if (floodTitle) floodTitle.style.display = "none"; // 제목 숨김
                if (noWarning) noWarning.style.display = "none";
                if (stationInfo) stationInfo.style.display = "block"; // 관측소 정보 표시

                loadWaterLevel(loc.id); // 데이터 로드
            });
       	 
            
        });
    }
	//관측소별 수위 현황
    function loadWaterLevel(wlObsCd) {
    	fetch('/waterlevel/recent/' + wlObsCd)
        .then(response => response.json())
        .then(data => {
            //console.log(data); // JSON 데이터 확인

            document.getElementById("obsName").innerText = data[0].obsNm;
            document.getElementById("obsAddr").innerText = data[0].addr+" "+data[0].etcAddr;
            document.getElementById("wrnWl").innerText = data[0].wrnWl;
            document.getElementById("almWl").innerText = data[0].almWl;

            var tableHead = document.querySelector(".water-level-table thead tr");
            var tableBody = document.querySelector(".water-level-table tbody tr");
            
            tableHead.innerHTML = "<th>시간</th>";
            tableBody.innerHTML = "<th>현재 수위 (m)</th>";
                    
            var labels = [];
            var levels = [];

            data.slice(0, 12).forEach(row => {
                labels.unshift(row.meaDt);
                levels.unshift(row.wl);

             	// 테이블 헤더에 시간 추가
                var th = document.createElement("td");
                th.textContent = row.meaDt;
                tableHead.appendChild(th);

                // 테이블 바디에 현재 수위 추가
                var td = document.createElement("td");
                td.textContent = row.wl;
                tableBody.appendChild(td);
            });

            drawChart(
            		labels, levels, data[0].wrnWl, data[0].almWl
			);
        });
    }
    
    let isDataLoaded = false;

    function fetchRainfallData(type) {
        if (isDataLoaded) return; // 이미 데이터가 로드되었다면 중복 요청 방지
        isDataLoaded = true;
        
        fetch("/rainfall/gurainfall")
        .then(response => response.json())
        .then(data => {
            console.log("받은 데이터:", data);
            // 데이터 로드가 정상적으로 완료된 경우에만 isDataLoaded를 다시 false로 변경
            setTimeout(() => { isDataLoaded = false; }, 5000); // 5초 후 다시 요청 가능하게 변경
        })
        .catch(error => {
            console.error("데이터 불러오기 오류:", error);
            isDataLoaded = false; // 오류 발생 시 즉시 다시 요청 가능
        });
    } 


	
    //관측소별 수위 그래프
    function drawChart(labels, levels, wrnWl, almWl) {
        var ctx = document.getElementById("waterLevelChart").getContext("2d");
        
        if (window.waterChart) {
            window.waterChart.destroy();
        }

        console.log("Chart Labels:", labels);
        console.log("Chart Data:", levels);
      
        window.waterChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: labels,
                datasets: [
                    { label: "현재 수위(m)", data: levels, borderColor: "green", fill: false },
                    { label: "주의보 수위(m)", data: Array(labels.length).fill(wrnWl), borderColor: "orange", borderDash: [5, 5] },
                    { label: "경보 수위(m)", data: Array(labels.length).fill(almWl), borderColor: "red", borderDash: [5, 5] }
                ]
            },
            options: { 
            	responsive: true, 
            	scales: { 
            		x: {
                        ticks: {
                            maxTicksLimit: 15  // 최대 15개까지 X축 눈금 표시
                        }
                    },
             	y: { beginAtZero: true 
             		
             		} 
             	} 
            }
        });
	} 

    </script>
</body>
</html>

