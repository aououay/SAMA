<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<html>
<head>
    <title>급수시설</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <link rel="stylesheet" type="text/css" href="/assets/css/ewsf.css">
    <link rel="stylesheet" type="text/css" href="/assets/css/bootstrap.min.css">
    <link href="/assets/css/header.css" rel="stylesheet">
    <style>

    </style>
</head>

<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>
<div class="container">
<div class="topArea">
<h3>자치구별 양수량</h3>
    <ul>
        <%--선택시 그래프와 표로 변경--%>
        <li><a id="showGraph" href="#">그래프</a></li>
        <li><a id="showTable" href="#">표</a></li>
    </ul>
</div>

<!-- 그래프 영역 -->
<div id="chartContainer">
    <canvas id="pumpChart" width="400" height="140"></canvas>
</div>


<!-- 표 영역 (초기에는 숨김) -->
<table id="pumpTable" class="table table-bordered hidden">
    <thead>
        <tr>
            <th style="text-align: center">자치구</th>
            <th>시설명</th>
            <th>양수량 (톤)</th>
        </tr>
    </thead>
    <tbody>
        <c:forEach var="ewsf" items="${list}">
        <tr>
            <td style="text-align: center">${ewsf.sgg}</td>
            <td>${ewsf.sggFclt}</td>
            <td>${ewsf.pumpTon}</td>
        </tr>
        </c:forEach>
    </tbody>
</table>

</div>


<script>
    //ewsfData 배열 생성 (JSP에서 넘어온 데이터를 JavaScript 배열로 변환)
    var ewsfData = [];

    // list에서 데이터 가져오기
    <c:forEach var="ewsf" items="${list}">
    ewsfData.push({
        sgg: '${ewsf.sgg}',
        sggFclt: '${ewsf.sggFclt}',
        pumpTon: parseInt('${ewsf.pumpTon}')
    });
    </c:forEach>

    // 지역별 펌프 용량을 합산하기 위한 객체 생성
    var regionData = {};

    // ewsfData 배열을 순회하며 같은 지역(sgg)의 펌프 용량을 합산
    ewsfData.forEach(item => {
        if (!regionData[item.sgg]) {
            regionData[item.sgg] = 0; // 해당 지역이 처음 나오면 초기값 0 설정
        }
        regionData[item.sgg] += item.pumpTon; // 해당 지역의 용량을 누적 합산
    });

    // Chart.js에서 사용할 데이터 변환
    var labels = Object.keys(regionData); // 지역 이름 배열 (X축 라벨)
    var data = Object.values(regionData); // 지역별 총 펌프 용량 배열 (Y축 데이터)

    // Chart.js를 이용하여 초기 그래프 생성
    var ctx = document.getElementById('pumpChart').getContext('2d');
    var pumpChart = new Chart(ctx, {
        type: 'bar', // 막대 그래프 선택
        data: {
            labels: labels, // X축 라벨 (지역 이름)
            datasets: [{
                label: '지역별 양수량 (톤)', // 데이터 설명
                data: data, // Y축 데이터 (펌프 용량)
                backgroundColor: 'rgba(54, 162, 235, 0.6)', // 막대 색상
                borderColor: 'rgba(54, 162, 235, 1)', // 테두리 색상
                borderWidth: 1 // 테두리 두께
            }]
        },
        options: {
            responsive: true, // 반응형 설정 (화면 크기에 따라 그래프 조정)
            scales: {
                y: {
                    beginAtZero: true // Y축의 값이 0부터 시작하도록 설정
                }
            }
        }
    });

    // 그래프 버튼 클릭 시 그래프를 다시 그리는 이벤트 리스너
    document.getElementById("showGraph").addEventListener("click", function() {
        document.getElementById("pumpTable").classList.add("hidden"); // 표 숨기기

        // 기존 그래프 삭제 후 다시 생성
        var chartContainer = document.getElementById("chartContainer");
        chartContainer.innerHTML = '<canvas id="pumpChart" width="400" height="140"></canvas>'; // 기존 캔버스 삭제 후 새로 생성

        var newCtx = document.getElementById('pumpChart').getContext('2d');
        pumpChart = new Chart(newCtx, {
            type: 'bar', // 막대 그래프 (bar) 선택
            data: {
                labels: labels, // X축 라벨 (지역 이름)
                datasets: [{
                    label: '지역별 양수량 (톤)', // 데이터 설명
                    data: data, // Y축 데이터 (펌프 용량)
                    backgroundColor: 'rgba(54, 162, 235, 0.6)', // 막대 색상
                    borderColor: 'rgba(54, 162, 235, 1)', // 테두리 색상
                    borderWidth: 1 // 테두리 두께
                }]
            },
            options: {
                responsive: true, // 반응형 설정
                scales: {
                    y: {
                        beginAtZero: true // Y축의 값이 0부터 시작하도록 설정
                    }
                }
            }
        });
    });

    // 표 버튼 클릭 시 그래프를 삭제하고 표를 보이게 하는 이벤트 리스너
    document.getElementById("showTable").addEventListener("click", function() {
        document.getElementById("pumpTable").classList.remove("hidden"); // 표 보이기
        document.getElementById("chartContainer").innerHTML = ""; // 그래프 삭제
    });
</script>


	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>
</body>
</html>