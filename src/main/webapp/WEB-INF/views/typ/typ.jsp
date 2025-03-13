<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib prefix="fn" uri="http://java.sun.com/jsp/jstl/functions" %>
<html>
<head>
  <title>태풍</title>
  <link rel="stylesheet" type="text/css" href="/assets/css/typ.css">
  <link rel="stylesheet" type="text/css" href="/assets/css/bootstrap.min.css">
  <link rel="stylesheet" type="text/css" href="/assets/css/header.css">
  
</head>

<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>
<div class="container">
<div class="earthquakeList">
  <div class="leftArea" style="margin-top: 45px">
    <div class="selectArea">
      <select class="selectTypeA" style="width:170px;" name="year" id="year" onchange="changeYear()">
        <option value="">선택</option>
        <%--searchWord 값이 비교값과 동일하다면 선택한 옵션을 출력하고 그값과 동일하지 않다면 빈값 출력
         그값으로 js의 changeYear 함수를 사용해 연도별로 분류--%>
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

    <div class="tableArea" style="margin-top: 44px;">
      <!-- 차트가 그려질 캔버스 -->
      <canvas id="typhoonChart" width="400" height="400"></canvas>
    </div>

  </div>

  <div class="rightArea">

    <div class="topArea">
      <ul>
        <%--controller에서 설정한 searchWord를 li 안에 있는 값으로 설정하여 js로 넘기고 다시 controller로 넘긴다--%>
        <li><a id="all" href="#" onclick="changeSelection('10')">전체</a></li>
        <li><a id="landing" href="#" onclick="changeSelection('20')">상륙</a></li>
        <li><a id="directly" href="#" onclick="changeSelection('30')">직접</a></li>
      </ul>
    </div>

    <div class="tableArea">

      <div class="titArea">
        <div class="tableSet">
          <span>태풍명</span>
          <span>시작일시</span>
          <span>종료일시</span>
          <span>한반도 영향</span>
        </div>
      </div>

      <ul class="tableList scr1">
        <%--만약 분류한값의 태풍 데이터가 없다면 표시--%>
        <c:if test="${empty typ}">
          <div class="noneData">데이터가 없습니다</div>
        </c:if>

        <!-- controller에서 typVO타입의 데이터를 typ에 담아서 뿌려줌 -->
        <c:forEach var="typ" items="${typ}">
          <li onclick="displayTyphoonData('${typ.typKorName}', '${typ.typSt}', '${typ.typEd}', ${typ.typMinPressure}, ${typ.typMaxWind})">
            <span>${typ.typKorName}</span>

            <%--태풍의 날짜 형식을 맞춰줌--%>
            <span id="formattedDate1">${typ.typSt}</span>
            <span id="formattedDate2">${typ.typEd}</span>
            <span>${typ.typEff}</span>
          </li>
        </c:forEach>
      </ul>

    </div>
  </div>
</div>
</div>


<script src="/assets/js/typ/typ_page.js"></script>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
<script>

  const ctx = document.getElementById('typhoonChart').getContext('2d');


  let typhoonChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels: [],
      datasets: [
        {
          label: '최소 기압 (kPa)',
          data: [],
          backgroundColor: 'rgba(75, 192, 192, 0.2)',
          borderColor: 'rgba(75, 192, 192, 1)',
          borderWidth: 1
        },
        {
          label: '최대 풍속 (m/s)',
          data: [],
          backgroundColor: 'rgba(255, 99, 132, 0.2)',
          borderColor: 'rgba(255, 99, 132, 1)',
          borderWidth: 1
        },
        {
          label: '태풍 존재일 (일)',
          data: [],
          backgroundColor: 'rgba(153, 102, 255, 0.2)',
          borderColor: 'rgba(153, 102, 255, 1)',
          borderWidth: 1
        }
      ]
    },
    options: {
      responsive: true,
      scales: {
        x: {
          title: {
            display: true,
            text: '태풍 최소 기압 - 태풍 최대 풍속 - 태풍 존재일'
          }
        },
        y: {
          title: {
            display: true,
            text: '단위 (kPa / m/s / 일)'
          },
          beginAtZero: true, // Y축이 0부터 시작하게 설정
          min: 0, // Y축의 최소 값
          max: 120 // Y축의 최대 값
        }
      }
    }
  });

  function displayTyphoonData(name, startDate, endDate, minPressure, maxWind) {

    const days = endDate - startDate;


    const labels = [`${startDate} - ${endDate}`];
    const minPressureData = [minPressure / 10];
    const maxWindData = [maxWind];
    const durationData = [days];


    typhoonChart.data.labels = labels;
    typhoonChart.data.datasets[0].data = minPressureData ;
    typhoonChart.data.datasets[1].data = maxWindData;
    typhoonChart.data.datasets[2].data = durationData;


    typhoonChart.update();
  }
  document.addEventListener("DOMContentLoaded", function() {
    // 모든 "formattedDate" ID를 가진 요소 가져오기
    let dateElements = document.querySelectorAll("#formattedDate1");

    dateElements.forEach(function(element) {
      let dateStr = element.innerText.trim();

      // 8자리 숫자인 경우만 변환
      if (dateStr.length === 8 && !isNaN(dateStr)) {
        let formattedDate = dateStr.substring(0, 4) + "-" +
                dateStr.substring(4, 6) + "-" +
                dateStr.substring(6, 8);
        element.innerText = formattedDate;
      }
    });
  });

  document.addEventListener("DOMContentLoaded", function() {
    // 모든 "formattedDate" ID를 가진 요소 가져오기
    let dateElements = document.querySelectorAll("#formattedDate2");

    dateElements.forEach(function(element) {
      let dateStr = element.innerText.trim();

      // 8자리 숫자인 경우만 변환
      if (dateStr.length === 8 && !isNaN(dateStr)) {
        let formattedDate = dateStr.substring(0, 4) + "-" +
                dateStr.substring(4, 6) + "-" +
                dateStr.substring(6, 8);
        element.innerText = formattedDate;
      }
    });
  });

  document.addEventListener("DOMContentLoaded", function() {
    // 모든 "formattedDateRange" ID를 가진 요소 가져오기
    let dateRangeElements = document.querySelectorAll("#formattedDateRange");

    dateRangeElements.forEach(function(element) {
      let dateStr = element.innerText.trim();

      // "YYYYMMDD - YYYYMMDD" 패턴인지 확인
      let match = dateStr.match(/^(\d{8})\s*-\s*(\d{8})$/);
      if (match) {
        let startDate = match[1]; // 시작 날짜 (YYYYMMDD)
        let endDate = match[2];   // 종료 날짜 (YYYYMMDD)

        // 변환된 날짜 포맷
        let formattedStartDate = startDate.substring(0, 4) + "-" +
                startDate.substring(4, 6) + "-" +
                startDate.substring(6, 8);

        let formattedEndDate = endDate.substring(0, 4) + "-" +
                endDate.substring(4, 6) + "-" +
                endDate.substring(6, 8);

        // 변환된 날짜 적용
        element.innerText = formattedStartDate + " - " + formattedEndDate;
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