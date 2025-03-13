<%--
  Created by IntelliJ IDEA.
  User: acorn4
  Date: 2025-02-28
  Time: PM 4:52
  To change this template use File | Settings | File Templates.
--%>
<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>사회재난 행동요령</title>
    <link rel="stylesheet" type="text/css" href="/assets/css/disasters_guide.css">
    <link rel="stylesheet" type="text/css" href="/assets/css/bootstrap.min.css">
    <link rel="stylesheet" type="text/css" href="/assets/css/header.css">

    <style>
        .container-fluid {
            flex: 0 !important;
        }
            #sidebar99 {
            position: fixed; /* 화면에 고정 */
            top: 0; /* 화면의 상단에 고정 */
            left: 0; /* 화면의 왼쪽에 고정 */
            width: 200px; /* 사이드바의 너비 */
            margin-top: 7%;
            height: 58vh;
            background-color: #f4f4f4; /* 배경색 */
            padding-top: 20px; /* 상단 여백 */
            box-shadow: 2px 0 5px rgba(0, 0, 0, 0.1); /* 사이드바에 그림자 효과 */
            z-index: 999; /* 다른 요소보다 위에 표시 */
        }

        #sidebar99 ul {
            list-style-type: none; /* 기본 불릿 제거 */
            padding-left: 0; /* 기본 패딩 제거 */
        }

        #sidebar99 li {
            margin: 10px 0; /* 각 항목 간의 간격 */
        }

        #sidebar99 a {
            display: block;
            padding: 10px 15px;
            text-decoration: none;
            color: #333;
            font-size: 16px;
        }

        #sidebar99 a:hover {
            background-color: #007bff; /* 마우스 오버 시 배경색 변경 */
            color: white; /* 마우스 오버 시 글자 색 변경 */
        }
        .div23{
            position : relative;
            transform : translateY(701px);
        }
    </style>
</head>



<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

<div id="container" class="message">
    <div id="state_sub">
        <div id="sidebar99">
            <ul>
                <li><a href="/disasterguide/fire">화재</a></li>
                <li><a href="/disasterguide/forestfires">산불</a></li>
                <li><a href="/disasterguide/electricity_gas_accident">전기-가스사고</a></li>
                <li><a href="/disasterguide/car_accident">교통사고</a></li>
            </ul>
        </div>
        <div class="content4">
        <div class="imgbox">
            <img alt="" src="https://www.junggu.seoul.kr/safety/img/safetyG.jpg">
            <div class="txtbox">
                <p class="txt_title">화재</p>
                <p class="txt_info">화재가 발생한 사무실에서 탈출할 때에는 문을 반드시 닫고 나와야 하며 탈출하면서 열린 문이 있으면 모두 닫읍시다.<br> 닫힌 문을 열 때에는 손등으로 문의 온도를 확인하고 뜨거우면 절대로 열지 말고 다른 비상통로를 이용합시다. <br>건물 밖으로 대피하지 못한 경우에는 밖으로 통하는 창문이 있는 방으로 들어가서 구조를 기다립시다.</p>
            </div>
        </div>
        <p class="txt_title">화재 예방법</p>
        <ul class="u_st1 mt20 mb30">
            <li>불을 발견하면 ‘불이야’하고 큰소리로 외쳐 다른 사람에게 알리고 화재경보 비상벨을 누릅시다.</li>
            <li>엘리베이터를 이용하지 말고 계단을 이용하되 아래층으로 대피가 불가능한 때에는 옥상으로 대피합시다.</li>
            <li>불길 속을 통과할 때에는 물에 적신 담요나 수건 등으로 몸과 얼굴을 감싸주세요.</li>
            <li>연기가 많을 때는 한 손으로는 코와 입을 젖은 수건 등으로 막고 낮은 자세로 이동합시다.</li>
            <li>방문을 열기 전에 문손잡이를 만져 보았을 때 뜨겁지 않으면 문을 조심스럽게 열고 밖으로 나갑시다.</li>
            <li>출구가 없으면 연기가 방안에 들어오지 못하도록 물을 적셔 문틈을 옷이나 이불로 막고 구조를 기다립니다.</li>
        </ul>
        <table class="tableA">
            <colgroup>
                <col style="width:50%;">
                <col style="width:50%;">
            </colgroup>
            <thead>
            <th>긴급상황시 신고전화</th>
            <th>유관기관 연락처</th>
            </thead>
            <tbody>
            <tr>
                <td>재난신고 119, 범죄신고 112, 민원상담 110<br>행정안전부 중앙재난안전상황실 044)205-1542~3</td>
                <td>행정안전부 044)205-6366, http://www.mois.go.kr <br>기상청 02)2181-0900, http://www.kma.go.kr</td>
            </tr>
            </tbody>
        </table>
        </div>
    </div>
</div>
 <div class="container-fluid px-0 div23">
      <!-- footer-->
      <jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
      <!--// footer-------------------------------------------------->
    </div>
</body>
</html>
