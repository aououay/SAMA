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
            transform : translateY(1571px);
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
                <img alt="" src="https://www.junggu.seoul.kr/safety/img/safetyH.jpg">
                <div class="txtbox">
                    <p class="txt_title">교통사고</p>
                    <p class="txt_info">교통사고는 우리 생활속에서 가장 빈번히 발생하는 사고입니다.<br>따라서 사고 발생시 신속하게 대처해야 사망 또는 중상의 위험으로부터 벗어날 수 있습니다.<br>각 교통사고 상황별로 행동요령에 대해 알려드립니다.</p>
                </div>
            </div>
            <p class="txt_title">교통사고 예방법</p>
            <p class="ul_title">교통사고 발생시</p>
            <ul class="u_st1 mt10 mb10">
                <li>위험물질 수송차량 사고시 사고지점에서 빠져 나와 대피합시다.</li>
                <li>화재가 발생한 경우 외에는 부상자를 건드리지 맙시다.</li>
                <li>구조대의 활동이 본격적으로 시작되면 구조에 참여하지 말고 사고 현장에서 물러나야 합니다.</li>
                <li>사고현장에서는 유류나 가스가 누출되어 화재가 발생할 위험성이 있으니 담배를 피우지 맙시다.</li>
                <li>배선은 가능한 보호관을 사용하고 열이나 외부 충격 등에 노출되지 않도록 합시다.</li>
            </ul>

            <p class="ul_title">지하철 화재 발생시</p>
            <ul class="u_st1 mt10 mb10">
                <li>노약자, 장애인석 옆에 있는 비상버튼을 눌러 승무원과 연락합시다.</li>
                <li>객차마다 2개씩 비치된 소화기를 이용하여 불을 꺼야 합니다.</li>
                <li>출입문이 열리지 않으면 비상용 망치를 이용하여 유리창을 깨야 합니다.<br> 망치가 없으면 소화기를 이용하여 유리창을 깨야 합니다. 망치가 없으면 소화기를 이용하여 유리창을 깨야 합니다.</li>
                <li>코와 입을 손수건이나 티슈로 막고 비상구로 신속히 대피하되, 뛰면 위험하므로 걸어서 대피합시다.</li>
                <li>정전 시에는 유도등을 따라 출구로 나가고, 유도등이 보이지 않을 때는 시각장애인용 보도블록을 따라 가거나 벽을 짚으면서 대피합시다.</li>
                <li>지상으로 대피가 여의치 않을 때에는 대피요원의 안내에 따라 철로를 이용하여 대피합시다.</li>
                <li>가능하다면 소화전을 이용 불을 꺼야합니다.</li>
            </ul>

            <p class="ul_title">자동차 침수 사고시</p>
            <ul class="u_st1 mt10 mb30">
                <li>안전벨트를 푼 다음 신발과 옷을 벗어 수영이 가능하도록 합시다.</li>
                <li>물에 뜨는 물건이 주위에 있으면 움켜쥐고 출입문을 통해 빠져나오거나, 망치를 이용해 유리창을 깨고 탈출해야 합니다.</li>
                <li>바로 탈출하지 못한 경우에는 차내에 물이 어느 정도 들어와 수압 차이가 없어져 출입문을 열수 있을 때까지 침착하게 기다렸다가 탈출합시다.</li>
                <li>차에서 나오기 전에 3~4회 심호흡을 하고 숨을 크게 들이 쉰 다음 숨을 멈추고 나오면 물속에서 더 오래 견딜 수 있습니다.</li>
            </ul>

            <p class="txt_title">자동차 사고 예방법</p>
            <p class="ul_title">도로 횡단시</p>
            <ul class="u_st1 mt10 mb30">
                <li>우선 멈추어 좌우를 살피는 습관을 가져야 합니다.</li>
                <li>횡단보도가 아닌 곳을 무단으로 횡단하는 일은 금해야 합니다.</li>
                <li>횡단보도에서 신호가 바뀌더라도 차량의 진행유무를 반드시 확인합시다.</li>
                <li>어린이나 노약자는 보호자와 함께 건너야 합니다.</li>
                <li>처음부터 건너기 시작한 사람이 아니면, 파란신호가 끝나지 않았더라도 건너지 않아야 합니다.</li>
                <li>신호등이 없거나 점멸신호가 있는 횡단보도에서는 차량이 완전히 멈추었거나 운전자의 수신호가 있을 때 건너도록 합시다.</li>
                <li>운전자는 사람이 내리고 있는 차량 옆으로 지나가거나 추월하지 않도록 해야 합니다.</li>
                <li>차에서 내리는 사람은 지나가는 자전거, 오토바이, 차량에 주의해야 합니다.</li>
                <li>사람은 인도로 다니고 차는 차도로 다녀야 합니다.</li>
                <li>인도와 차도가 구분이 안 된 도로의 경우에는 '길 가장자리' 즉 한쪽으로 다니도록 합시다.</li>
                <li>운동은 운동장이나 놀이터와 같은 안전한 장소에서 해야 합니다.</li>
                <li>비가 오는 날에 우산을 숙여 쓰면 앞을 살필 수 없으니 똑바로 쓰고 차도에서 떨어진 길의 가장자리로 걷도록 합시다.</li>
                <li>운전자의 눈에 잘 띄게 하기 위해 밝은 색 옷을 입어야 합니다.</li>
                <li>좁은 길이나 골목길에서 넓은 도로로 나올 때에는 아무리 급한 일이 있더라도 일단 멈추어 서서 좌우를 잘 확인하고 나서야 합니다.</li>
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
