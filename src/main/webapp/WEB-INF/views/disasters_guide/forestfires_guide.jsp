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
            transform : translateY(731px);
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
                <img alt="" src="https://www.junggu.seoul.kr/safety/img/safetyF.jpg">
                <div class="txtbox">
                    <p class="txt_title">산불</p>
                    <p class="txt_info">산불은 바람이 불어가는 쪽으로 번지게 되므로 바람 방향을 감안하여 산불의 진행경로에서 벗어나야 합니다.<br>대피장소는 불이 지나간 타버린 장소, 낮은 장소, 도로, 바위 뒤 등으로 정하고 산불보다 높은 장소를 피하고 불길로부터 멀리 떨어져야 합니다.<br>대피할 시간적인 여유가 없을 때는 낮은 지역을 찾아 낙엽이나 나뭇가지 등을 긁어낸 후 얼굴 등을 가리고 불길이 지나갈 때까지 엎드려 있어야 합니다.</p>
                </div>
            </div>
            <p class="txt_title">산불 예방법</p>
            <ul class="u_st1 mt20 mb30">
                <li>산불을 발견하면 119에 신고합니다.</li>
                <li>초기의 작은 산불을 외투, 나뭇가지 등을 이용해 두드리거나 덮어서 불을 끕니다.</li>
                <li>산불 규모가 커지면 산불 발생 지역에서 멀리 떨어진 안전한 곳으로 불길을 등지고 바람이 불어오는 방향으로 빨리 대피합니다.</li>
                <li>대피할 여유가 없을 때는 낙엽이나 나뭇가지 등이 없는 곳에서 얼굴 등을 가리고 불길이 지나갈 때까지 엎드려 있습니다.</li>
                <li>산행전에 산림청 홈페이지를 통해 통제되지 않은 출입 가능한 등산로를 확인합니다.</li>
                <li>산에는 성냥, 라이터 등 화기물을 가져가지 않고 담배를 피우지 않습니다.</li>
                <li>산에서 취사, 야영을 하지 않습니다. (※ 지정된 야영장과 대피소에서만 가능)<br>※ 산불예방 - 산불조심기간(봄철: 2.1~5.15, 가을철: 11.1~12.15)</li>
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
