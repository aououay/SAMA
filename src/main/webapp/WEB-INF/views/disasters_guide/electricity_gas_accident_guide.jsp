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
            transform : translateY(2061px);
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
                <img alt="" src="https://www.junggu.seoul.kr/safety/img/safetyE.jpg">
                <div class="txtbox">
                    <p class="txt_title">전기·가스사고</p>
                    <p class="txt_info">많은 인명, 재산피해를 남기는 전기ㆍ가스 사고의 원인은 대부분 시설 안전점검 미흡으로 인한 사고이거나 가스 취급 부주의로 인한 화재 또는 폭발사고입니다.<br>전기ㆍ가스 사고는 조금만 더 주의를 기울인다면 충분히 예방할 수 있습니다.<br>전기ㆍ가스 사고의 예방법과 대처방법에 대해서 알려드립니다.</p>
                </div>
            </div>
            <p class="txt_title">전기사고 예방법</p>
            <p class="ul_title">합선 예방</p>
            <ul class="u_st1 mt10 mb10">
                <li>퓨즈나 과전류 차단기는 반드시 정격용량의 것을 사용합시다.</li>
                <li>용량에 적합한 규격 전선을 사용하고, 노후되었거나 손상된 전선은 새 전선으로 교체합시다.</li>
                <li>스위치, 배전반 등의 내부를 정기적으로 점검하여 전기가 통할 수 있는 물질이나 가연성 물질 등을 제거합시다.</li>
                <li>천장 등 보이지 않는 장소에 시설된 전선에 대해서도 수시로 점검하여 이상 유무를 확인합시다.</li>
                <li>배선은 가능한 보호관을 사용하고 열이나 외부 충격 등에 노출되지 않도록 합시다.</li>
                <li>못이나 스테이플러로 전선을 고정하지 않도록 합시다.</li>
                <li>바닥이나 문틀을 통과하는 전선이 손상되지 않도록 배관 등으로 보호합시다.</li>
            </ul>

            <p class="ul_title">누전 예방</p>
            <ul class="u_st1 mt10 mb10">
                <li>건물이나 대용량 전기기구에는 배선을 분류하여 배선별로 누전차단기를 설치 합시다.</li>
                <li>배선의 피복손상 여부를 수시로 확인합시다.</li>
                <li>전선이 금속체나 젖은 구조물에 직접 접촉되지 않도록 배선해야 합니다.</li>
            </ul>
            <p class="ul_title">전기 스파크 예방</p>
            <ul class="u_st1 mt10 mb10">
                <li>각종 전기기기는 사용 후에 반드시 플러그를 뽑아 둡시다.</li>
                <li>정전이 되면 플러그를 뽑거나 스위치를 꺼둡시다.</li>
                <li>배전반 내의 먼지, 금속가루 등 분진을 제거합시다.</li>
                <li>밀가루, 톱밥, 섬유 먼지 등 가연성 분진이 많이 발생하는 장소에서는 수시로 청소를 실시하여 분진이 쌓이지 않도록 합시다.</li>
                <li>배전반 등 전기시설 부근에는 가구, 위험물, 기타 가연물은 두지 맙시다.</li>
            </ul>

            <p class="ul_title">과부하 예방</p>
            <ul class="u_st1 mt10 mb30">
                <li>한 콘센트에 여러 개의 플러그를 꽂아 사용하는 문어발식 사용을 금합시다.</li>
                <li>전기기기의 전기용량 및 전압에 적합한 규격전선을 사용합시다.</li>
            </ul>

            <p class="txt_title">가스사고 예방법</p>
            <p class="ul_title">가스 사용전</p>
            <ul class="u_st1 mt10 mb10">
                <li>가스가 누출되지는 않았는지 냄새로 우선 확인합시다.</li>
                <li>LPG는 바닥으로부터, 도시가스(LNG)는 천정으로부터 냄새를 맡아야 합니다.</li>
                <li>불쾌한 냄새가 나면 가스가 새고 있는 것입니다.</li>
                <li>가스기구를 사용할 때에는 창문을 열어 신선한 공기로 충분히 실내를 환기시켜야 합니다.</li>
                <li>가스레인지 주위에는 가연성 물질(빨래, 스프레이 통 등)을 가까이 두지 맙시다.</li>
            </ul>

            <p class="ul_title">가스 사용중</p>
            <ul class="u_st1 mt10 mb10">
                <li>가스불을 켤 때에는 불이 붙었는지 꼭 확인합시다(불이 붙지 않은 상태로 점화코크가 열리면 가스가 누출될 수 있습니다).</li>
                <li>파란 불꽃이 되도록 공기 조절기를 조절합시다. 불완전연소 시 유독성가스와 일산화탄소가 나오고 연료 소비량도 많아집니다.</li>
                <li>국물이 넘치거나 바람으로 인해 불이 꺼지지 않았는지 옆에서 지켜보도록 하고, 가능한 자리를 떠나지 맙시다.</li>
                <li>불이 꺼지면 자동으로 가스가 차단되는 제품을 사용하고 자동차단 장치가 제대로 작동하는지 자주 확인합시다.</li>
            </ul>

            <p class="ul_title">가스 사용후</p>
            <ul class="u_st1 mt10 mb10">
                <li>가스사용 후에는 연소기 코크와 중간밸브를 꼭 잠가야합니다.</li>
                <li>장기간 외출 시에는 용기밸브도 잠그는 것이 안전합니다.</li>
                <li>도시가스의 경우는 메인밸브를 잠가야 하며 이사를 갈 때에는 도시가스관리사무소에 연락하여 필요한 조치를 취합시다.</li>
                <li>가스레인지는 자주 이동하지 말고 한 곳에 고정하여 사용합시다.</li>
            </ul>

            <p class="ul_title">휴대용 부탄 가스레인지 사용시</p>
            <ul class="u_st1 mt10 mb30">
                <li>휴대용 가스레인지는 야외에서 사용하도록 만들어 졌습니다. 부득이 실내에서 사용할 경우에는 밖에서 사용 시험을 한 후 이상이 없을 때 사용하도록 합시다.</li>
                <li>다 쓴 용기(캔)는 잔류가스를 제거하고 구멍을 내어 화기가 없는 장소에 버려야 합니다.</li>
                <li>용기(캔)를 접속할 때는 완전히 결합이 되게 하고, 용기 상단의 절개된 홈 부분이 위로 향하도록 접속해야 합니다.</li>
                <li>사용 중에 가스가 누출될 경우 신속히 연결레버를 위로 올려 용기(캔)를 분리시켜야 합니다.</li>
                <li>밀폐된 텐트 안이나 좁은 방에서는 질식 또는 화재의 위험이 있으므로 사용하지 맙시다.</li>
                <li>사용하는 그릇의 바닥이 삼발이보다 넓으면 화기가 가스용기(캔)를 가열하게 되어 폭발의 원인이 되므로 삼발이보다 큰 그릇을 사용하지 않도록 해야 합니다.</li>
                <li>실내 사용 시 반드시 환기를 해야합니다.</li>
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
