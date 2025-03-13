<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<html>
<head>
    <title>자연재난 행동요령</title>
    <link rel="stylesheet" type="text/css" href="/assets/css/disasters_guide.css">
    <link rel="stylesheet" type="text/css" href="/assets/css/bootstrap.min.css">
    <link rel="stylesheet" type="text/css" href="/assets/css/header.css">

    <style>
        .container-fluid{
            flex: 0!important;
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

        /* 이미지 감싸는 div 스타일 */
        .image-gallery {
            display: flex;
            justify-content: center; /* 수평 중앙 정렬 */
            align-items: center; /* 수직 중앙 정렬 */
            gap: 20px; /* 이미지 간의 간격 */
            flex-wrap: wrap; /* 화면 크기에 맞게 자동 줄바꿈 */
            margin-top: 50px;
            padding: 20px;
        }

        .card-img {
            width: 200px; /* 이미지 크기 */
            height: auto; /* 비율에 맞게 높이 자동 조정 */
            margin: auto;
            border-radius: 8px; /* 모서리 둥글게 */
        }
        .div23{
            position : relative;
            transform : translateY(4571px);
        }
    </style>
</head>

<body>

<div class="">
    <!-- header-->
    <jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
    <!--// header-------------------------------------------------->
</div>

<div>
    <!-- 여기에 본문 내용이 들어갑니다 -->
    <div id="sidebar99">
        <ul>
            <li><a href="/disasterguide/finedust">미세먼지</a></li>
            <li><a href="/disasterguide/typhoon">태풍</a></li>
            <li><a href="/disasterguide/heatwave">폭염</a></li>
            <li><a href="/disasterguide/heavyrain">호우</a></li>
            <li><a href="/disasterguide/flood">홍수</a></li>
            <li><a href="/disasterguide/coldwave">한파</a></li>
            <li><a href="/disasterguide/snowstorm">대설</a></li>
        </ul>
    </div>
    <!-- 컨텐츠 영역 -->
    <div class="content4">
        <div class="image-gallery">
            <img class="card-img" src="https://www.junggu.seoul.kr/safety/img/card/card4_1.jpg" alt="Image 1">
            <img class="card-img" src="https://www.junggu.seoul.kr/safety/img/card/card4_2.jpg" alt="Image 2">
            <img class="card-img" src="https://www.junggu.seoul.kr/safety/img/card/card4_3.jpg" alt="Image 3">
            <img class="card-img" src="https://www.junggu.seoul.kr/safety/img/card/card4_4.jpg" alt="Image 4">
            <img class="card-img" src="https://www.junggu.seoul.kr/safety/img/card/card4_5.jpg" alt="Image 5">
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
