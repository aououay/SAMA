<%@ page contentType="text/html;charset=UTF-8" language="java"%>

<html>
<head>
<title>재난문자 상세</title>
<link href="/assets/css/header.css" rel="stylesheet">
<link rel="stylesheet" type="text/css"
	href="/assets/css/dismes_detail.css">
<link rel="stylesheet" type="text/css"
	href="/assets/css/bootstrap.min.css">
<script src="/assets/js/jquery_3_7_1.js"></script>
</head>


<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

	<h2 class="mt-4">재난문자 상세</h2>

	<div class="container99">

		<%--controller에서 단건검색한 데이터를 disasterMessage에 담아서 불러옴--%>
		<div class="tab">
			<div class="leftbox">지역: ${disasterMessage.disMesArea}</div>
			<div class="rightbox">재난유형: ${disasterMessage.disMesType}</div>
		</div>
		<div class="content">
			<p>${disasterMessage.disMesCon}</p>
		</div>

		<div class="button-container">
			<%--목록 버튼을 클릭하면 전에 있던 페이지로 이동한다--%>
			<button onclick="window.history.back()">목록</button>
		</div>

	</div>

	<div class="container-fluid px-0 " style="margin-top: 350px;">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>

</body>
</html>
