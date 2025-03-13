<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<c:set var="CP" value="${pageContext.request.contextPath}" />

<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=1200">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet">
<link href="/assets/css/header.css" rel="stylesheet">
<link href="/assets/css/pm_warning.css" rel="stylesheet">
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<title>Insert title here</title>
<script src="/assets/js/jquery_3_7_1.js"></script>
</head>
<link rel="icon" href="data:;base64,iVBORw0KGgo=">

<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>


	<!-- main-->
	<main>


		<div class="">
			<h1 class="mt-3 mb-5 text-center">서울 미세먼지 경보 발령 내역</h1>
		</div>
		<div class="container-fluid d-flex justify-content-center">
			<table class="table table-bordered border-dark text-center w-50">
				<thead>
					<tr class="text-center table-success">
						<th class="pb-2" scope="col">번호</th>
						<th class="pb-2" scope="col">지역</th>
						<th class="pb-2" scope="col">권역</th>
						<th class="pb-2" scope="col">항목</th>
						<th class="pb-2" scope="col">경보단계</th>
						<th class="pb-2" scope="col">발령농도</th>
						<th class="pb-2" scope="col">발령시간</th>
						<th class="pb-2" scope="col">해제시간</th>
					</tr>
				</thead>
				<tbody>
					<c:set var="count" value="0"></c:set>
					<c:forEach var="pm_Warning" items="${pm_Warning_List}">
						<tr>
							<th class="table-success" scope="row">${count+1}</th>
							<td>${pm_Warning.dist_Nm}</td>
							<td>${pm_Warning.move_Nm}</td>
							<c:choose>
								<c:when test='${pm_Warning.issue_Item.equals("PM25")}'>
									<td>PM2.5</td>
								</c:when>
								<c:otherwise>
									<td>${pm_Warning.issue_Item}</td>
								</c:otherwise>
							</c:choose>
							<td>${pm_Warning.issue_Gbn}</td>
							<td>${pm_Warning.issue_Conc}</td>
							<td>${pm_Warning.issue_Dt}</td>
							<td>${pm_Warning.clear_Dt}</td>
						</tr>
						<c:set var="count" value="${count + 1}" />
					</c:forEach>

				</tbody>
			</table>
		</div>

	</main>


	<!--// main---------------------------------------------------->





	<div class="container">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>


	<script>
		// 오늘 날짜 자동 업데이트

		let day = new Date().toLocaleDateString('ko-KR');

		let dayOfWeek = new Date(day).getDay();
		let dayList = [ '일', '월', '화', '수', '목', '금', '토' ];
		let label = dayList[dayOfWeek];

		document.getElementById("date").innerText = day + "(" + label + ")";
	</script>

</body>
</html>