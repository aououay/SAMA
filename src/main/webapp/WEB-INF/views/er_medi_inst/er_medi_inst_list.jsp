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
<link href="/assets/css/er_medi_inst_list.css" rel="stylesheet">
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


		<div class="container pb-0">
			<h1 class="mt-3 mb-3 text-start fs-3 ps-3">서울특별시 응급의료기관 정보</h1>
			<div class="row">
				<div class="col-3 d-flex justify-content-center">
					<div class="flex-grow-1 text-center mt-1">
						<p class="fs-5">서울특별시</p>
					</div>
					<div class="flex-grow-1">
						<select class="form-select" id="gu">
							<option selected disabled hidden value="">${gu_Nm}</option>
							<option value="">전체</option>
							<option value="111123">종로구</option>
							<option value="111121">중구</option>
							<option value="111131">용산구</option>
							<option value="111181">은평구</option>
							<option value="111191">서대문구</option>
							<option value="111201">마포구</option>
							<option value="111142">성동구</option>
							<option value="111141">광진구</option>
							<option value="111152">동대문구</option>
							<option value="111151">중랑구</option>
							<option value="111161">성북구</option>
							<option value="111291">강북구</option>
							<option value="111171">도봉구</option>
							<option value="111311">노원구</option>
							<option value="111301">양천구</option>
							<option value="111212">강서구</option>
							<option value="111221">구로구</option>
							<option value="111281">금천구</option>
							<option value="111231">영등포구</option>
							<option value="111241">동작구</option>
							<option value="111251">관악구</option>
							<option value="111262">서초구</option>
							<option value="111261">강남구</option>
							<option value="111273">송파구</option>
							<option value="111274">강동구</option>
						</select>
					</div>
				</div>
				<div class="col-2">
					<c:choose>
						<c:when test='${duty_Nm.equals("")}'>
							<input type="text" id="duty_Nm" class="form-control"
								placeholder="병원명">
						</c:when>
						<c:otherwise>
							<input type="text" id="duty_Nm" class="form-control"
								value="${duty_Nm}">
						</c:otherwise>
					</c:choose>
				</div>
				<div class="col-2">
					<button id="search_Btn" type="button" class="btn btn-success">검색</button>
				</div>
			</div>
		</div>

		<div class="container mt-3 px-4">
			<div class="row row-cols-1 row-cols-md-4 g-4">
				<c:forEach var="emi" items="${er_Medi_Inst_List}">
					<div style="cursor: pointer;" class="col mt-4"
						onclick="window.location.href='${CP}/er_medi_inst/detail/${emi.hpid}'">
						<div class="card border-secondary h-100">
							<img src="/assets/images/er_medi_inst/${emi.hpid}.jpg"
								class="card-img-top" alt="${emi.dutyNm} 이미지" height="309.4px">
							<div class="card-body">
								<h5 style="font-size: 19px;" class="card-title fw-bolder">${emi.dutyNm}</h5>

								<p style="font-size: 14px; color: #616161;"
									class="card-text mb-1">${emi.dutyAddr}</p>
								<p style="font-size: 14px; color: #616161;"
									class="card-text pt-1 mb-0">대표전화 : ${emi.dutyTel}</p>
								<p style="font-size: 14px; color: #616161;"
									class="card-text mb-0">응급실전화 : ${emi.erTel}</p>
							</div>
						</div>
					</div>
				</c:forEach>


			</div>
		</div>
	</main>


	<!--// main---------------------------------------------------->





	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>


	<script>
		document.addEventListener("DOMContentLoaded", function() {

			const search_Btn = document.getElementById("search_Btn");

			search_Btn.addEventListener("click", function(event) {

				event.preventDefault();

				let gu = document.getElementById("gu").value;
				let duty_Nm = document.getElementById("duty_Nm").value;

				window.location.href = "${CP}/er_medi_inst/list?gu_Code=" + gu
						+ "&duty_Nm=" + duty_Nm;

			});

		});
	</script>

</body>
</html>