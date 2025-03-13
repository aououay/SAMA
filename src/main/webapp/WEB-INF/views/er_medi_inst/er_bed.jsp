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
<link href="/assets/css/er_bed.css" rel="stylesheet">
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


		<div class="container">
			<h1 class="mt-3 mb-3 text-start fs-3 ps-3">서울특별시 응급실 가용 병상 정보</h1>
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
				<div class="container ps-0 pt-0 pb-1">
					<p style="font-size: 14px;"
						class="pt-2 mb-0 text-start text-dark">최근 업데이트
						시간&nbsp:&nbsp${er_Bed_List[0].hviDt}</p>
				</div>
			</div>
		</div>


		<c:forEach var="er_bed" items="${er_Bed_List}">
			<div class="container-fluid d-flex justify-content-center mt-0">
				<table class="table table-bordered border-dark text-center w-75"
					style="table-layout: fixed; max-width: 1320px;">

					<thead>
						<tr class="text-center table-success">
							<th class="pb-4" scope="col" style="width: 16%;">기관명</th>
							<th class="pb-4" scope="col" style="width: 10%;">전화</th>
							<th class="pb-3" scope="col" style="width: 7%;">응급실</th>
							<th class="pb-3" scope="col" style="width: 7%;">수술실</th>
							<th class="pb-3" scope="col" style="width: 7%;">신경중환자실</th>
							<th class="pb-3" scope="col" style="width: 7%;">신생중환자실</th>
							<th class="pb-3" scope="col" style="width: 7%;">흉부중환자실</th>
							<th class="pb-3" scope="col" style="width: 7%;">일반중환자실</th>
							<th class="pb-3" scope="col" style="width: 7%;">입원실</th>
							<th class="pb-3" scope="col" style="width: 7%;">내과중환자실</th>
							<th class="pb-3" scope="col" style="width: 7%;">외과중환자실</th>
							<th class="pb-2" scope="col" style="width: 7%;">외과입원실</th>
							<th class="pb-3" scope="col" style="width: 7%;">신경과입원실</th>
							<th class="pb-3" scope="col" style="width: 7%;">신경외과중환자실</th>
							<th class="pb-3" scope="col" style="width: 7%;">약물중환자실</th>
							<th class="pb-3" scope="col" style="width: 7%;">인큐베이터</th>
							<th class="pb-3" scope="col" style="width: 7%;">CT가용</th>
							<th class="pb-3" scope="col" style="width: 7%;">MRI가용</th>
						</tr>
					</thead>
					<tbody>
						<tr>
							<th style="cursor: pointer;"
								onclick="window.location.href='${CP}/er_medi_inst/detail/${er_bed.hpid}'"
								class="table-success text-decoration-underline" scope="row">${er_bed.dutyNm}</th>
							<td>${er_bed.erTel}</td>
							<td>${er_bed.hvec}</td>
							<td>${er_bed.hvoc}</td>
							<td>${er_bed.hvcc}</td>
							<td>${er_bed.hvncc}</td>
							<td>${er_bed.hvccc}</td>
							<td>${er_bed.hvicc}</td>
							<td>${er_bed.hvgc}</td>
							<td>${er_bed.hv2}</td>
							<td>${er_bed.hv3}</td>
							<td>${er_bed.hv4}</td>
							<td>${er_bed.hv5}</td>
							<td>${er_bed.hv6}</td>
							<td>${er_bed.hv7}</td>
							<td>${er_bed.hv11}</td>
							<td>${er_bed.hvctYn }</td>
							<td>${er_bed.hvmriYn}</td>
						</tr>
					</tbody>
				</table>
			</div>
		</c:forEach>
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

				window.location.href = "${CP}/er_medi_inst/er_bed?gu_Code="
						+ gu + "&duty_Nm=" + duty_Nm;

			});

		});
	</script>

</body>
</html>