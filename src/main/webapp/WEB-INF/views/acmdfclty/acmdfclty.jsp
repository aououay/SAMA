<%@ page import="com.pcwk.ehr.cmn.StringUtil"%>
<%@ page import="com.pcwk.ehr.cmn.SearchVO"%>
<%@ page contentType="text/html;charset=UTF-8" language="java"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%
int bottomCount = 10;
int pageSize = 10;
int pageNo = 0;

Object totalCntObj = request.getAttribute("totalCnt");
int maxNum = 0;
if (totalCntObj != null) {
	maxNum = Integer.parseInt(totalCntObj.toString());
}

SearchVO paramVO = (SearchVO) request.getAttribute("search");
if (paramVO != null) {
	pageSize = paramVO.getPageSize();
	pageNo = paramVO.getPageNo();
}

String cp = request.getContextPath();
String pageHtml = StringUtil.renderingPager(maxNum, pageNo, pageSize, bottomCount, cp + "/acmdfclty/view",
		"pageDoRetrieve");
%>
<html>
<head>
<title>지진옥외대피장소</title>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet">
<link href="/assets/css/header.css" rel="stylesheet">
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<script src="/assets/js/jquery_3_7_1.js"></script>
<link rel="stylesheet" type="text/css" href="/assets/css/acmdfclty.css">
<script>
	function openPopup(name, address, lat, lng, capacity, facilityArea,
			useSeCd, managementOffice) {
		var url = "/acmdfclty/mapPopup?name=" + encodeURIComponent(name)
				+ "&address=" + encodeURIComponent(address) + "&lat=" + lat
				+ "&lng=" + lng + "&capacity=" + encodeURIComponent(capacity)
				+ "&facilityArea=" + encodeURIComponent(facilityArea)
				+ "&useSeCd=" + encodeURIComponent(useSeCd)
				+ "&managementOffice=" + encodeURIComponent(managementOffice);
		window.open(url, "popupWindow", "width=600,height=500");
	}
</script>
</head>


<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

	<div id="container" class="message">
		<div id="state_sub">
			<div class="stateWrap">
				<strong>대피소목록</strong>
				<ul>
					<li><a><strong> 지진옥외대피장소</strong></a></li>
				</ul>
			</div>
		</div>
		<div class="intro-section">
			<p>위치, 시설명으로 대피시설 정보를 조회하실 수 있습니다.</p>
			<p>민방위사태 발생 시 주민의 생명과 재산을 보호하기 위하여 정부 지원으로 설치 또는 공공용으로 지정된
				대피시설입니다.</p>
		</div>
		<div class="listWrap">
		<form action="#" class="search-form" name="userForm" id="userForm"
			method="get">
			<input type="hidden" name="pageNo" id="pageNo">
			<div class="search-group">
				<select name="searchDiv" id="searchDiv">
					<option value="10"
						<c:if test="${10 == search.searchDiv}">selected</c:if>>검색
						항목 선택</option>
					<option value="20"
						<c:if test="${20 == search.searchDiv}">selected</c:if>>위치
						검색</option>
					<option value="30"
						<c:if test="${30 == search.searchDiv}">selected</c:if>>시설명
						검색</option>
				</select> <input type="search" name="searchWord" id="searchWord"
					value="${search.searchWord}" placeholder="검색어를 입력하세요"> <select
					name="pageSize" id="pageSize">
					<option value="10"
						<c:if test="${10 == search.pageSize}">selected</c:if>>10</option>
					<option value="20"
						<c:if test="${20 == search.pageSize}">selected</c:if>>20</option>
				</select> <input type="button" value="조회" id="doRetrieveBtn"> <input
					type="button" value="검색초기화" id="reset">
			</div>
		</form>

		<table class="tableTypeE">
			<thead>
				<tr>
					<th>위치</th>
					<th>시설명</th>
					<th>면적</th>
					<th>최대 수용 인원</th>
				</tr>
			</thead>
			<tbody>
				<c:forEach var="list" items="${list}">
					<tr>
						<td><a href="javascript:void(0);"
							onclick="openPopup('${list.vtAcmdfcltyNm}', '${list.rnDtlAdres}', '${list.la}', '${list.lo}', 
    '${list.vtAcmdPsbilNmpr}', '${list.fcltyAr}', '${list.useSeCd}');">
								${list.rnDtlAdres} </a></td>
						<td>${list.vtAcmdfcltyNm}</td>
						<td>${list.fcltyAr}㎡</td>
						<td>${list.vtAcmdPsbilNmpr}명</td>

					</tr>
				</c:forEach>
			</tbody>
		</table>
	</div>
	<br>
	<%
	out.print(pageHtml);
	%>
	</div>

	


	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>
	<script src="/assets/js/acmdfclty/acmdfclty_page.js"></script>
</body>
</html>
