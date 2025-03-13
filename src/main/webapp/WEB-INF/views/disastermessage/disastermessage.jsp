<%--
  Created by IntelliJ IDEA.
  User: acorn4
  Date: 2025-02-24
  Time: PM 2:07
  To change this template use File | Settings | File Templates.
--%>
<%@ page import="com.pcwk.ehr.cmn.StringUtil"%>
<%@ page import="com.pcwk.ehr.cmn.SearchVO"%>
<%@ page contentType="text/html;charset=UTF-8" language="java"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<%
/**
 *
 * @param maxNum : 총 글수
 * @param currentPageNo: 현재 페이지번호
 * @param rowPerPage : 페이지 사이즈 (10,20,...100)
 * @param bottomCount: 10/5
 * @param url : 서버 호출 URL
 * @param scriptName : 자바스크립트 함수명
 * @return "html 텍스트"
 */
int bottomCount = 10;
int pageSize = 10;
int pageNo = 0;

int maxNum = Integer.parseInt(request.getAttribute("totalCnt").toString());//총 글수

//out.print("****:"+maxNum);
SearchVO paramVO = (SearchVO) request.getAttribute("search");
pageSize = paramVO.getPageSize();
pageNo = paramVO.getPageNo();

//out.print("pageSize****:"+pageSize);
//out.print("pageNo****:"+pageNo);

String cp = request.getContextPath();
String pageHtml = StringUtil.renderingPager(maxNum, pageNo, pageSize, bottomCount, cp + "/disastermessage/view",
		"pageDoRetrieve");
%>

<!DOCTYPE html>
<html>
<head>
<title>재난문자</title>
<link href="/assets/css/header.css" rel="stylesheet">
<link href="/assets/css/dismes.css" rel="stylesheet">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet">
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<title>Insert title here</title>
<script src="/assets/js/jquery_3_7_1.js"></script>
</head>


<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>


	<div id="container" class="message">

		<div class="container">

			<div class="listWrap">

				<h1 class="fs-4 mb-4 pt-4 fw-bolder">긴급재난문자</h1>

				<form action="#" class="search-form" name="userForm" id="userForm"
					method="get" enctype="application/x-www-form-urlencoded">
					<input type="hidden" name="pageNo" id="pageNo">


					<div class="search-group">

						<%--controller 에서 지정한 searchVO 타입의 search.searchDiv에 값을 줌--%>
						<select name="searchDiv" id="searchDiv">
							<option id="" value="10"
								<c:if test="${10 == search.searchDiv}">selected</c:if>>검색
								항목을 선택하세요</option>
							<option value="20"
								<c:if test="${20 == search.searchDiv}">selected</c:if>>지역</option>
							<option value="30"
								<c:if test="${30 == search.searchDiv}">selected</c:if>>재난종류</option>
						</select>

						<%--controller 에서 지정한 searchVO 타입의 search.searchWord에 값을 줌--%>
						<input type="search" name="searchWord" id="searchWord"
							value="${search.searchWord}" placeholder="검색어를 입력하세요">

						<%--controller에서 @RequestParam pageSize를 화면에서 값을 주도록 함--%>
						<select name="pageSize" id="pageSize">
							<option value="10"
								<c:if test = "${10 == search.pageSize}">selected</c:if>>10</option>
							<option value="20"
								<c:if test = "${20 == search.pageSize}">selected</c:if>>20</option>
							<option value="30"
								<c:if test = "${30 == search.pageSize}">selected</c:if>>30</option>
							<option value="40"
								<c:if test = "${40 == search.pageSize}">selected</c:if>>40</option>
							<option value="50"
								<c:if test = "${50 == search.pageSize}">selected</c:if>>50</option>
							<option value="100"
								<c:if test = "${100 == search.pageSize}">selected</c:if>>100</option>
						</select> <input type="button" value="조회" id="doRetrieveBtn"> <input
							type="button" value="검색초기화" id="reset">

					</div>
				</form>
			</div>
			</form>

			<table class="tableTypeE">

				<thead>
					<tr>
						<th>번호</th>
						<th>지역</th>
						<th>재난종류</th>
						<th>재난내용</th>
						<th>등록일</th>
					</tr>
				</thead>


				<tbody>
					<%--controller에서 DisasterMessageVO타입의 데이터를 list에 담아서 반복문을 돌려 뿌려줌--%>
					<c:forEach var="list" items="${list}">
						<td>${list.disMesNum}</td>
						<td>${list.disMesArea}</td>
						<td>${list.disMesType}</td>
						<%--controller에서 @Requestparam으로 disMesNum을 받아와 해당 disMesNum의 데이터를 단건검색함--%>
						<td><a
							href="/disastermessage/Detail?disMesNum=${list.disMesNum}">${list.disMesCon}</a></td>
						<td>${list.disMesCreateDt}</td>
						</tr>
					</c:forEach>
				</tbody>

			</table>
		</div>


		<br>
		<%--pagination--%>
		<%
		out.print(pageHtml);
		%>

	</div>


	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>


	<script src="/assets/js/dismes/dismes_page.js"></script>
	<!-- 여기에 스크립트 파일 경로 -->
</body>
</html>