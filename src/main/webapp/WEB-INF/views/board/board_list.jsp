<%@page import="com.pcwk.ehr.cmn.SearchBoardVO"%>
<%@page import="com.pcwk.ehr.cmn.StringUtil"%>
<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<%
// 페이징 처리를 위한 코드
int bottomCount = 10;
SearchBoardVO paramVO = (SearchBoardVO) request.getAttribute("search");
int pageSize = paramVO.getPageSize();
int pageNo = paramVO.getPageNo();
int maxNum = Integer.parseInt(request.getAttribute("totalCnt").toString());
String cp = request.getContextPath();
String pageHtml = StringUtil.renderingPager(maxNum, pageNo, pageSize, bottomCount, cp + "/board/doRetrieve.do",
		"pageDoRetrieve");
%>

<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="/assets/css/list.css" rel="stylesheet">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet">
<link href="/assets/css/header.css" rel="stylesheet">
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
	
	
<!-- 제목 설정 -->
<c:choose>
	<c:when test="${board_div eq '10'}">
		<title>공지사항</title>
	</c:when>
	<c:when test="${board_div eq '20'}">
		<title>자유게시판</title>
	</c:when>
	<c:when test="${board_div eq '30'}">
		<title>통계자료실</title>
	</c:when>
</c:choose>
<script src="/assets/js/jquery_3_7_1.js"></script>
</head>
<link rel="icon" href="data:;base64,iVBORw0KGgo=">

		
<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

	<div class="wrapper">


		<!-- main -->
		<main class="container my-4">
			<article class="col-md-12">
				<!-- 제목 영역 -->
				<c:choose>
					<c:when test="${board_div eq '10'}">
						<h2 class="text-center mb-4">공지사항</h2>
					</c:when>
					<c:when test="${board_div eq '20'}">
						<h2 class="text-center mb-4">자유게시판</h2>
					</c:when>
					<c:when test="${board_div eq '30'}">
						<h2 class="text-center mb-4">통계자료실</h2>
					</c:when>
				</c:choose>
				<hr>

				<!-- 검색 조건 폼 -->
				<form class="row g-3 mb-3 align-items-center justify-content-end"
					action="#" name="userForm" id="userForm" method="get">
					<input type="hidden" name="pageNo" id="pageNo"> <input
						type="hidden" name="div" id="div" value="${board_div}">

					<div class="col-auto">
						<select name="searchDiv" id="searchDiv" class="form-select">
							<option value="">전체</option>
							<option value="10"
								<c:if test="${10 == search.searchDiv}">selected</c:if>>제목</option>
							<option value="20"
								<c:if test="${20 == search.searchDiv}">selected</c:if>>내용</option>
							<option value="30"
								<c:if test="${30 == search.searchDiv}">selected</c:if>>작성자</option>
						</select>
					</div>
					<div class="col-auto">
						<input class="form-control" type="search" name="searchWord"
							id="searchWord" value="${search.searchWord}">
					</div>
					<div class="col-auto">
						<select class="form-select" name="pageSize" id="pageSize">
							<option value="10"
								<c:if test="${10 == search.pageSize}">selected</c:if>>10</option>
							<option value="20"
								<c:if test="${20 == search.pageSize}">selected</c:if>>20</option>
							<option value="30"
								<c:if test="${30 == search.pageSize}">selected</c:if>>30</option>
							<option value="50"
								<c:if test="${50 == search.pageSize}">selected</c:if>>50</option>
							<option value="100"
								<c:if test="${100 == search.pageSize}">selected</c:if>>100</option>
						</select>
					</div>
					<div class="col-auto">
						<input class="btn btn-primary" type="button" value="조회"
							id="doRetrieveBtn">

						<!-- 등록 버튼 조건: 자유게시판(20)은 로그인한 회원, 공지사항(10)과 통계자료실(30)은 관리자만 -->
						<c:choose>
							<c:when
								test="${ (board_div eq '20' and not empty sessionScope.user)
	                            or ((board_div eq '10' or board_div eq '30') and not empty sessionScope.user and sessionScope.user.isAdmin eq 'Y') }">
								<input class="btn btn-success" type="button" value="등록"
									id="moveToRegBtn">
							</c:when>
							<c:otherwise>
								<input class="btn btn-success" type="hidden" value="등록"
									id="moveToRegBtn">
							</c:otherwise>
						</c:choose>
					</div>
				</form>

				<!-- 게시글 목록 테이블 -->
				<div class="table-responsive">
					<table id="listTable"
						class="table table-bordered table-hover table-sm">
						<thead class="table-light">
							<tr>
								<th class="text-center py-2 px-3" style="width: 7%;">번호</th>
								<th class="text-left py-2 px-3">제목</th>
								<th class="text-left py-2 px-3" style="width: 15%;">작성자</th>
								<th class="text-center py-2 px-3" style="width: 10%;">작성일</th>
								<th class="text-center py-2 px-3" style="width: 7%;">조회수</th>
							</tr>
						</thead>
						<tbody>
							<c:choose>
								<c:when test="${boardList.size() > 0}">
									<c:forEach var="vo" items="${boardList}">
										<tr class="table-hover">
											<td class="text-center py-2 px-3">${vo.boardId}</td>
											<td class="text-left py-2 px-3"><a
												href="/board/doSelectOne.do?div=${board_div}&boardId=${vo.boardId}"
												class="text-decoration-none text-dark"> <c:out
														value="${vo.title}" escapeXml="true" />
											</a></td>
											<td class="text-left py-2 px-3">${vo.regId}</td>
											<td class="text-center py-2 px-3">${vo.regDt}</td>
											<td class="text-center py-2 px-3">${vo.readCnt}</td>
										</tr>
									</c:forEach>
								</c:when>
								<c:otherwise>
									<tr>
										<td colspan="5" class="text-center py-2 px-3">데이터가 존재하지
											않습니다.</td>
									</tr>
								</c:otherwise>
							</c:choose>
						</tbody>
					</table>
				</div>



				<!-- 페이징 영역 -->
				<div class="d-flex justify-content-center mt-3">
					<%=pageHtml%>
				</div>
			</article>
		</main>

	<!-- footer-->
      <jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
    <!--// footer-------------------------------------------------->
	</div>

	<!-- 스크립트 -->
	<script src="/assets/js/board_list.js"></script>
	<script src="/assets/js/bootstrap.bundle.min.js"></script>
</body>
</html>
