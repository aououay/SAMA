<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<c:set var="CP" value="${pageContext.request.contextPath}" />

<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Member Admin</title>
<!-- Bootstrap 5 -->
<link
	href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css"
	rel="stylesheet">
<!-- Bootstrap Icons -->
<link
	href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.8.1/font/bootstrap-icons.css"
	rel="stylesheet">
<link href="/assets/css/user_list.css" rel="stylesheet">

</head>
<body>
	<!-- 데스크탑 사이드바 -->
	<div class="sidebar-desktop">
		<div class="logo">서울365재난안전포털</div>
		<a href="#" class="nav-link active"> <i class="bi bi-people"></i>
			회원관리
		</a>
		<!--<a href="#" class="nav-link">
      <i class="bi bi-bar-chart"></i> 자유게시판
    </a>
    <a href="#" class="nav-link">
      <i class="bi bi-credit-card-2-front"></i> 영상 업로드
    </a>-->
	</div>
	<!-- 상단 해더 -->
	<div class="topbar">
		<div class="profile-area">
			<button class="btn btn-add-member"
				onclick="location.href='${CP}/main_user/user_reg_index.do'">
				회원 추가</button>
			<button class="btn btn-add-member"
				onclick="location.href='${CP}/index'">
				메인페이지로 이동</button>
		</div>
	</div>
	<!-- 메인 콘텐츠 영역 -->
	<div class="main-content">
		<!-- 검색 폼 -->
		<form action="/main_user/doRetrieve.do" method="get"
			class="row g-2 mb-3 align-items-center">
			<input type="hidden" name="pageNo" id="pageNo">
			<div class="col-auto">
				<select name="searchDiv" id="searchDiv"
					class="form-select form-select-sm">
					<option value="">전체</option>
					<option value="10"
						<c:if test="${10 == search.searchDiv}">selected</c:if>>회원ID</option>
					<option value="20"
						<c:if test="${20 == search.searchDiv}">selected</c:if>>이름</option>
					<option value="40"
						<c:if test="${40 == search.searchDiv}">selected</c:if>>이메일</option>
				</select>
			</div>
			<div class="col-auto">
				<input type="search" name="searchWord" id="searchWord"
					class="form-control form-control-sm" placeholder="검색어 입력"
					value="${search.searchWord}">
			</div>
			<div class="col-auto">
				<select name="pageSize" id="pageSize"
					class="form-select form-select-sm">
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
				<input type="submit" class="btn btn-outline-secondary btn-sm"
					value="조회">
			</div>
		</form>
		<!-- 회원 목록 카드 -->
		<div class="card">
			<div
				class="card-header d-flex justify-content-between align-items-center">
				<h4>Members</h4>
			</div>
			<div class="table-responsive">
				<table id="listTable" class="table table-hover align-middle">
					<thead>
						<tr>
							<th class="text-center">번호</th>
							<th class="text-start">회원ID</th>
							<th class="text-start">이름</th>
							<th class="text-start">이메일</th>
							<th class="text-start">닉네임</th>
							<th class="text-start">주소</th>
							<th class="text-center">핸드폰번호</th>
							<th class="text-center">등록일</th>
							<th class="text-center">관리자</th>
							<th class="text-center">구분</th>
							<th class="text-center">수정</th>
							<th class="text-center">삭제</th>
						</tr>
					</thead>
					<tbody>
						<c:forEach var="vo" items="${list}">
							<tr title="더블클릭 하면 상세 정보를 볼 수 있습니다.">
								<td class="text-center">${vo.getNo()}</td>
								<td class="text-start">${vo.userId}</td>
								<td class="text-start">${vo.name}</td>
								<td class="text-start">${vo.email}</td>
								<td class="text-start">${vo.nickname}</td>
								<td class="text-start">${vo.address}</td>
								<td class="text-center">${vo.phone}</td>
								<td class="text-center">${vo.regDate}</td>
								<td class="text-center"><c:choose>
										<c:when test="${vo.isAdmin == 'Y'}">관리자</c:when>
										<c:otherwise>유저</c:otherwise>
									</c:choose></td>
								<td class="text-center"><c:choose>
										<c:when test="${vo.isAdmin == 'Y'}">관리자</c:when>
										<c:otherwise>
											<a
												href="${pageContext.request.contextPath}/main_user/setAdmin.do?userId=${vo.userId}"
												class="btn btn-outline-secondary btn-sm">관리자 임명</a>
										</c:otherwise>
									</c:choose></td>
								<td class="text-center"><a
									href="/main_user/doSelectOne.do?userId=${vo.userId}"
									class="btn btn-outline-secondary btn-sm">수정</a></td>
								<td class="text-center">
									<button class="btn btn-outline-danger btn-sm"
										onclick="deleteUser('${vo.userId}')">삭제</button>
								</td>
							</tr>
						</c:forEach>
					</tbody>
				</table>
			</div>
			<!-- 페이징 -->
			<c:set var="totalPages"
				value="${(totalCnt + search.pageSize - 1) / search.pageSize}" />
			<c:if test="${totalPages lt 1}">
				<c:set var="totalPages" value="1" />
			</c:if>
			<div class="pagination-container">
				<c:if test="${search.pageNo > 1}">
					<a
						href="doRetrieve.do?pageNo=${search.pageNo - 1}&pageSize=${search.pageSize}&searchDiv=${search.searchDiv}&searchWord=${search.searchWord}"
						class="pagination-btn">이전</a>
				</c:if>
				<c:forEach var="i" begin="1" end="${totalPages}">
					<c:choose>
						<c:when test="${i == search.pageNo}">
							<span class="pagination-btn disabled"
								style="background-color: #365281; color: #fff;">${i}</span>
						</c:when>
						<c:otherwise>
							<a
								href="doRetrieve.do?pageNo=${i}&pageSize=${search.pageSize}&searchDiv=${search.searchDiv}&searchWord=${search.searchWord}"
								class="pagination-btn">${i}</a>
						</c:otherwise>
					</c:choose>
				</c:forEach>
				<c:if test="${search.pageNo < totalPages}">
					<a
						href="doRetrieve.do?pageNo=${search.pageNo + 1}&pageSize=${search.pageSize}&searchDiv=${search.searchDiv}&searchWord=${search.searchWord}"
						class="pagination-btn">다음</a>
				</c:if>
			</div>
		</div>
	</div>

	<!-- jQuery & Bootstrap JS -->
	<script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
	<script
		src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
	<script src="/assets/js/user_list.js"></script>
</body>
</html>
