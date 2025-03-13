<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>

<!DOCTYPE html>
<html>

<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet" >
<link href="/assets/css/header.css" rel="stylesheet" >
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<title>시민안전보험 조회</title>
<script src="/assets/js/jquery_3_7_1.js"></script>
</head>
<link rel="icon" href="data:;base64,iVBORw0KGgo=">
<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>


	<div id="container" class="container">
		<!-- main-->
		<main  class="container my-4 mx-1">
      		<article class="col-md-12">	     
		
		    
	        <!-- 페이지 제목 -->
	        <h2 class="text-center mb-4">시민안전보험 조회</h2>
		
			<!-- 보험 조회 폼 -->
			<form action="/ins/getIns.do" method="get" class="mb-5">
			    <div class="d-flex justify-content-between align-items-center">
			  
			        <p class="lead mb-0">원하는 지역과 연도를 선택하세요.</p>
			        
			        <div class="d-flex align-items-center">
			            <!-- 시군구명 선택 -->
			            <div class="mr-3">
			                <select id="insSggNm" name="insSggNm" class="form-control custom-select-width" required>
			                    <c:forEach var="sggNm" items="${sggNames}">
			                        <option value="${sggNm.insSggNm}"
			                                <c:if test="${sggNm.insSggNm == param.insSggNm}">selected</c:if>>
			                            ${sggNm.insSggNm eq 'None' ? '본청' : sggNm.insSggNm}
			                        </option>
			                    </c:forEach>
			                </select>
			            </div>
			
			            <!-- 년도 선택 -->
			            <div class="mr-3">
			            	<select id="insStartDt" name="insStartDt" class="form-control custom-select-width" required>
						        <option value="20250101" 
						            <c:if test="${param.insStartDt == null}">selected</c:if>
						            <c:if test="${param.insStartDt == '20250101' || param.insStartDt == null}">selected</c:if>
						        >2025</option>
						        <option value="20240101" 
						            <c:if test="${'20240101' == param.insStartDt}">selected</c:if>
						        >2024</option>
						        <option value="20230101" 
						            <c:if test="${'20230101' == param.insStartDt}">selected</c:if>
						        >2023</option>
						        <option value="20220101" 
						            <c:if test="${'20220101' == param.insStartDt}">selected</c:if>
						        >2022</option>
						        <option value="20210101" 
						            <c:if test="${'20210101' == param.insStartDt}">selected</c:if>
						        >2021</option>
						    </select>
			            </div>
			
			            <!-- 검색 버튼 -->
			            <div>
			                <button type="submit" class="btn btn-primary">조회</button>
			            </div>
			        </div>
			    </div>
				<hr>
			</form>


	        <!-- 보험 정보 테이블 -->
	        <div class="table-responsive">
	            <table id="listTable" class="table table-bordered">
	            	<colgroup>
			            <col width="10%">
			            <col width="15%">
			            <col width="10%">
			            <col width="10%">
			            <col width="10%"> 
			            <col width="40%"> 
			            <col width="5%">            
		        	</colgroup>
	                <thead>
	                    <tr class="table-primary">
	                        <th>시군구명</th>
	                        <th>보험대상</th>
	                        <th>보장시작일</th>
	                        <th>보장종료일</th>
	                        <th>보험사</th>
	                        <th>청구방법</th>
	                        <th>홈페이지</th>
	                    </tr>
	                </thead>
	                <tbody>
	                    <c:choose>
	                        <c:when test="${ins.size() > 0}">
	                            <c:forEach var="vo" items="${ins}">
	                                <tr>
	                                    <td class="text-center">${vo.insSggNm eq 'None' ? '본청' : vo.insSggNm}</td>
	                                    <td class="text-left">${vo.insClient}</td>
	                                    <td class="text-center">${vo.insStartDt}</td>
	                                    <td class="text-left">${vo.insEndDt}</td>
	                                    <td class="text-center">${vo.insCompany}</td>
	                                    <td class="text-center">${vo.insMethod}</td>
	                                    <td class="text-center"><a href="${vo.insUrl}" target="_blank">Go</a></td>
	                                </tr>
	                            </c:forEach>
	                        </c:when>
	                        <c:otherwise>
	                            <tr>
	                                <td colspan="7" class="text-center">해당 지역의 보험정보가 존재하지 않습니다.</td>
	                            </tr>
	                        </c:otherwise>
	                    </c:choose>
	                </tbody>
	            </table>
	        </div>
	
	        <!-- 보장항목 테이블 -->
	        <div class="table-responsive">
	            <table id="grntTable" class="table table-striped table-bordered">
	                <thead>
	                    <tr class="table-primary">
	                        <th>보장항목</th>
	                        <th>보장내용</th>
	                        <th>보장금액(만원)</th>
	                    </tr>
	                </thead>
	                <tbody>
	                    <c:choose>
	                        <c:when test="${grnt.size() > 0}">
	                            <c:forEach var="gr" items="${grnt}">
	                                <tr>
	                                    <td class="text-left">${gr.grntNm}</td>
	                                    <td class="text-left">${gr.grntContent}</td>
	                                    <td class="text-center">${gr.grntAmount}</td>
	                                </tr>
	                            </c:forEach>
	                        </c:when>
	                        <c:otherwise>
	                            <tr>
	                                <td colspan="3" class="text-center">해당 지역의 보험정보가 존재하지 않습니다.</td>
	                            </tr>
	                        </c:otherwise>
	                    </c:choose>
	                </tbody>
	            </table>
	        </div>
	 
	    	</article>
	    </main>
		<!--// main---------------------------------------------------->
	    

	</div>
		<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>
    <!-- 사용자 정의 스크립트 -->
    <script src="/assets/js/pcwk.js"></script>
    <!-- Bootstrap JS -->
    <script src="/assets/js/bootstrap.min.js"></script>
</body>
</html>
