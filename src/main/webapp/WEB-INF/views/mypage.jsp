<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <title>회원 정보 수정</title>
    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="/assets/css/mypage.css" rel="stylesheet">
	<link href="/assets/css/header.css" rel="stylesheet">
    
	
	<script>
	  document.addEventListener('DOMContentLoaded', function() {
	    document.getElementById('editButton').addEventListener('click', function() {
	      if (this.textContent === '정보 수정') {
	        ['name', 'email', 'nickname', 'address', 'phone', 'password'].forEach(function(id) {
	          document.getElementById(id).readOnly = false;
	        });
	        this.textContent = '수정 내용 저장';
	        this.classList.replace('btn-primary', 'btn-success');
	      } else {
	        document.getElementById('userForm').submit();
	      }
	    });

	    <c:if test="${not empty successMessage}">
	      alert('${successMessage}');
	      window.location.href = '${pageContext.request.contextPath}/main';
	    </c:if>
	  });
	</script>


</head>
<body>
	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>
    
    <!-- 정보 수정 카드 -->
    <div class="container mt-5">
        <div class="row justify-content-center">
            <div class="col-md-6">
                <div class="card p-4">
                    <h2 class="text-center mb-4">회원 정보 수정</h2>
                    <!-- 오류 또는 성공 메시지 -->
                    <c:if test="${not empty errorMessage}">
                        <div class="alert alert-danger">${errorMessage}</div>
                    </c:if>
                    <c:if test="${not empty successMessage}">
                        <div class="alert alert-success">${successMessage}</div>
                    </c:if>
					<!-- 정보 수정 폼 -->
					<form id="userForm" action="${pageContext.request.contextPath}/mypage/update" method="post">
					    <!-- 회원ID: 수정 불가 -->
					    <div class="mb-3">
					        <label for="userId" class="form-label">회원ID</label>
					        <input type="text" class="form-control" id="userId" name="userId" value="${user.userId}" readonly>
					    </div>
					    <!-- 이름 -->
					    <div class="mb-3">
					        <label for="name" class="form-label">이름</label>
					        <input type="text" class="form-control" id="name" name="name" value="${user.name}" readonly>
					    </div>
					    <!-- 이메일 -->
					    <div class="mb-3">
					        <label for="email" class="form-label">이메일</label>
					        <input type="email" class="form-control" id="email" name="email" value="${user.email}" readonly>
					    </div>
					    <!-- 닉네임 -->
					    <div class="mb-3">
					        <label for="nickname" class="form-label">닉네임</label>
					        <div class="input-group">
					        	<input type="text" class="form-control" id="nickname" name="nickname" value="${user.nickname}" readonly>
					   			<button type="button" class="btn btn-secondary" onclick="checkNickname()">중복 확인</button>
					   		</div>
					    </div>
					    <!-- 주소 (주소찾기 기능 포함) -->
					    <div class="mb-3">
					        <label for="address" class="form-label">주소</label>
					        <div class="input-group">
					            <input type="text" class="form-control" id="address" name="address" value="${user.address}" readonly>
					            <button type="button" class="btn btn-outline-secondary" onclick="execDaumPostcode()">주소찾기</button>
					        </div>
					    </div>
					    <!-- 전화번호 -->
					    <div class="mb-3">
					        <label for="phone" class="form-label">전화번호</label>
					        <input type="text" class="form-control" id="phone" name="phone" value="${user.phone}" readonly>
					    </div>
					    <!-- 비밀번호: 빈 칸이면 기존 비밀번호 유지 -->
					    <div class="mb-3">
					        <label for="password" class="form-label">비밀번호</label>
					        <input type="password" class="form-control" id="password" name="password" placeholder="변경할 비밀번호 입력" readonly>
					    </div>
					    <!-- 단일 수정/저장 버튼 -->
					    <div class="d-grid gap-2">
					        <button type="button" id="editButton" class="btn btn-primary">정보 수정</button>
					    </div>
					</form>
                    <div class="mt-3 text-center">
                        <a href="${pageContext.request.contextPath}/main" class="btn btn-secondary me-2">메인 페이지로 이동</a>
                    </div>
                </div>
            </div>
        </div>
    </div>
	
	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>
    
    <!-- 성공 메시지가 있는 경우 알럿 및 리다이렉트 -->
    <c:if test="${not empty successMessage}">
        <script type="text/javascript">
            alert('정보수정이 완료 되었습니다');
            window.location.href = '${pageContext.request.contextPath}/main';
        </script>
    </c:if>
    
    <!-- Bootstrap JS (옵션) -->
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="/assets/js/mypage.js"></script>
    
    <!-- 스크립트 -->
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    <!-- Daum 우편번호 서비스 스크립트 -->
    <script src="https://t1.daumcdn.net/mapjsapi/bundle/postcode/prod/postcode.v2.js"></script>
</body>

</html>
