<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta http-equiv="X-UA-Compatible" content="IE=edge">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>회원가입</title>
    <!-- Bootstrap CSS -->
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link href="/assets/css/user_reg.css" rel="stylesheet">
    <!-- jQuery -->
    <script src="https://code.jquery.com/jquery-3.6.0.min.js"></script>
    
</head>
<body>
    <div class="form-container">
        <h1>회원가입</h1>
        <form id="userForm" action="/main_user/doSave.do" method="post">
            <!-- 아이디 입력 -->
            <div class="mb-3">
                <label for="userId" class="form-label">아이디</label>
                <div class="input-group">
                    <input type="text" class="form-control" id="userId" name="userId" placeholder="아이디를 입력하세요">
                    <button type="button" class="btn btn-secondary" onclick="checkUserId()">중복 확인</button>
                </div>
            </div>

            <!-- 비밀번호 입력 -->
            <div class="mb-3">
                <label for="password" class="form-label">비밀번호</label>
                <input type="password" class="form-control" id="password" name="password" placeholder="비밀번호를 입력하세요" required>
            </div>

            <!-- 비밀번호 확인 입력 -->
            <div class="mb-3">
                <label for="confirmPassword" class="form-label">비밀번호 확인</label>
                <input type="password" class="form-control" id="confirmPassword" placeholder="비밀번호를 다시 입력하세요" required>
                <div id="passwordError" class="error-message"></div>
            </div>

            <!-- 이름 입력 -->
            <div class="mb-3">
                <label for="name" class="form-label">이름</label>
                <input type="text" class="form-control" id="name" name="name" placeholder="이름을 입력하세요" required>
            </div>

            <!-- 닉네임 입력 -->
            <div class="mb-3">
                <label for="nickname" class="form-label">닉네임</label>
                <div class="input-group">
	                <input type="text" class="form-control" id="nickname" name="nickname" placeholder="닉네임을 입력하세요" required>
	                <button type="button" class="btn btn-secondary" onclick="checkNickname()">중복 확인</button>
	            </div>
            </div>

            <!-- 이메일 입력 및 인증 -->
            <div class="mb-3">
                <label for="email" class="form-label">이메일</label>
                <div class="input-group">
                    <input type="email" class="form-control" id="email" name="email" placeholder="이메일을 입력하세요" required>
                    <button type="button" class="btn btn-secondary" onclick="sendAuthCode()">인증 요청</button>
                </div>
            </div>

            <!-- 인증 코드 입력 -->
            <div class="mb-3">
                <label for="authCode" class="form-label">인증 코드</label>
                <div class="input-group">
                    <input type="text" class="form-control" id="authCode" name="authCode" placeholder="인증 코드를 입력하세요">
                    <button type="button" class="btn btn-secondary" onclick="verifyAuthCode()">인증 확인</button>
                </div>
            </div>

            <!-- 전화번호 입력 -->
            <div class="mb-3">
                <label for="phone" class="form-label">전화번호</label>
                <input type="text" class="form-control" id="phone" name="phone" placeholder="전화번호를 입력하세요" required>
            </div>

            <!-- 주소 입력 -->
            <div class="mb-3">
                <label for="address" class="form-label">주소</label>
                <div class="input-group">
                    <input type="text" class="form-control" id="address" name="address" placeholder="주소를 검색하세요" readonly required>
                    <button type="button" class="btn btn-secondary" onclick="sample5_execDaumPostcode()">주소 검색</button>
                </div>
            </div>

            <!-- 버튼 그룹 -->
            <div class="d-grid gap-2">
                <button type="submit" class="btn btn-primary btn-custom">회원가입</button>
                <button id="return_main" class="btn btn-secondary btn-custom">로그인으로 돌아가기</button>
            </div>
        </form>
    </div>

    

    <!-- Daum 주소 API -->
    <script src="//t1.daumcdn.net/mapjsapi/bundle/postcode/prod/postcode.v2.js"></script>
    <script src="//dapi.kakao.com/v2/maps/sdk.js?appkey=6494369aebd01baf926e57a1affa1288&libraries=services"></script>
    <script src="/assets/js/user_reg.js"></script>
</body>
</html>