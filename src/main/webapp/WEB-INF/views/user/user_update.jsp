<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/fmt" prefix="fmt" %>

<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>회원 정보 수정</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body>
    <div class="container">
        <main class="mt-4">
            <h2>회원 정보 수정</h2>
            <hr class="title-underline">
            <form action="/main_user/doUpdate.do" method="post">
                <!-- Hidden Field to Pass User ID -->

                
                <div class="mb-3">
                    <label for="userId" class="form-label">회원ID</label>
                    <input type="text" class="form-control" id="userId" name="userId" value="${outVO.userId}" readonly>
                </div>

                <div class="mb-3">
                    <label for="name" class="form-label">이름</label>
                    <input type="text" class="form-control" id="name" name="name" value="${outVO.name}" readonly>
                </div>

                <div class="mb-3">
                    <label for="email" class="form-label">이메일</label>
                    <input type="email" class="form-control" id="email" name="email" value="${outVO.email}" required>
                </div>

                <div class="mb-3">
                    <label for="nickname" class="form-label">닉네임</label>
                    <input type="text" class="form-control" id="nickname" name="nickname" value="${outVO.nickname}">
                </div>

                <div class="mb-3">
                    <label for="address" class="form-label">주소</label>
                    <input type="text" class="form-control" id="address" name="address" value="${outVO.address}">
                </div>

                <div class="mb-3">
                    <label for="phone" class="form-label">핸드폰 번호</label>
                    <input type="text" class="form-control" id="phone" name="phone" value="${outVO.phone}">
                </div>

                <div class="mb-3">
                    <label for="password" class="form-label">비밀번호</label>
                    <input type="password" class="form-control" id="password" name="password" required>
                </div>

                <div class="mb-3">
                    <button type="submit" class="btn btn-primary">수정하기</button>
                    <a href="/main_user/doRetrieve.do" class="btn btn-secondary">취소</a>
                </div>
            </form>
        </main>
    </div>
    
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
