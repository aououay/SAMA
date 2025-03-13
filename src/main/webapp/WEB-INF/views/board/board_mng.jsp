<%@ page language="java" contentType="text/html; charset=UTF-8"
    pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <!-- Bootstrap 및 헤더 CSS -->
  <link href="/assets/css/bootstrap.min.css" rel="stylesheet">
  <link href="/assets/css/header.css" rel="stylesheet">
  <link href="/assets/css/footer.css" rel="stylesheet">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
  <title>${vo.title}</title>
  <script src="/assets/js/jquery_3_7_1.js"></script>
  <link rel="icon" href="data:;base64,iVBORw0KGgo=">
  <!-- 커스텀 CSS -->
  <style>
    /* 전체 컨테이너 */
    #container {
      max-width: 1320px;
      margin: 0 auto;
    }
    
    textarea {
      resize: none;
    }
    
    /* 메인 영역: 박스 쉐도우 제거, 대신 연한 회색 라인 테두리 */
    main.container {
      background-color: #fff;
      padding: 2rem;
      margin-top: 1rem;
      border: 1px solid #eee;
      border-radius: 5px;
    }
    /* 제목 영역: 제목과 수정/삭제 버튼 한 줄 배치 */
    .title-section {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 0.5rem;
    }
    .title-section h2 {
      font-size: 1.75rem;
      font-weight: 700;
      margin: 0;
    }
    .title-actions button {
      margin-left: 0.5rem;
    }
    .title-underline {
      width: 50px;
      border-top: 3px solid #007bff;
      margin-top: 0.5rem;
      margin-bottom: 1.5rem;
    }
    /* 폼 기본 스타일 */
    .form-label {
      font-weight: 600;
    }
    .form-control {
      padding: 0.75rem 1rem;
      border: 1px solid #ddd;
      border-radius: 4px;
    }
    .form-control[disabled] {
      background-color: #f9f9f9;
    }
    /* 첨부파일 영역 */
    .attachment-section {
      margin-top: 1.5rem;
    }
    .attachment-section h6 {
      font-weight: 600;
      margin-bottom: 0.5rem;
    }
    .attachment-box {
      border: 1px solid #ddd;
      padding: 0.5rem 1rem;
      border-radius: 4px;
      display: flex;
      align-items: center;
    }
    .attachment-box a {
      flex-grow: 1;
      text-decoration: none;
      color: #007bff;
    }
    /* 댓글 영역 헤더 */
    .comment-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-top: 2rem;
    }
    .comment-header h4 {
      margin: 0;
    }
    /* 댓글 등록 폼 */
    #replyForm .row {
      margin-bottom: 1rem;
    }
    /* 댓글 입력 영역: 버튼 위치를 위해 relative 설정 */
    #replyTextArea-container {
      position: relative;
    }
    #replyTextArea-container textarea {
      width: 100%;
      padding: 0.75rem 1rem;
      border: 1px solid #ddd;
      border-radius: 4px;
    }
    #replySubmitBtn {
      position: absolute;
      right: 20px;
      bottom: 10px;
    }
    /* 댓글 목록 스타일 */
    .reply-item {
      border: 1px solid #eee;
      border-radius: 4px;
      padding: 1rem;
      margin-bottom: 1rem;
      position: relative;
    }
    .reply-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 0.5rem;
    }
    .reply-nickname {
      font-size: 1.25rem;
      font-weight: 600;
    }
    .reply-date {
      font-size: 0.8rem;
      color: #888;
    }
    /* 댓글 내용 영역: 수정/삭제 버튼 배치를 위해 relative 처리 */
    .reply-content {
      position: relative;
    }
    /* 댓글 수정/삭제 버튼: 인풋박스 내부 오른쪽 아래에 위치, 버튼 간격 10px */
    .reply-actions {
      position: absolute;
      right: 10px;
      bottom: 5px;
    }
    .reply-actions button {
      margin-left: 10px;
    }
  </style>
</head>
<body>
    <div class="">
        <!-- header-->
        <jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
        <!--// header-------------------------------------------------->
    </div>
  <div id="container" class="container">
    <!-- 메인 영역 -->
    <main class="container my-4">
      <article class="col-md-12">
        <!-- 제목 및 수정/삭제 버튼 -->
        <div class="title-section">
          <h2>${vo.title}</h2>
          <c:if test="${not empty sessionScope.user and (sessionScope.user.isAdmin eq 'Y' or sessionScope.user.nickname eq vo.regId)}">
            <div class="title-actions">
              <input type="button" id="changeBtn" class="btn btn-primary" value="수정">
              <input type="button" id="doDeleteBtn" class="btn btn-primary" value="삭제">
            </div>
          </c:if>
        </div>
        <hr>
        <!-- 게시글 상세 내용 폼 -->
        <form action="#" method="post">
          <input type="hidden" name="boardId" id="boardId" value="${vo.boardId}">
          <input type="hidden" name="div" id="div" value="${vo.getDiv()}">
          <!-- 제목 -->
          <div class="row mb-3">
            <label for="title" class="col-sm-2 col-form-label form-label">제목</label>
            <div class="col-sm-10">
              <input disabled class="form-control" value="${vo.title}" type="text" maxlength="50" name="title" id="title">
            </div>
          </div>
          <!-- 작성자 -->
          <div class="row mb-3">
            <label for="regId" class="col-sm-2 col-form-label form-label">작성자</label>
            <div class="col-sm-10">
              <input disabled class="form-control" value="${vo.regId}" type="text" maxlength="20" name="regId" id="regId">
            </div>
          </div>
          <!-- 작성일 -->
          <div class="row mb-3">
            <label for="regDt" class="col-sm-2 col-form-label form-label">작성일</label>
            <div class="col-sm-10">
              <input disabled class="form-control" value="${vo.regDt}" type="text" maxlength="13" name="regDt" id="regDt">
            </div>
          </div>
          <!-- 내용 -->
          <div class="row mb-3">
            <label for="contentsTextArea" class="col-sm-2 col-form-label form-label">내용</label>
            <div class="col-sm-10">
              <textarea disabled class="form-control" id="contentsTextArea" name="content" placeholder="내용을 입력하세요" rows="6"><c:out value="${vo.content}"/></textarea>
            </div>
          </div>
        </form>
        <!-- 첨부파일 영역 -->
        <div class="attachment-section">
          <h6>첨부파일</h6>
          <c:choose>
            <c:when test="${null != fileName.saveFileName}">
              <div class="attachment-box mt-2">
                <a href="/file/downloadFile.do?fileName=${fileName.saveFileName}" download="${fileName.orgFileName}">
                  ${fileName.orgFileName}
                </a>
                <input disabled hidden="enabled" type="file" name="file" id="file" class="form-control">
                <c:if test="${not empty sessionScope.user and (sessionScope.user.isAdmin eq 'Y' or sessionScope.user.nickname eq vo.regId)}">
                    <button type="button" id="fileDeleteBtn" class="btn btn-danger btn-sm" style="display: none;">삭제</button>
                </c:if>
              </div>
            </c:when>
            <c:otherwise>
              <p>첨부파일이 없습니다.</p>
              <c:if test="${not empty sessionScope.user and (sessionScope.user.isAdmin eq 'Y' or sessionScope.user.nickname eq vo.regId)}">
                <div class="col-12">
                    <label for="file">파일 업로드</label>
                    <input disabled type="file" name="file" id="file" class="form-control">
                    <button hidden="enabled" type="button" id="fileDeleteBtn" class="btn btn-danger btn-sm" style="display: none;">삭제</button>                                                               
                </div>
              </c:if>
            </c:otherwise>
          </c:choose>
        </div>
        <!-- 댓글 영역 (vo.getDiv() == 20일 때만 노출) -->
        <c:choose>
          <c:when test="${20 == vo.getDiv()}">
            <!-- 댓글 헤더 -->
            <div class="comment-header mt-4">
              <h4>댓글</h4>
            </div>
            <hr>
            
            <!-- 댓글 등록 폼 (로그인한 사용자만) -->
            <c:if test="${not empty sessionScope.user}">
              <form action="#" id="replyForm" method="post">
                <!-- 작성자 값 숨김 -->
                <input type="hidden" id="replyAuthor" name="replyAuthor" value="${sessionScope.user.nickname}">
                <!-- 댓글 입력 영역: 레이블 제거 -->
                <div class="row mb-3" id="replyTextArea-container">
                  <div class="col-sm-12" style="position: relative;">
                    <textarea class="form-control" id="replyTextArea" name="replyContent" placeholder="댓글을 입력하세요" rows="3"></textarea>
                    <button type="button" id="replySubmitBtn" class="btn btn-primary">등록</button>
                  </div>
                </div>
              </form>
            </c:if>
            <c:if test="${empty sessionScope.user}">
              <p>댓글 등록은 로그인 후 이용 가능합니다.</p>
            </c:if>
            <!-- 댓글 목록 -->
            <div class="row mb-2">
              <c:choose>
                <c:when test="${replyList.size() > 0}">
                  <c:forEach var="reply" items="${replyList}">
                    <div class="col-12 reply-item">
                      <div class="reply-header">
                        <span class="reply-nickname">${reply.regId}</span>
                        <span class="reply-date">${reply.regDt}</span>
                        <input value="${reply.replyId }" type="hidden" id="replyId">
                      </div>
                      <!-- 댓글 내용 및 수정/삭제 버튼를 인풋박스 내부에 배치 -->
                      <div class="reply-content">
                        <input class="form-control" id="replyContent" value="${reply.replyContent}" disabled type="text">
                        <div class="reply-actions">
                          <c:if test="${not empty sessionScope.user and (sessionScope.user.isAdmin eq 'Y' or sessionScope.user.nickname eq reply.nickname)}">
                            <button type="button" id="replyChangeBtn" class="btn btn-link">수정</button>
                            <button type="button" id="replyDeleteBtn" class="btn btn-link">삭제</button>
                          </c:if>
                        </div>
                      </div>
                    </div>
                  </c:forEach>
                </c:when>
                <c:otherwise>
                  <p class="text-center">댓글이 존재하지 않습니다.</p>
                </c:otherwise>
              </c:choose>
            </div>
          </c:when>
          <c:otherwise>
            <!-- 댓글 영역 미노출 -->
          </c:otherwise>
        </c:choose>
      </article>
    </main>
  </div>
    <div class="container-fluid px-0">
        <!-- footer-->
        <jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
        <!--// footer-------------------------------------------------->
    </div>
  <!-- 기능 스크립트 (변경 없음) -->
  <script src="/assets/js/board_mng.js"></script>
  <script src="/assets/js/reply.js"></script>
  <script src="/assets/js/common.js"></script>
  <script src="/assets/js/bootstrap.min.js"></script>
</body>
</html>
