<%@ page language="java" contentType="text/html; charset=UTF-8"
pageEncoding="UTF-8"%> <%@ taglib prefix="c"
uri="http://java.sun.com/jsp/jstl/core"%> <%@ taglib prefix="fmt"
uri="http://java.sun.com/jsp/jstl/fmt"%>

<!DOCTYPE html>
<html>
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <link href="/assets/css/bootstrap.min.css" rel="stylesheet" />
    <link href="/assets/css/header.css" rel="stylesheet" />
    <link
      rel="stylesheet"
      href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"
    />
    <title>Insert title here</title>
    <script src="/assets/js/jquery_3_7_1.js"></script>
  </head>
  <link rel="icon" href="data:;base64,iVBORw0KGgo=" />

  <body>
    <div id="container" class="container">
      <!-- header-->
      <jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
      <!--// header-------------------------------------------------->

      <!-- main-->
      <main class="container my-4 mx-1">
        <article class="col-md-12"></article>
      </main>
      <!--// main---------------------------------------------------->

      <!-- footer-->
      <jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
      <!--// footer-------------------------------------------------->
    </div>
  </body>
</html>
