<%@ page contentType="text/html;charset=UTF-8" language="java"%>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c"%>
<html>
<head>
<title>영상 업로드</title>
<link rel="stylesheet" type="text/css"
	href="/assets/css/videoupload.css">
<script>
	function validateForm() {
		var title = document.getElementById("videoTitle").value;
		var videoUrl = document.getElementById("videoUrl").value;
		var category = document.getElementById("videoCategory").value; // 카테고리 값 추가

		if (title.trim() === "" || videoUrl.trim() === "") {
			alert("영상 제목과 영상 URL을 입력하세요.");
			return false;
		}

		if (category.trim() === "") {
			alert("카테고리를 선택하세요.");
			return false;
		}

		return true;
	}
</script>
</head>
<body>
	<div id="container">
		<h2>영상 업로드</h2>

		<form action="<c:url value='/videopost/upload' />" method="post"
			onsubmit="return validateForm();">
			<table class="upload-table">
				<tr>
					<th><label for="videoTitle">제목</label></th>
					<td><input type="text" id="videoTitle" name="videoTitle"
						required></td>
				</tr>
				<tr>
					<th><label for="videoUrl">유튜브 주소</label></th>
					<td><span>https://youtu.be/</span> <input type="text"
						id="videoUrl" name="videoUrl" required></td>
				</tr>
				<tr>
					<th><label for="thumbnailUrl">대표 이미지 URL</label></th>
					<td><input type="text" id="thumbnailUrl" name="thumbnailUrl"
						placeholder="https://example.com/image.jpg">
						<p class="desc">* 유튜브 썸네일 URL 또는 직접 업로드한 이미지의 URL을 입력하세요.</p></td>
				</tr>
				<tr>
					<th><label for="videoDescription">영상소개</label></th>
					<td><textarea id="videoDescription" name="videoDescription"></textarea></td>
				</tr>
				<tr>
					<th><label for="videoCategory">카테고리</label></th>
					<td><select id="videoCategory" name="videoCategory" required>
							<option value="">카테고리 선택</option>
							<option value="HURRICANE">태풍</option>
							<option value="FLOOD">홍수</option>
							<option value="DOWNPOUR">호우</option>
							<option value="STORM">해일</option>
							<option value="HEAVY_SNOW">대설</option>
							<option value="COLD_WAVE">한파</option>
							<option value="HEAT_WAVE">폭염</option>
							<option value="EARTHQUAKE">지진</option>
							<option value="OTHER">기타</option>
					</select></td>
				</tr>

			</table>

			<div class="btn-wrap">
				<button type="submit" class="btn-submit">등록</button>
				<a href="<c:url value='/videopost/list' />" class="btn-list">목록</a>
			</div>
		</form>

		<br>
		<c:if test="${not empty message}">
			<p class="message">${message}</p>
		</c:if>
	</div>

	<script src="/assets/js/videopost/upload.js"></script>
</body>
</html>
