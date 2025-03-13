<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>체크리스트 이미지 다운로드</title>
</head>
<body>
    <h1>체크리스트 이미지 다운로드</h1>

    <!-- 체크된 항목들 표시 -->
    <p>선택된 항목들: ${checkedItems}</p>

    <!-- 이미지 다운로드 링크 -->
    <a href="${imagePath}" download="checkedItems.jpg">이미지 다운로드</a>
</body>
</html>
