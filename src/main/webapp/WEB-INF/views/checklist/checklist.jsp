<%@ page contentType="text/html;charset=UTF-8" language="java"%>
<!DOCTYPE html>
<html lang="ko">

<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>재난 대피 물품 체크리스트</title>
    
    <link href="/assets/css/bootstrap.min.css" rel="stylesheet">
    <link href="/assets/css/header.css" rel="stylesheet">
    <link href="/assets/css/checklist.css" rel="stylesheet">
    
    <script src="/assets/js/jquery_3_7_1.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/html2canvas/1.4.1/html2canvas.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    
    <script>
        document.addEventListener("DOMContentLoaded", function() {
            var triggerTabList = document.querySelectorAll('[data-bs-toggle="tab"]');
            triggerTabList.forEach(function(tab) {
                tab.addEventListener("click", function(event) {
                    event.preventDefault();
                    let tabInstance = new bootstrap.Tab(tab);
                    tabInstance.show();
                });
            });
        });

        function toggleExtraItems(category, sectionId, button) {
            var section = document.getElementById(sectionId);
            var items = section.querySelectorAll("[data-category='" + category + "']");
            var isActive = button.classList.contains("active");

            // 리스트 항목 보이기 / 숨기기
            items.forEach(function(item) {
                item.style.display = isActive ? "none" : "block";
            });

            // 버튼 스타일 변경
            button.classList.toggle("active");
        }

        function showDownloadButton(tabId) {
            let downloadBtn = document.getElementById("download-btn");
            if (!downloadBtn) return;
            downloadBtn.style.display = tabId === "checklist" ? "block" : "none";
        }

        function downloadChecklist() {
            let checklistContent = document.getElementById("checklist-content");
            html2canvas(checklistContent, { scale: 2 }).then(canvas => {
                let image = canvas.toDataURL("image/png");
                let link = document.createElement("a");
                link.href = image;
                link.download = "checklist.png";
                link.click();
            });
        }
    </script>
</head>


	
<body class="checklist-body">

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

    <div class="checklist-container mt-5 p-4 border rounded">
        <h1 class="checklist-title">재난 대피 물품 안내</h1>

        <!-- 네비게이션 탭 -->
        <ul class="nav checklist-nav-tabs mt-4" id="myTab" role="tablist">
            <li class="nav-item">
                <a class="nav-link active" id="carry-tab" data-bs-toggle="tab" href="#carry" role="tab" onclick="showDownloadButton('carry')">손에 들고 가야할 것</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" id="store-tab" data-bs-toggle="tab" href="#store" role="tab" onclick="showDownloadButton('store')">집에 비치할 것</a>
            </li>
            <li class="nav-item">
                <a class="nav-link" id="checklist-tab" data-bs-toggle="tab" href="#checklist" role="tab" onclick="showDownloadButton('checklist')">체크리스트</a>
            </li>
        </ul>

        <div class="tab-content mt-3" id="myTabContent">
<!-- 손에 들고 가야할 것 -->
<div class="tab-pane fade show active" id="carry" role="tabpanel">
<br>
    <h4>손에 들고 가야할 것</h4>
    <p>모든 가족 구성원들은 반드시 비상용 백(Go Bag)을 준비해 두어야 합니다. 비상용 백에는 대피시에 필요한 물품들을 넣어두십시오.<br> 각 가족 구성원의 비상용 백을 배낭이나 바퀴달린 여행용 가방처럼 튼튼하고 휴대가 편리한 가방에 넣어두십시오. <br>비상용 백은 집에서 나갈 때 쉽게 가져갈 수 있어야 합니다. 연중 언제나 반드시 준비해 두십시오.</p>
<br>
    <!-- 조건 선택 버튼 -->
    <div class="mb-3">
        <button class="btn checklist-btn-outline-primary me-2" onclick="toggleExtraItems('pet', 'carry', this)">반려동물</button>
        <button class="btn checklist-btn-outline-primary me-2" onclick="toggleExtraItems('elderly', 'carry', this)">노약자</button>
        <button class="btn checklist-btn-outline-primary" onclick="toggleExtraItems('baby', 'carry', this)">아기</button>
    </div>

    <ul class="checklist-list">
        <li>긴급구호 물품</li>
        <li>손전등 및 배터리</li>
        <li>개인 신분증</li>
        <li>응급처치 키트</li>
    </ul>

    <!-- 추가 아이템 -->
    <ul class="checklist-extra-items" data-category="pet" style="display: none;">
        <li><b>[반려동물]</b></li>
        <li> 반려동물 사료 및 식수</li>
        <li>이동장 및 목줄</li>
        <li>예방접종 기록</li>
    </ul>
    <ul class="checklist-extra-items" data-category="elderly" style="display: none;">
        <li><b>[노약자]</b></li>
        <li>상비약 및 처방약</li>
        <li>보청기 및 배터리</li>
        <li>지팡이 또는 보조기구</li>
    </ul>
    <ul class="checklist-extra-items" data-category="baby" style="display: none;">
        <li><b>[아기]</b></li>
        <li> 분유 및 이유식</li>
        <li>기저귀 및 물티슈</li>
        <li>아기용 방수 담요</li>
    </ul>

    <!-- 손에 들고 가야할 것 이미지 -->
    <div class="text-center mt-4">
        <img src="/assets/images/손에들고가야할것.png" class="checklist-img" alt="손에 들고 가야할 것">
    </div>
</div>

<!-- 집에 비치할 것 -->
<div class="tab-pane fade" id="store" role="tabpanel">
<br>
    <h4>집에 비치할 것</h4>
    <a>최소 3일동안 자립적으로 생존하기에 충분한 생필품을 가정해 비치해 두십시오. <br>가급적 이 품목들을 찾기 쉬운 별도의 용기나 특별한 찬장에 보관하십시오.<br></a>
     <a><b>이 생필품은 비상용이라는 점을 가족들에게 알려주어야 합니다.</b></a><br><br>
    <ul class="checklist-list">
        <li>비상 식량 및 물</li>
        <a>· 식량은 가구별로 15일 ~ 30일 분 정도를 준비해야한다.<br>
· 가공식품.<br>
· 라면, 통조림 등 적정 소요량.<br>
· 정부는 양곡을 충분히 비축하고 있으며, 필요시에는 식량 배급제를 실시하므로 지나친 사재기를 하지 맙시다.<br><br></a>
        <li>의약품</li>
        <li>라디오 및 비상 배터리</li>
        <li>비상용 랜턴</li>
    </ul>

    <!-- 집에 비치할 것 이미지 -->
    <div class="text-center mt-4">
        <img src="/assets/images/집에비치할것.png" class="checklist-img" alt="집에 비치할 것">
    </div>
</div>


            <!-- 체크리스트 -->
            <div class="tab-pane fade" id="checklist" role="tabpanel">
            <br>
                <h4>체크리스트</h4>
                <div id="checklist-content" class="p-3 border rounded">
                    <ul class="checklist-list">
                        <li><input type="checkbox"> 비상식량 및 물</li>
                        <li><input type="checkbox"> 손전등 및 건전지</li>
                        <li><input type="checkbox"> 성냥 및 라이터</li>
                        <li><input type="checkbox"> 비상 의류 및 속옷</li>
                        <li><input type="checkbox"> 응급처치 키트</li>
                    </ul>
                </div>
            </div>
        </div>

        <div class="text-center mt-3">
            <button id="download-btn" class="btn btn-primary" style="display: none;" onclick="downloadChecklist()">체크리스트 다운로드</button>
        </div>
    </div>
</body>
	<div class="container-fluid px-0">
	<!-- footer-->
	<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
	<!--// footer-------------------------------------------------->
</html>
