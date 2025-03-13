<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>

<html>

<head>
    <title>서울시 안전지수</title>

    <link rel="stylesheet" type="text/css" href="/assets/css/bootstrap.min.css">
    <link rel="stylesheet" type="text/css" href="/assets/css/disastermessage_index.css">
<link href="/assets/css/header.css" rel="stylesheet">
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>

    <style>
    </style>
</head>
    <div class="">
        <!-- header-->
        <jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
        <!--// header-------------------------------------------------->
    </div>
    
    
<body>
    
    <br>
    
    <div class="container">


<!-- 안전지수 정보 및 지역 선택을 가로로 배치 -->
<div class="safety-wrapper">
    <!-- 안전지수 정보 -->
    <div class="safety-info-container">
        <div class="safety-index">
            재난 빈도수 * 가중치 : <strong><c:out value="${safetyIndex}" default="-" /></strong>
        </div>
        <div class="safety-index">
            양수량 : <strong><c:out value="${safetyIndex_EWSF}" default="-" /> ton</strong>
        </div>
        <div class="safety-index">
            안전지수 : <strong id="safetyIndexValue">${safetyIndex_EWSF - safetyIndex}</strong>
        </div>
        <div id="safetyImageContainer" class="safety-image"></div>
    </div>

    <!-- 지역 선택 버튼 -->
    <div class="region-select-wrapper">
        <button id="regionSelectBtn" class="region-select-btn">
            <c:out value="${searchWord}" default="지역 선택" />
        </button>
    </div>
</div>



        <!-- 차트를 가로 정렬 -->
        <div class="charts-wrapper">
            <div class="chart-container">
                <canvas id="disasterFrequency"></canvas>
            </div>
            <div class="chart-container">
                <canvas id="safetyChart"></canvas>
            </div>
        </div>
    </div>

    <!-- 지역 선택 모달 -->
    <div id="overlay" class="overlay"></div>

    <div id="regionModal" class="modal">
        <h3>지역 선택</h3>
        <div class="region-box">
            <button data-value="강남">강남</button>
            <button data-value="강동">강동</button>
            <button data-value="강서">강서</button>
            <button data-value="강북">강북</button>
            <button data-value="관악">관악</button>
            <button data-value="광진">광진</button>
            <button data-value="구로">구로</button>
            <button data-value="금천">금천</button>
            <button data-value="노원">노원</button>
            <button data-value="도봉">도봉</button>
            <button data-value="동대문">동대문</button>
            <button data-value="동작">동작</button>
            <button data-value="마포">마포</button>
            <button data-value="서대문">서대문</button>
            <button data-value="서초">서초</button>
            <button data-value="성동">성동</button>
            <button data-value="성북">성북</button>
            <button data-value="송파">송파</button>
            <button data-value="양천">양천</button>
            <button data-value="영등포">영등포</button>
            <button data-value="용산">용산</button>
            <button data-value="은평">은평</button>
            <button data-value="종로">종로</button>
            <button data-value="중구">중구</button>
            <button data-value="중랑">중랑</button>
        </div>
        <div class="modal-footer">
            <button class="close-btn" id="closeModal">닫기</button>
            <button class="complete-btn" id="completeSelection">완료</button>
        </div>
    </div>

    <form id="regionForm" method="GET" action="/disastermessage/safety_Index">
        <input type="hidden" id="selectedRegion" name="searchWord" value="${searchWord}">
    </form>
</body>

<div class="container-fluid px-0">
    <!-- footer-->
    <jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
    <!--// footer-------------------------------------------------->
</div>
<!-- JavaScript -->
<script>
    document.addEventListener("DOMContentLoaded", function () {
        var regionSelectBtn = document.getElementById("regionSelectBtn");
        var overlay = document.getElementById("overlay");
        var regionModal = document.getElementById("regionModal");
        var closeModal = document.getElementById("closeModal");
        var completeSelection = document.getElementById("completeSelection");
        var selectedRegionInput = document.getElementById("selectedRegion");
        var selectedRegion = "${searchWord}";

        // 모달 열기
        regionSelectBtn.addEventListener("click", function () {
            overlay.style.display = "block";
            regionModal.style.display = "block";
        });

        // 지역 선택
        document.querySelectorAll(".region-box button").forEach(function (button) {
            button.addEventListener("click", function () {
                document.querySelectorAll(".region-box button").forEach(btn => btn.classList.remove("selected"));
                button.classList.add("selected");
                selectedRegion = button.getAttribute("data-value");
            });
        });

        // 완료 버튼 클릭 시
        completeSelection.addEventListener("click", function () {
            if (selectedRegion) {
                selectedRegionInput.value = selectedRegion;
                document.getElementById("regionForm").submit(); // 폼 제출
            }
        });

        // 닫기 버튼 클릭 시
        closeModal.addEventListener("click", function () {
            overlay.style.display = "none";
            regionModal.style.display = "none";
        });

        // 배경 클릭 시 모달 닫기
        overlay.addEventListener("click", function () {
            overlay.style.display = "none";
            regionModal.style.display = "none";
        });
    });

    document.addEventListener("DOMContentLoaded", function () {
        // JSP 변수 가져오기 (없을 경우 0 처리)
        let disasterFrequency = ${safetyIndex != null ? safetyIndex : 0}; // 재난 빈도수
        let waterVolume = ${safetyIndex_EWSF != null ? safetyIndex_EWSF : 0}; // 양수량

        // 차트 초기화
        const ctx = document.getElementById('safetyChart').getContext('2d');

        let safetyChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: [
                    '재난 빈도수', '양수량'], // X축 레이블
                datasets: [
                    {
                        label: '재난 빈도수',
                        data: [disasterFrequency, 0], // 첫 번째 데이터
                        backgroundColor: 'rgba(255, 99, 132, 0.2)',
                        borderColor: 'rgba(255, 99, 132, 1)',
                        borderWidth: 1,
                    },
                    {
                        label: '양수량',
                        data: [0, waterVolume], // 두 번째 데이터
                        backgroundColor: 'rgba(54, 162, 235, 0.2)',
                        borderColor: 'rgba(54, 162, 235, 1)',
                        borderWidth: 1,
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false, // 크기 조정 가능하게 설정
                scales: {
                    x: {
                        ticks: {
                            font: {
                                size: 14
                            },
                            padding: 20 // X축 레이블과 막대 사이의 간격 설정
                        },
                        grid: {
                            display: false // X축 그리드 숨기기
                        },
                        barPercentage: 0.6, // 막대 너비 조정
                        categoryPercentage: 0.6 // 카테고리 내 정렬 조정
                    },
                    y: {
                        title: {
                            display: true,
                            text: '값 (단위: 빈도수/㎥)',
                            font: {
                                size: 14
                            }
                        },
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        usePointStyle: true,
                        padding: 100,
                        display: true,
                        position: 'top' // 범례 위치 상단
                    }
                }
            }
        });
    });

    document.addEventListener("DOMContentLoaded", function () {
        //JSP 변수 가져오기 (없을 경우 0 처리)
        let disasterFrequency = ${safetyIndex != null ? safetyIndex : 0}; // 재난 빈도수item_count 객체 받아오기

        //list 타입 데이터이기때문에 index를 지정해 주어야 함
        let itemCount = {
            dust_count: parseInt(${item_count[0].dustCount != null ? item_count[0].dustCount : 0}),
            heat_wave_count: parseInt(${item_count[0].heatWaveCount != null ? item_count[0].heatWaveCount : 0}),
            heavy_rain_count: parseInt(${item_count[0].heavyRainCount != null ? item_count[0].heavyRainCount : 0}),
            cold_wave_count: parseInt(${item_count[0].coldWaveCount != null ? item_count[0].coldWaveCount : 0}),
            heavy_snow_count: parseInt(${item_count[0].heavySnowCount != null ? item_count[0].heavySnowCount : 0}),
            wildfire_count: parseInt(${item_count[0].wildfireCount != null ? item_count[0].wildfireCount : 0}),
            storm_count: parseInt(${item_count[0].stormCount != null ? item_count[0].stormCount : 0}),
            epidemic_count: parseInt(${item_count[0].epidemicCount != null ? item_count[0].epidemicCount : 0}),
            landslide_count: parseInt(${item_count[0].landslideCount != null ? item_count[0].landslideCount : 0})
        };

        console.log(itemCount); // itemCount 확인하기
        // item_count 데이터를 필터링하여 0이 아닌 값만 포함한 배열 생성
        let labels = [];
        let counts = [];

        // 영어에서 한글로 매핑하기 위한 객체
        const labelMapping = {
            dust_count: '먼지',
            heat_wave_count: '폭염',
            heavy_rain_count: '호우',
            cold_wave_count: '한파',
            heavy_snow_count: '폭설',
            wildfire_count: '산불',
            storm_count: '폭풍',
            epidemic_count: '전염병',
            landslide_count: '산사태'
        };

        for (const [key, value] of Object.entries(itemCount)) {
            if (value > 0) {
                labels.push(labelMapping[key] || key); // 한글로 변환된 레이블 또는 기본 키 사용
                counts.push(value);
            }
        }

        // 차트 초기화
        const ctx = document.getElementById('disasterFrequency').getContext('2d');

        let disasterTypeChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: '재난 유형별 빈도수',
                    data: counts, // 필터링된 카운트 데이터
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    borderColor: 'rgba(75, 192, 192, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        ticks: {
                            font: {
                                size: 12
                            },
                            padding: 15
                        },
                        grid: {
                            display: false
                        }
                    },
                    y: {
                        title: {
                            display: true,
                            text: '빈도수',
                            font: {
                                size: 14
                            }
                        },
                        beginAtZero: true
                    }
                },
                plugins: {
                    legend: {
                        display: true,
                        position: 'top'
                    }
                }
            }
        });
    });

    // 이미지 URL 결정 로직
    let safetyIndexValue = ${safetyIndex_EWSF - safetyIndex}; // 안전지수 계산

    // 이미지 결정
    let imageUrl = '';
    if (safetyIndexValue > 50000) {
        imageUrl = "/assets/images/yeong-chang/매우좋음.png";
    } else if (safetyIndexValue > 10000) {
        imageUrl = '/assets/images/yeong-chang/좋음.png';		
    } else if (safetyIndexValue < -50000) {
        imageUrl = '/assets/images/yeong-chang/매우나쁨.png';
    } else if (safetyIndexValue < 10000) {
        imageUrl = '/assets/images/yeong-chang/나쁨.png';
    }

    // 이미지 삽입
    const safetyImageContainer = document.getElementById('safetyImageContainer');
    safetyImageContainer.setAttribute("data-image-url", imageUrl);  // 데이터 속성에 이미지 URL 삽입

    // data-image-url 값 읽어오기
    const imageUrlFromContainer = safetyImageContainer.getAttribute("data-image-url");

    // 이미지 삽입
    if (imageUrlFromContainer) {
        safetyImageContainer.innerHTML = '<img src="' + imageUrlFromContainer + '" alt="Safety Image" style="max-width: 100%; height: auto;">';
    }

</script>
</html>
