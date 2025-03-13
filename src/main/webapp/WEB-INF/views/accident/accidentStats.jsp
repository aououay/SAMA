<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<html>
<head>
	<meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="/assets/css/bootstrap.min.css" rel="stylesheet" >
	<link href="/assets/css/header.css" rel="stylesheet" >
	<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
    
    <title>사고 통계</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <script src="/assets/js/jquery_3_7_1.js"></script>
    <link rel="icon" href="data:;base64,iVBORw0KGgo=">
</head>
<style>
	.title {
		text-align: center;
		margin-top: 50px;
	}
	
    .chart-container {
        display: flex;
        justify-content: center;
        align-items: center;
        height: 600px;  
    }
    
    .head {
    	margin-left: 200px;
    	margin-top: 50px;
    }
    
    .accident-header {
        display: flex;
        justify-content: center;
        align-items: center;
        margin: 20px 0px;
        margin-top: 50px;
        
    }

    .accident-tab {
        display: flex;
        background: #f4f4f4;
        border-radius: 50px;
        padding: 10px;
        width: 1000px; 
        justify-content: space-between; 
        align-items: center;
        font-size: 18px;
        font-weight: bold;
    }
    
    .tab-item {
        flex: 1;
        text-align: center;
        padding: 10px 20px;
        border-radius: 50px;
        margin: 0 15px; 
    }
</style>


<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

	<div id="container" class="container">
	
		<div>
			<h1 class="title">주요 재난 통계</h1>
		</div>
	
		<div class="accident-header">
		    <div class="accident-tab">
		        <div class="tab-item">공사장</div>
		        <div class="tab-item">화재</div>
		        <div class="tab-item">교통</div>
		    </div>
		</div>
	
	    <h2 class="head">주간 사고 통계 (최근 8주)</h2>
	    <div class="chart-container">
	    	<canvas id="weeklyChart" width="800" height="500"></canvas>
		</div>
	
	    <h2 class="head">월별 사고 통계 (최근 8개월)</h2>
	    <div class="chart-container">
	    	<canvas id="monthlyChart" width="800" height="500"></canvas>
		</div>
	
		<h2 class="head">전년 동월 대비 사고 통계</h2>
		<div class="chart-container">
		    <canvas id="yearlyComparisonChart" width="800" height="500"></canvas>
		</div>
		


    </div>
   
</body>
	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>
 <script>  		
    //주간 통계
	async function fetchWeeklyStats() {
            const response = await fetch('/accident-stats/data');
            const data = await response.json();

            const labels = data.map(item => item.statsCrtrStaDt+' ~ '+item.statsCrtrEndDt);
            const accidentCounts = data.map(item => item.acdntCnt);
            const deathCounts = data.map(item => item.dcsdCnt);

            const ctx = document.getElementById('weeklyChart').getContext('2d');
            const weeklyChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels.reverse(),
                    datasets: [
                        {
                            label: '사고 건수',
                            data: accidentCounts.reverse(),
                            borderColor: '#424242',
                            backgroundColor: 'rgba(0, 0, 0, 0.8)',
                            type: 'line',
                            fill: false
                        },
                        {
                            label: '사망자 + 부상자 수',
                            data: deathCounts.reverse(),
                            backgroundColor: '#fe6c5c',
                        }
                    ]
                },
                options: {
                    responsive: false,  // 반응형 크기 조정 해제
                    maintainAspectRatio: false,  // 가로세로 비율 유지 안함
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        }
	//월간 통계
	async function fetchMonthlyStats() {
            const response = await fetch('/accident-stats/monthly-data');
            const data = await response.json();

            const labels = data.map(item => item.month);
            const accidentCounts = data.map(item => item.totalAcdntCnt);
            const deathCounts = data.map(item => item.totalDcsdCnt);

            const ctx = document.getElementById('monthlyChart').getContext('2d');
            const monthlyChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels.reverse(),
                    datasets: [
                        {
                            label: '사고 건수',
                            data: accidentCounts.reverse(),
                            borderColor: '#424242',
                            backgroundColor: 'rgba(0, 0, 0, 0.8)',
                            type: 'line',
                            fill: false
                        },
                        {
                            label: '사망자 + 부상자 수',
                            data: deathCounts.reverse(),
                            backgroundColor: '#f96029',
                        }
                    ]
                },
                options: {
                    responsive: false,  // 반응형 크기 조정 해제
                    maintainAspectRatio: false,  // 가로세로 비율 유지 안함
                    scales: {
                        y: {
                            beginAtZero: true
                        }
                    }
                }
            });
        }
    //전년 동월
	async function fetchYearlyComparisonStats() {
            const response = await fetch('/accident-stats/yearly-comparison');
            const data = await response.json();

            if (!data || data.length === 0) {
                console.error("전년 동월 대비 데이터 없음");
                return;
            }
			
            const labels = ['발생 건수', '사망자 + 부상자 수'];
            const currentYearData = [data.totalAcdntCnt || 0, data.totalDcsdCnt || 0];
            const prevYearData = [data.prevTotalAcdntCnt || 0, data.prevTotalDcsdCnt || 0];

            const ctx = document.getElementById('yearlyComparisonChart').getContext('2d');
            new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [                       
                        {
                            label: '전년도',
                            data: prevYearData,
                            backgroundColor: '#fc512a'
                        },
                        {
                            label: '올해',
                            data: currentYearData,
                            backgroundColor: '#1bb9fc'
                        }
                    ]
                },
                    options: {
                        responsive: false,  // 반응형 크기 조정 해제
                        maintainAspectRatio: false,  // 가로세로 비율 유지 안함
                        scales: {
                            y: {
                                beginAtZero: true
                            }
                        }
                    }
                });
            } 
        
        fetchYearlyComparisonStats();
        fetchWeeklyStats();
        fetchMonthlyStats();
    </script>
</html>
