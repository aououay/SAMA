<%@ page contentType="text/html;charset=UTF-8" language="java" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/core" prefix="c" %>
<%@ taglib uri="http://java.sun.com/jsp/jstl/functions" prefix="fn" %>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <link href="/assets/css/bootstrap.min.css" rel="stylesheet" >
	<link href="/assets/css/header.css" rel="stylesheet" >
	<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
    
    <title>신호 방법</title>
    <script src="/assets/js/jquery_3_7_1.js"></script>
    <style>
        #sr-container { 
        	font-family: Arial, sans-serif; 
        	line-height: 1.6;
        	padding: 20px; 
        	 }
        h1 { 
        	text-align: left; 
        	padding-bottom: 15px; 
        	margin-bottom: 40px;
        	}
        h2 { 
        	text-align: left; 
        	border-bottom: 2px solid #000; 
        	padding-bottom: 15px; 
        	}
        h3 { 
        	margin-top: 20px; 
        }
        .siren-container { 
	        display: flex; 
	        justify-content: space-between; 
        }
        .left-column { 
        	width: 48%; 
        	}
        .right-column { 
        	width: 48%; 
        }
        .section { 
        	margin-bottom: 30px; 
        }
        .highlight { 
	        color: navy; 
	        font-weight: bold;
         }
        .box { 
	        padding: 10px;
	        background-color: #f8f8f8;
	        border-left: 5px solid navy;
	        margin: 10px 0; 
	        }
        ul { 
       		padding-left: 20px;
		}

	    .siren-button {
	        cursor: pointer;
	        transition: transform 0.1s ease-in-out;
	    }
	    .siren-button:active {
	        transform: scale(0.95);
	    }
            
    </style>
</head>
<body>
<div id="" class="">
	<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
</div>	

<div id="container" class="container">	
	<div id=sr-container class="mt-5">
    <h1>신호방법</h1>


		<div class="section">
		    <h2>경보란</h2>
		    <p>
		        경보란 민방위 사태에 대비하여 인명과 재산의 피해를 최소화 하기 위한 사전적인 신호수단으로서 민방공경보와 재난경보로 구분할 수 있다.
		    </p>
		
		    <h3 class="highlight">민방공 경보</h3>
		    <p>
		        · 적의 침공에 의하여 전국 또는 일부지역에 항공기나 유도탄, 지ㆍ해상병력에 의한 공격이 예상되거나 공격이 있을 경우 또는 화생방에 의한<br> 
		        공격이 있을 경우에 발령하는 경보
		    </p>
		
		    <h3 class="highlight">재난경보</h3>
		    <p>
		        · 「재난 및 안전관리 기본법」에서 정한 바에 따라 호우, 폭설, 태풍, 지진, 해일 등 중대한 재난이 발생되었거나 발생이 예상될 경우에 발령되는 경보
		    </p>
		</div>

	    <div class="section">
	        <h2>민방위 경보의 종류</h2>
	        <h3 class="highlight">민방공 경보</h3>
	        <ul>
	            <li><strong>경계경보</strong> : 화생방무기를 포함한 적의 항공기, 유도탄 또는 지상·해상전력에 의한 공격이 예상 될 때 발령하는 경보</li>
	            <li><strong>공습경보</strong> : 화생방무기를 포함한 적의 항공기, 유도탄 또는 지상·해상전력에 의한 공격이 임박 하거나 공격이 진행중일 때 발령하는 경보</li>
	            <li><strong>화생방경보</strong> :  적의 화생방작용제가 살포되거나 탐지되었을때 또는 화생방 무기의 공격으로 오염이 예상되거나 화생방 공격 확인시에 발령하는 경보</li>
	            <li><strong>핵경보</strong> :  적의 항공기, 유도탄 또는 지상·해상전력에 의한 핵 공격이 임박하거나 공격이 진행 중일 때에 발령하는 경보</li>
	            <li><strong>해제경보</strong> : 화생방무기를 포함한 적의 항공기, 유도탄, 지·해상 전력에 의한 공격 징후 및 추가적인 공격이 예상되지 않을 때</li>
	        </ul>
	
	        <h3 class="highlight">재난경보</h3>
	        <p>· 「재난 및 안전관리 기본법」에서 정한 바에 따라 발령되는 경보</p>
	    </div>

	    <div class="siren-container">
	        <div class="left-column">
	            <div class="section">
	                <h2>민방위경보신호방법</h2>
	
	                <h3 class="highlight">공중파 : 라디오</h3>
	                <div class="box">
	                    <p><strong>민방공 경보</strong></p>
	                    <ul>
	                        <li>경계 : 음성방송</li>
	                        <li>공습 : 사이렌 + 음성방송</li>
	                        <li>화생방 : 음성방송</li>
	                        <li>해 : 사이렌 + 음성방송</li>
	                        <li>해제 : 음성방송</li>
	                    </ul>
	                </div>
					<p>*- 민방위기본법시행령 제47조(응급조치 및 보상), 「민방위경보와 비슷한 음향」 을 제한하거나 금지할 수 있음</p>
	                <h3 class="highlight">공중파 : TV, DMB, CBS</h3>
	                <p>· 문자방송</p>
	
	                <h3 class="highlight">사이렌 장비</h3>
	                <div class="box">
	
	                    <p><strong>민방공 경보</strong>
	                    <img src="assets/images/eun_bi/warning.png"  class="siren-button" alt="경보 이미지" width="25" height="25" onclick="playSiren('siren-audio')">
	                    <audio id="siren-audio">
						    <source src="audio/siren.mp3" type="audio/mpeg">
						</audio>
	                    </p>       
						
		                <ul>
	                        <li>경계 : 음성방송</li>
	                        <li>공습 : 사이렌 (1분, 5초 상승, 3초 하강) + 음성방송</li>
	                        <li>화생방 : 음성방송</li>
	                        <li>해제 : 사이렌 (1분, 5초 상승, 3초 하강) + 음성방송</li>
	                    </ul>
	                </div>
	
	                <h3 class="highlight">단말시설 : 옥내 · 외 방송시설</br> (확성기 등)</h3>
	                <p>- 음성방송 (녹음)</p>
	            </div>
	        </div>
	
	        <div class="right-column">
	            <div class="section">
	                <h2>재난경보
	                <img src="assets/images/eun_bi/warning.png" class="siren-button" alt="경보 이미지" width="25" height="25" onclick="playSiren('disater-audio')">
	                <audio id="disater-audio">
						    <source src="audio/disaster.mp3" type="audio/mpeg">
					</audio>
	                </h2>
						
	                <h3 class="highlight">발령권자</h3>
	                <p>· 「재난 및 안전관리 기본법」에서 정한 바에 따름</p>
	
	                <h3 class="highlight">기타 사이렌음</h3>
	                <p>· 「민방위 경보발령·전달 규정」제18조(민방위사태외의 사이렌장비 활용)</p>
	                <p>· 사이렌음: 평탄음 1분
	                <img src="assets/images/eun_bi/warning.png" class="siren-button" alt="경보 이미지" width="25" height="25" onclick="playSiren('flatSiren-audio')">
	                <audio id="flatSiren-audio">
						    <source src="audio/flatSiren.mp3" type="audio/mpeg">
					</audio>
	                </p>
	                <p>· 활용사례 : 현충일 추념식 등</p>
	            </div>
	        </div>
	    </div>
	</div>
	
</div>




</body>

	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>

	
<script>
    function playSiren(audioId) {
        var audio = document.getElementById(audioId);
        if (audio) {
            audio.play();
        } else {
            console.error("오디오 파일을 찾을 수 없습니다: " + audioId);
        }
    }

</script>
</html>
