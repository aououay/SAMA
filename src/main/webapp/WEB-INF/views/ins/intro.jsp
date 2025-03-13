<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<%@ taglib prefix="fmt" uri="http://java.sun.com/jsp/jstl/fmt"%>

<!DOCTYPE html>
<html>

<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<link href="/assets/css/bootstrap.min.css" rel="stylesheet">
<link href="/assets/css/header.css" rel="stylesheet">
<link rel="stylesheet"
	href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css">
<title>시민안전보험 소개</title>
<script src="/assets/js/jquery_3_7_1.js"></script>
</head>
<link rel="icon" href="data:;base64,iVBORw0KGgo=">
<style>
.wrap-order-provision {
	margin: 30px auto 30px;
	padding: 0 10px;
	overflow: hidden;
}

.order-provision {
	display: flex;
	flex-wrap: wrap;
	justify-content: center;
}

.order-provision .box-provision {
	min-width: 90px;
	padding: 15px;
	border: 1px solid #dddddd;
	border-radius: 14px;
}

.order-provision .box-provision>strong {
	display: flex;
	justify-content: center;
	align-items: center;
	height: 60px;
	padding: 0 10px;
	color: #ffffff;
	font-size: 1.125rem;
	font-weight: 700;
	text-align: center;
	line-height: 1.2;
	border-radius: 6px;
	background: #3774ea;
}

.order-provision .box-provision>div {
	display: flex;
	flex-wrap: wrap;
	justify-content: center;
	align-items: center;
	position: relative;
	height: 60px;
	margin-top: 10px;
	padding: 0 5px;
	color: #666666;
	font-weight: 500;
	text-align: center;
}

.order-provision>li::after {
	content: ">";
	position: absolute;
	top: 50%; /* 수직 중앙 정렬 */
	right: -8px; /* 오른쪽 끝에서 살짝 오른쪽으로 이동 */
	transform: translateY(-50%); /* 수직 중앙 정렬 */
	font-size: 24px; /* 폰트 크기 */
	font-weight: bold; /* 폰트 두께 */
	color: black; /* 기호 색상 */
	display: block;
}

ul, ol {
	list-style: none;
}

.textbox {
	border: 1px solid lightgrey;
	border-radius: 5px;
	padding: 20px;
	margin-bottom: 100px;
}

.order-provision>li {
	position: relative; /* 부모 요소에 relative 추가 */
}

.order-provision>li:last-child::after {
	content: ""; /* > 기호를 없앰 */
}
</style>

<body>

	<div class="">
		<!-- header-->
		<jsp:include page="/WEB-INF/views/template/header.jsp"></jsp:include>
		<!--// header-------------------------------------------------->
	</div>

	<div id="container" class="container">


		<main class="container my-4 mx-1">
			<article class="col-md-12">


				<!-- 페이지 제목 -->
				<h2 class="text-center mb-4">시민안전보험 소개</h2>

				<div class="d-flex justify-content-between align-items-center">
					<p class="lead mb-0">시민안전보험 정보를 소개합니다.</p>
				</div>
				<hr>

				<h4 style="margin-top: 48px;">시민안전보험이란?</h4>
				<div class="textbox">
					<p>
						재난 · 사고로 인한 시 · 도민의 생명 · 신체 피해를 보상하고자 지자체가 자율적으로 보험사 · 공제회와 가입 계약한
						보장 제도로 지자체 보험 가입 시, 해당 지자체에 주소를 둔 시 · 도민은 별도 절차없이 일괄 가입됩니다. 보험료를
						관할지자체가 부담함으로써 일상생활 중 예기치 못한 사고들을 당했을 경우 피해자가 일상으로 복귀할수 있도록 지원하는
						제도입니다. <br> <br> <span
							class="badge rounded-pill bg-danger" style="font-size: 0.9em;">!
							지방자치단체별로 보장항목 및 보장규모에 차이가 있으니 주소지의 안전보험 가입정보를 꼭 확인하세요 !</span>
					</p>
				</div>

				<h4>보험금 청구 절차</h4>

				<div class="wrap-order-provision" style="padding: 0px;">
					<ol class="order-provision">
						<li class="order0" style="flex: 0 0 16%; padding: 10px 10px;">
							<div class="box-provision">
								<strong>피보험자</strong>
								<div>사고발생</div>
							</div>
						</li>
						<li class="order1" style="flex: 0 0 16%; padding: 10px 10px;">
							<div class="box-provision">
								<strong style="font-size: 15px;">시민안전보험 조회<br />(주소 등록
									지자체 선택)
								</strong>
								<div>보장항목 및 보장내역 확인</div>
							</div>
						</li>
						<li class="order2" style="flex: 0 0 16%; padding: 10px 10px;">
							<div class="box-provision">
								<strong>필요서류 확인</strong>
								<div>
									주소 등록 지자체<br />시민안전 홈페이지<br />또는 보험사 문의
								</div>
							</div>
						</li>
						<li class="order3" style="flex: 0 0 16%; padding: 10px 10px;">
							<div class="box-provision">
								<strong>보험금 신청</strong>
								<div>청구서 및 필요서류 접수</div>
							</div>
						</li>
						<li class="order4" style="flex: 0 0 16%; padding: 10px 10px;">
							<div class="box-provision">
								<strong>청구내용 심사</strong>
								<div>서류심사</div>
							</div>
						</li>
						<li class="order5" style="flex: 0 0 16%; padding: 10px 10px;">
							<div class="box-provision">
								<strong>보험금 지급</strong>
								<div>
									<span>보험금 지급 <i>(통장지급)</i></span>
								</div>
							</div>
						</li>
					</ol>
				</div>

				<br> <br>

				<h4>자주 묻는 질문</h4>
				<div class="accordion">
					<div class="accordion-item">
						<h2 class="accordion-header">
							<button class="accordion-button" type="button"
								data-bs-toggle="collapse"
								data-bs-target="#panelsStayOpen-collapseOne"
								aria-expanded="true" aria-controls="panelsStayOpen-collapseOne">
								<strong>시민안전보험 가입대상은 누구인가요?</strong>
							</button>
						</h2>
						<div id="panelsStayOpen-collapseOne"
							class="accordion-collapse collapse show">
							<div class="accordion-body">주소지의 지방자치단체에서 시민안전보험에 가입하여 운영
								중인 경우 해당 지자체에 주소를 두고 있는 시민(등록외국인 포함)은 자동으로 시민안전보험에 가입됩니다. 단,
								지방자치단체별로 가입하고 있는 보장항목과 보상한도가 상이하니 자세한 내용은 해당 지방자치단체의 시민안전보험
								가입내용을 확인하시길 바랍니다.</div>
						</div>
					</div>
					<div class="accordion-item">
						<h2 class="accordion-header">
							<button class="accordion-button collapsed" type="button"
								data-bs-toggle="collapse"
								data-bs-target="#panelsStayOpen-collapseTwo"
								aria-expanded="false" aria-controls="panelsStayOpen-collapseTwo">
								<strong>주소지가 아닌 타지역에서 피해를 입었는데 이러한 경우도 보상받을 수 있나요?</strong>
							</button>
						</h2>
						<div id="panelsStayOpen-collapseTwo"
							class="accordion-collapse collapse">
							<div class="accordion-body">주소지의 지방자치단체가 가입한 시민안전보험 약관상에
								타지역에서 발생한 사고로 인한 피해까지도 보장 범위에 포함하고 있는 경우 보상이 가능합니다. 자세한 내용은 약관
								또는 지방자치단체 담당부서에 확인하시길 바랍니다.</div>
						</div>
					</div>
					<div class="accordion-item">
						<h2 class="accordion-header">
							<button class="accordion-button collapsed" type="button"
								data-bs-toggle="collapse"
								data-bs-target="#panelsStayOpen-collapseThree"
								aria-expanded="false"
								aria-controls="panelsStayOpen-collapseThree">
								<strong>자연재난으로 인하여 피해를 입었을 경우 재난지원금과 시민안전보험에 의한 보험금을
									중복하여 받을 수 있는 건가요?</strong>
							</button>
						</h2>
						<div id="panelsStayOpen-collapseThree"
							class="accordion-collapse collapse">
							<div class="accordion-body">재난지원금과 시민안전보험에 의한 보험금은 중복하여 수령할
								수 있습니다.</div>
						</div>
					</div>
					<div class="accordion-item">
						<h2 class="accordion-header">
							<button class="accordion-button collapsed" type="button"
								data-bs-toggle="collapse"
								data-bs-target="#panelsStayOpen-collapseFour"
								aria-expanded="false"
								aria-controls="panelsStayOpen-collapseFour">
								<strong>시민안전보험에 따른 보험금 청구 기한은 언제까지인가요?</strong>
							</button>
						</h2>
						<div id="panelsStayOpen-collapseFour"
							class="accordion-collapse collapse">
							<div class="accordion-body">사고사실을 안 날로부터 3년 이내에 청구해야 합니다.</div>
						</div>
					</div>
					<div class="accordion-item">
						<h2 class="accordion-header">
							<button class="accordion-button collapsed" type="button"
								data-bs-toggle="collapse"
								data-bs-target="#panelsStayOpen-collapseFive"
								aria-expanded="false"
								aria-controls="panelsStayOpen-collapseFive">
								<strong>15세 미만자의 경우 사망으로 인한 보험금 지급이 제한되는 이유는 무엇인가요?</strong>
							</button>
						</h2>
						<div id="panelsStayOpen-collapseFive"
							class="accordion-collapse collapse">
							<div class="accordion-body">「상법」 제732조(15세미만자등에 대한 계약의 금지)
								조항에 따라 15세 미만자를 대상으로 하는 사망담보의 보험계약 자체가 ‘무효’이기 때문에, 15세 미만자의 사망에
								대해서는 보상이 이루어질 수 없습니다.</div>
						</div>
					</div>
				</div>

				<br>
				<!-- 버튼 -->
				<div class="text-center mb-4">
					<a class="btn btn-primary" href="/ins/getIns.do">시민안전보험 조회하기</a>
				</div>

			</article>
		</main>
		<!--// main-->


	</div>
	<div class="container-fluid px-0">
		<!-- footer-->
		<jsp:include page="/WEB-INF/views/template/footer.jsp"></jsp:include>
		<!--// footer-------------------------------------------------->
	</div>
	<!-- 사용자 정의 스크립트 -->
	<script src="/assets/js/pcwk.js"></script>
	<!-- Bootstrap JS -->
	<!-- <script src="/assets/js/bootstrap.min.js"></script> -->
</body>
</html>
