<%@ page language="java" contentType="text/html; charset=UTF-8"
	pageEncoding="UTF-8"%>
<%@ taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>

<link rel="stylesheet" href="/assets/css/footer.css">

<footer id="footer">
	<!-- 상단 주요 카테고리 -->
	<div class="footer-top">
		<ul>
			<li><a href="${CP}/waterlevel/view">홍수</a></li>
			<li><a href="${CP}/typ/view">태풍</a></li>
			<li><a href="${CP}/weather">날씨</a></li>
			<li><a href="${CP}/air_quality">대기오염</a></li>
			<li><a href="${CP}/faclt/view">비상대피시설</a></li>
			<li><a href="${CP}/disasterguide/finedust">위기대응</a></li>
			<li><a href="${CP}/er_medi_inst/list">응급의료기관</a></li>
			<li><a href="${CP}/ins/intro.do">시민안전보험</a></li>
			<li><a href="${CP}/board/doRetrieve.do?div=10">공지사항</a></li>
		</ul>
	</div>

	<hr class="footer-line" />

	<!-- 하단 정보 -->
	<div class="footer-bottom">
		<div class="footer-links">
			<a href="#">개인정보처리방침</a> <a href="#">이용약관</a> <a href="#">사이트맵</a> <a
				href="#">고객센터</a> <a href="#">FAQ</a>
		</div>
		<div class="footer-info">
			<p>서울특별시 마포구 양화로 122, LAB7 빌딩 3층, 4층 | 전화: 02-1234-5678 | 팩스:
				02-1234-5679 | 등록번호 문화, 나00009등록일자 1980.12.29 | 발행일자 1980.12.29 |
				Tel.02-398-3114 이메일: info@example.com</p>
		</div>
		<div class="footer-copy">
			<p>&copy; 2025 서울 365 안전포털 웹 페이지. All rights reserved.</p>
		</div>
	</div>
</footer>

<script src="/assets/js/bootstrap.bundle.min.js"></script>
