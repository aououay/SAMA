
document.addEventListener("DOMContentLoaded", function() {
	let isEmailVerified = false;  // 이메일 인증 여부
	const sendAuthCodeBtn = document.getElementById("sendAuthCodeButton");  // 인증 코드 전송 버튼
	const verifyAuthCodeBtn = document.getElementById("verifyAuthCodeButton");  // 인증 코드 확인 버튼
	const changePasswordBtn = document.getElementById("changePasswordButton");  // 비밀번호 변경 버튼

	// 인증 코드 전송 버튼 클릭 시 이벤트
	sendAuthCodeBtn.addEventListener('click', function() {
		let email = document.getElementById("email").value;  // 입력된 이메일 가져오기

		// 이메일이 빈 값인지 확인
		if (!email) {
			alert("이메일을 입력해주세요.");
			return;
		}

		// 이메일 유효성 체크 (간단한 정규식 사용)
		let emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
		if (!emailRegex.test(email)) {
			alert("유효한 이메일 주소를 입력해주세요.");
			return;
		}

		// 서버로 인증 코드 요청 (AJAX)
		$.ajax({
			type: "POST",
			url: "/email/sendAuthCode.do",  // 서버 URL
			data: JSON.stringify({ email: email }),  // 이메일 데이터
			contentType: "application/json",  // 요청 데이터 타입
			success: function(response) {
				alert(response.message);  // 서버 응답 메시지 출력
				document.getElementById("verifyAuthCodeSection").style.display = "block";  // 인증 코드 입력란 표시
			},
			error: function(xhr, status, error) {
				console.error("AJAX 요청 실패:", status, error);
				alert("인증 코드 요청에 실패했습니다.");  // 오류 메시지 출력
			}
		});
	});

	// 인증 코드 확인 버튼 클릭 시 이벤트
	verifyAuthCodeBtn.addEventListener('click', function() {
		let email = document.getElementById("email").value;  // 이메일 가져오기
		let authCode = document.getElementById("authCode").value;  // 인증 코드 가져오기

		// 인증 코드가 입력되지 않은 경우 알림
		if (!authCode) {
			alert("인증 코드를 입력해주세요.");
			return;
		}

		// 서버로 인증 코드 확인 요청 (AJAX)
		$.ajax({
			type: "POST",
			url: "/email/verifyAuthCode.do",  // 서버 URL
			data: JSON.stringify({ email: email, auth_code: authCode }),  // 이메일과 인증 코드 전송
			contentType: "application/json",  // 요청 데이터 타입
			success: function(response) {
				// 인증 성공 시
				if (response.messageId === 1) {
					isEmailVerified = true;  // 이메일 인증 상태 업데이트
					alert('이메일 인증이 완료되었습니다.');
					document.getElementById("passwordChangeSection").style.display = "block";  // 비밀번호 변경 섹션 표시
				} else {
					isEmailVerified = false;  // 인증 실패 시
					alert('이메일 인증에 실패했습니다.');
				}
			},
			error: function(xhr, status, error) {
				console.error("AJAX 요청 실패:", status, error);
				alert("인증 코드 확인에 실패했습니다.");
			}
		});
	});

	// 비밀번호 변경 버튼 클릭 시 이벤트
	changePasswordBtn.addEventListener('click', function() {
		const email = $('#email').val();  // 이메일 가져오기
		const newPassword = $('#newPassword').val();  // 새로운 비밀번호 가져오기

		// 새로운 비밀번호가 입력되지 않은 경우
		if (!newPassword) {
			alert("새로운 비밀번호를 입력해주세요.");
			return;
		}

		// 서버로 비밀번호 변경 요청 (AJAX)
		$.ajax({
			url: '/email/changePassword.do',  // 서버 URL
			type: 'POST',
			contentType: 'application/json',  // 요청 데이터 타입
			data: JSON.stringify({ email: email, newPassword: newPassword }),  // 이메일과 새 비밀번호 전송
			success: function(response) {
				console.log("response.messageId:" + response.messageId);
				$('#passwordChangeMessage').text(response.message).css('color', 'green');  // 비밀번호 변경 성공 메시지
				// 비밀번호 변경 성공 후 메인 페이지로 리디렉션
				if (response.messageId === 1) {
					setTimeout(function() {
						window.location.href = "/login/login.do";  // 메인 페이지로 이동
					}, 1000);  // 1초 후에 리디렉션
				}
			},
			error: function(xhr, status, error) {
				$('#passwordChangeMessage').text("비밀번호 변경에 실패했습니다. 다시 시도해주세요.").css('color', 'red');  // 실패 시 메시지
			}
		});
	});
});
