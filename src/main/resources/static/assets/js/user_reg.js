let isEmailVerified = false;
let isNicknameChecked = false;
let isUserIdChecked = false;

$(document).ready(function() {
	// 비밀번호 확인 기능
	$('#confirmPassword').on('input', function() {
		const password = $('#password').val();
		const confirmPassword = $(this).val();
		const errorMessage = $('#passwordError');

		if (password !== confirmPassword) {
			errorMessage.text('비밀번호가 일치하지 않습니다.');
		} else {
			errorMessage.text('');
		}
	});

	// 폼 제출 시 비밀번호 일치 여부 확인
	$('#userForm').submit(function(event) {
		const password = $('#password').val();
		const confirmPassword = $('#confirmPassword').val();
		
		if (!isUserIdChecked) {
			alert('아이디 중복확인을 먼저 완료해주세요.');
			event.preventDefault();
			return;
		}

		if (password !== confirmPassword) {
			alert('비밀번호가 일치하지 않습니다.');
			event.preventDefault();
			return;
		}

		if (!isNicknameChecked) {
			alert('닉제임 중복확인을 먼저 완료해주세요.');
			event.preventDefault();
			return;
		}
		
		if (!isEmailVerified) {
			alert('이메일 인증을 먼저 완료해주세요.');
			event.preventDefault();
			return;
		}
		

		var formData = $(this).serialize();
		event.preventDefault();
		$.ajax({
			url: '/main_user/doSave.do',
			type: 'POST',
			data: formData,
			success: function(response) {
				try {
					var jsonResponse = JSON.parse(response);
					alert(jsonResponse.message);
					if (jsonResponse.messageId == 1) {
						window.location.href = '/login/login.do';
					}
				} catch (e) {
					console.error("JSON 파싱 오류:", e);
					alert('응답 처리 중 오류가 발생했습니다.');
				}
			},
			error: function() {
				alert('회원가입 중 오류가 발생했습니다.');
			}
		});
	});
});

// 나머지 함수들
function checkUserId() {
	let userId = document.getElementById("userId").value;
	$.ajax({
		type: "GET",
		url: "/main_user/checkUserId.do",
		data: { userId: userId },
		dataType: "json",
		success: function(response) {
			if (response.messageId === 1) {
				alert(response.message);
				isUserIdChecked = false;
			} else {
				alert(response.message);
				isUserIdChecked = true;
			}
		},
		error: function(xhr, status, error) {
			console.error("AJAX 요청 실패:", status, error);
			alert("아이디 중복 체크에 실패했습니다.");
		}
	});
}



function checkNickname() {
    let nickname = document.getElementById("nickname").value;
    $.ajax({
        type: "GET",
        url: "/main_user/checkNickname.do",  
        data: { nickname: nickname },
        dataType: "json",
        success: function(response) {
            if (response.messageId === 1) {
                alert(response.message);
				isNicknameChecked = false;
            } else {
                alert(response.message); 
				isNicknameChecked = true;				
			}	 
        },
        error: function(xhr, status, error) {
            console.error("AJAX 요청 실패:", status, error);
            alert("닉네임 중복 체크에 실패했습니다.");  
        }
    });
}


function sendAuthCode() {
	let email = document.getElementById("email").value;
	$.ajax({
		type: "POST",
		url: "/email/sendAuthCode.do",
		data: JSON.stringify({ email: email }),
		contentType: "application/json",
		success: function(response) {
			alert(response.message);
		},
		error: function(xhr, status, error) {
			console.error("AJAX 요청 실패:", status, error);
			alert("인증 코드 요청에 실패했습니다.");
		}
	});
}

function verifyAuthCode() {
	let email = document.getElementById("email").value;
	let authCode = document.getElementById("authCode").value;
	$.ajax({
		type: "POST",
		url: "/email/verifyAuthCode.do",
		data: JSON.stringify({ email: email, auth_code: authCode }),
		contentType: "application/json",
		success: function(response) {
			if (response.messageId === 1) {
				isEmailVerified = true;
				alert('이메일 인증이 완료되었습니다.');
			} else {
				isEmailVerified = false;
				alert('이메일 인증에 실패했습니다.');
			}
		},
		error: function(xhr, status, error) {
			console.error("AJAX 요청 실패:", status, error);
			alert("인증 코드 확인에 실패했습니다.");
		}
	});
}

$('#return_main').on('click', function(event) {
	event.preventDefault();
	let register_cancel = window.confirm('회원 가입을 취소 하시겠습니까?');
	if (register_cancel) {
		window.location.href = "/login/login.do";
	}
});


function sample5_execDaumPostcode() {
	new daum.Postcode({
		oncomplete: function(data) {
			document.getElementById("address").value = data.address;
		}
	}).open();
}
