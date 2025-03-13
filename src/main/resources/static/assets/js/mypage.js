// script.js
let isNicknameChecked = false; 

function execDaumPostcode() {
    new daum.Postcode({
        oncomplete: function(data) {
            // 선택한 주소를 address 필드에 설정
            document.getElementById('address').value = data.address;
        }
    }).open();
}

	// 폼 제출 시 비밀번호 일치 여부 확인
	$('#userForm').submit(function(event) {
		if (!isNicknameChecked) {
			alert('닉제임 중복확인을 먼저 완료해주세요.');
			event.preventDefault();
			return;
		}
		
		console.log(isNicknameChecked);
		
		var formData = $(this).serialize();
		event.preventDefault();
		$.ajax({
			url: '/mypage/update',
			type: 'POST',
			data: formData,
			success: function(response) {
				try {
					console.log(isNicknameChecked);
					var jsonResponse = JSON.parse(response);
					alert(jsonResponse.message);
					if (jsonResponse.messageId == 1) {
						window.location.href = '/mypage';
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
	
	
	
	
