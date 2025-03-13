// id_find.js

document.addEventListener("DOMContentLoaded", function() {
    const sendAuthCodeBtn = document.getElementById("sendAuthCodeButton");
    const verifyAuthCodeBtn = document.getElementById("verifyAuthCodeButton");

    // 인증 코드 전송 버튼 클릭 이벤트
    sendAuthCodeBtn.addEventListener('click', function () {
        let email = document.getElementById("email").value.trim();
        if (!email) {
            alert("이메일을 입력해주세요.");
            return;
        }
        // 이메일 유효성 체크 (간단한 정규식)
        let emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(email)) {
            alert("유효한 이메일 주소를 입력해주세요.");
            return;
        }
        $.ajax({
            type: "POST",
            url: "/email/sendAuthCode.do",
            data: JSON.stringify({ email: email }),
            contentType: "application/json",
            success: function(response) {
                alert(response.message);
                document.getElementById("verifyAuthCodeSection").style.display = "block";
            },
            error: function(xhr, status, error) {
                console.error("인증 코드 요청 실패:", status, error);
                alert("인증 코드 요청에 실패했습니다.");
            }
        });
    });

    // 인증 코드 확인 버튼 클릭 이벤트
    verifyAuthCodeBtn.addEventListener('click', function () {
        let email = document.getElementById("email").value.trim();
        let authCode = document.getElementById("authCode").value.trim();
        if (!email || !authCode) {
            alert("이메일과 인증 코드를 모두 입력해주세요.");
            return;
        }
        $.ajax({
            type: "POST",
            url: "/email/verifyAuthCode.do",
            data: JSON.stringify({ email: email, auth_code: authCode }),
            contentType: "application/json",
            success: function(response) {
                if (response.messageId === 1) {
                    alert("이메일 인증이 완료되었습니다.");
                    // 인증 성공 시 바로 아이디 찾기 AJAX 요청 실행
                    $.ajax({
                        type: "POST",
                        url: "/email/findUserId.do",
                        data: JSON.stringify({ email: email, auth_code: authCode }),
                        contentType: "application/json",
                        success: function(resp) {
                            // 아이디 찾기 결과 출력
                            document.getElementById("idDisplayMessage").innerHTML = "<p>" + resp.message + "</p>";
                            document.getElementById("idDisplaySection").style.display = "block";
                        },
                        error: function(xhr, status, error) {
                            console.error("아이디 찾기 실패:", status, error);
                            document.getElementById("idDisplayMessage").innerHTML = "<p>입력하신 정보와 일치하는 아이디가 없습니다.</p>";
                            document.getElementById("idDisplaySection").style.display = "block";
                        }
                    });
                } else {
                    alert("인증에 실패했습니다: " + response.message);
                }
            },
            error: function(xhr, status, error) {
                console.error("인증 코드 확인 실패:", status, error);
                alert("인증 코드 확인에 실패했습니다.");
            }
        });
    });
});
