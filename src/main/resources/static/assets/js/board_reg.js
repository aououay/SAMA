document.addEventListener('DOMContentLoaded', function () {
    console.log('DOMContentLoaded');

    const divInput = document.querySelector("#div");
    const titleInput = document.querySelector("#title");
    const regIdInput = document.querySelector("#regId");
    const contentTextArea = document.querySelector("#contentsTextArea");
    const fileInput = document.querySelector("#file"); // 파일 입력 요소 추가

    const doSaveButton = document.querySelector("#doSave");
    doSaveButton.addEventListener("click", function (event) {
        console.log('doSaveButton click');
        // Validation  

        if (isEmpty(titleInput.value) === true) {
            alert('제목을 입력 하세요.');
            titleInput.focus();
            return;
        }

        if (isEmpty(contentTextArea.value) === true) {
            alert('내용을 입력 하세요.');
            contentTextArea.focus();
            return;
        }

        console.log("divInput.value:" + divInput.value);
        if (confirm('등록 하시겠습니까?') === false) return;

        // FormData 객체 생성 (파일 업로드와 텍스트 데이터 포함)
        var formData = new FormData();
        formData.append("div", divInput.value);
        formData.append("title", titleInput.value);
        formData.append("regId", regIdInput.value);
        formData.append("content", contentTextArea.value);
        
        // 파일이 선택되었으면 FormData에 파일 추가
        if (fileInput.files.length > 0) {
            formData.append("file", fileInput.files[0]);
        }

        // AJAX 요청
        $.ajax({
            type: "POST",
            url: "/board/doSave.do", // 게시글 저장 URL
            async: true,
            data: formData,
            contentType: false, // multipart/form-data로 전송하기 위해 false
            processData: false, // jQuery가 데이터를 처리하지 않도록 설정
            success: function (response) {
                console.log("success response:" + response);
                const message = JSON.parse(response);
                if (1 == message.messageId) { // 등록 성공
                    alert(message.message);
                    // 목록으로 화면 이동
                    window.location.href = '/board/doRetrieve.do?div=' + divInput.value;
                } else {
                    alert(message.message);
                }
            },
            error: function (response) {
                console.log("error:" + response);
            }
        });

    });
});

