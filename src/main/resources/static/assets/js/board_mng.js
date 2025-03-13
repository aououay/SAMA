document.addEventListener('DOMContentLoaded',function(){



   const  boardIdInput = document.getElementById("boardId");
   const  divInput = document.getElementById("div");

   const  titleInput = document.getElementById("title");
   const  contentTextArea = document.getElementById("contentsTextArea");
   const fileInput = document.querySelector("#file"); // 파일 입력 요소 추가
   const changeButton = document.getElementById('changeBtn')
   changeButton.addEventListener('click', function() {
       this.value = '저장';  // value를 '저장'으로 변경
       this.id = 'doUpdateBtn';  // id를 'doUpdateBtn'으로 변경
     	
	   fileDeleteButton.style.display = 'inline-block';
	   document.getElementById('title').disabled = false;
	       document.getElementById('contentsTextArea').disabled = false;	
		   document.getElementById('file').disabled = false;
		  
		   const doUpdateButton = document.querySelector("#doUpdateBtn");
		      doUpdateButton.addEventListener("click", function (event) {
		          console.log("doUpdateButton click");
		          event.preventDefault();

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

		          if (confirm('수정 하시겠습니까?') === false) return;

		          // FormData 객체 생성 (파일 업로드와 텍스트 데이터 포함)
		          var formData = new FormData();
		          formData.append("boardId", boardIdInput.value);  // 게시글 ID
		          formData.append("title", titleInput.value);
		          formData.append("content", contentTextArea.value);
		          
		          // 파일이 선택되었으면 FormData에 파일 추가
		          if (fileInput.files.length > 0) {
		              formData.append("file", fileInput.files[0]);
		          }

		          // AJAX 요청
		          $.ajax({
		              type: "POST",
		              url: "/board/doUpdate.do", // 게시글 수정 URL
		              async: true,
		              data: formData,
		              contentType: false, // multipart/form-data로 전송하기 위해 false
		              processData: false, // jQuery가 데이터를 처리하지 않도록 설정
		              success: function (response) {
		                  console.log("success response:" + response);
		                  const message = JSON.parse(response);

		                  if (1 == message.messageId) {
		                      alert(message.message);
		                      window.location.href = '/board/doSelectOne.do?div=' + divInput.value + '&boardId=' + boardIdInput.value;
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
   const  doDeleteButton = document.getElementById("doDeleteBtn");
   console.log(doDeleteButton);
   doDeleteButton.addEventListener("click",function(event){
    console.log("doDeleteButton click");
    event.preventDefault();

    if(confirm('게시글을 삭제 하시겠습니까?') === false)return;

    $.ajax({
      type: "GET",
      url: "/board/doDelete.do",
      async: true,
      dataType: "html",
      data: {
          "boardId": boardIdInput.value
      },
      success: function(response) {
          console.log("success response:" + response);
          const message = JSON.parse(response);

          if(1 == message.messageId){
            alert(message.message);
            window.location.href = '/board/doRetrieve.do?div='+divInput.value; 
          }else{
            alert(message.message);
          }
      },
      error: function(response) {
          console.log("error:" + response);
      }
  });
  
  
   });
   
   
   const  fileDeleteButton = document.getElementById("fileDeleteBtn");
     console.log(fileDeleteButton);
          fileDeleteButton.addEventListener("click",function(event){
           console.log("fileDeleteButton click");
           event.preventDefault();

           if(confirm('첨부파일을 삭제 하시겠습니까?') === false)return;

           $.ajax({
             type: "GET",
             url: "/file/deleteFile.do",
             async: true,
             dataType: "html",
             data: {
                 "boardId": boardIdInput.value
             },
             success: function(response) {
                 console.log("success response:" + response);
                 const message = JSON.parse(response);

                 if(1 == message.messageId){
                   alert(message.message);
                   window.location.href = '/board/doSelectOne.do?div='+divInput.value+'&boardId='+boardIdInput.value; 
                 }else{
                   alert(message.message);
                 }
             },
             error: function(response) {
                 console.log("error:" + response);
             }
         });



        });


     
});