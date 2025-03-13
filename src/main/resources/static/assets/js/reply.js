document.addEventListener('DOMContentLoaded',function(){


   const divInput = document.getElementById("div");
   const boardIdInput = document.getElementById("boardId");
   const replyIdInput = document.getElementById("replyId");
   const replyTextAreaInput = document.getElementById("replyTextArea");
   const regId = document.getElementById("replyRegId");
   const replyContent = document.getElementById("replyContent");
   
   const replySaveBtn = document.querySelector("#replySubmitBtn");
   replySaveBtn.addEventListener("click",function(event){
    console.log("replySaveBtn click");
    event.preventDefault();

    //console.log(${sessionScope.user.userId});
    if(confirm('등록 하시겠습니까?') === false)return; 

    $.ajax({
      type: "POST",
      url: "/reply/doSave.do",
      async: true,
      dataType: "html",
      data: {
          "boardId": boardIdInput.value,
          //"regId": regId.value,
		  "replyContent": replyTextAreaInput.value
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
   
   const  replyDeleteBtn = document.getElementById("replyDeleteBtn");
    replyDeleteBtn.addEventListener("click",function(event){
     console.log("replyDeleteBtn click");
     event.preventDefault();

     if(confirm('댓글을 삭제 하시겠습니까?') === false)return;

     $.ajax({
       type: "GET",
       url: "/reply/doDelete.do",
       async: true,
       dataType: "html",
       data: {
           "replyId": replyIdInput.value
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
	const changeReplyButton = document.getElementById('replyChangeBtn')
	changeReplyButton.addEventListener('click', function() {
		
		this.value = '저장';  // value를 '저장'으로 변경
		      this.id = 'replyUpdateBtn';  // id를 'doUpdateBtn'으로 변경
		   document.getElementById('replyContent').disabled = false;
		    
		       
	const replyUpdateBtn = document.querySelector("#replyUpdateBtn");
	   replyUpdateBtn.addEventListener("click",function(event){
	    console.log("replyUpdateBtn click");
	    event.preventDefault();

	    //console.log(${sessionScope.user.userId});
	    if(confirm('수정 하시겠습니까?') === false)return; 

	    $.ajax({
	      type: "POST",
	      url: "/reply/doUpdate.do",
	      async: true,
	      dataType: "html",
	      data: {
	          "replyContent": replyContent.value,
	          "replyId": replyIdInput.value
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

});
