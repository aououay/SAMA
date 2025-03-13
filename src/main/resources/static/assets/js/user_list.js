// 사용자 삭제 함수
function deleteUser(userId) {
	if (confirm('정말 삭제하시겠습니까?')) {
		$.ajax({
			url: '/main_user/doDelete.do',
			method: 'GET',
			data: { userId: userId },
			success: function(response) {
				const message = JSON.parse(response);
				if (message.messageId == 1) {
					alert(message.message);
					window.location.href = '/main_user/doRetrieve.do';
				} else {
					alert(message.message);
				}
			},
			error: function(xhr, status, error) {
				alert('삭제 실패: ' + error);
			}
		});
	}
}