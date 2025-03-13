window.onload = function () {
    var mapContainer = document.getElementById('map');
    var mapOption = {
        center: new kakao.maps.LatLng(37.5665, 126.9780), // 서울 중심
        level: 10
    };
    var map = new kakao.maps.Map(mapContainer, mapOption);

    fetch('/waterlevel')
        .then(response => response.json())
        .then(data => {
            data.forEach(point => {
                var marker = new kakao.maps.Marker({
                    position: new kakao.maps.LatLng(point.lat, point.lon),
                    map: map
                });

                kakao.maps.event.addListener(marker, 'click', function () {
                    showWaterLevelDetail(point.wlObsCd, point.obsNm, point.warningLevel, point.dangerLevel);
                });
            });
        });

    function showWaterLevelDetail(wlObsCd, obsNm, warningLevel, dangerLevel) {
        document.getElementById('obsName').innerText = obsNm;
        document.getElementById('warningLevel').innerText = warningLevel;
        document.getElementById('dangerLevel').innerText = dangerLevel;

        fetch('/waterlevel/' + wlObsCd)
            .then(response => response.json())
            .then(data => {
                var labels = [], values = [];
                data.forEach(entry => {
                    labels.push(entry.meaDt);
                    values.push(entry.wl);
                });

                new Chart(document.getElementById('waterLevelChart'), {
                    type: 'line',
                    data: { labels: labels, datasets: [{ label: "수위", data: values }] }
                });
            });

        document.getElementById('waterLevelDetail').style.display = "block";
    }
};
