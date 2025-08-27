class OWE {
    constructor() {
    }

    // 页面<效率> 按钮<缝制工人每日缝制效率>
    listenDailySewerEff () {
        let btn = $('#daily-sewer-eff');

        btn.click(function () {
            console.log('welcome');

            xfzajax.post({
                'url': '/measures/daily_sewer_eff/',
                'success': function (result) {
                    if (result['code'] === 200) {
                        window.location.reload();
                    }
                }
            });
        });
    }

}

$(function () {
   let owe = new OWE();

   owe.listenDailySewerEff();
});