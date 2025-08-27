class TOOLS {
    constructor() {
    }
    // 页面<工具> 按钮<复制生产效率报告>
    listenCopyEffReport () {
        let btn = $('#copy-eff-report');

        btn.click(function () {
            console.log('welcome');

            xfzajax.post({
                'url': '/measures/copy_eff_report/',
                'success': function (result) {
                    if (result['code'] === 200) {
                        window.location.reload();
                    }
                }
            });
        });
    }

    // 页面<工具> 按钮<清空缝制工人每日车缝效率>
    listenClearExcel () {
        let btn = $('#clear-excel');

        btn.click(function () {
            console.log('welcome');

            xfzajax.post({
                'url': '/measures/clear_excel/',
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
   let tools = new TOOLS();

   tools.listenCopyEffReport();
   tools.listenClearExcel();
});