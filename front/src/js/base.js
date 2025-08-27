// 左侧边栏的菜单,包含多个应用
// MenuAsideLeft 控制应该显示哪个应用的菜单

function MenuAsideLeft(){
    let self = this;
    // .menu-left 出现在 index.html
    self.allSideMenus = $('.menu-left');
    // .menu-top 出现在 index.html
    self.allTitles = $('.menu-top');
    self.flag = 0;
}


// 隐藏所有应用的菜单
MenuAsideLeft.prototype.hideAllSideMenus  = function (){
    let self = this;

    $.each(self.allSideMenus, function (index, value) {
        $(value).hide();
    });
};


// 网页加载时,菜单的显示方式
// 不需要事件触发, 页面完成了加载, 立刻执行 loading 方法
MenuAsideLeft.prototype.loading = function () {
    let self = this;

    self.allSideMenus.eq(self.flag).attr('data-widget', 'treeview').show();
}


// 点击顶部导航栏的应用, 显示对应应用的菜单
MenuAsideLeft.prototype.showMenuEvent = function (){
    let self = this;

    self.allTitles.click(function (){
        let index = self.allTitles.index($(this));

        self.hideAllSideMenus();
        self.allSideMenus.eq(self.flag).removeAttr('data-widget')
        self.allSideMenus.eq(index).attr('data-widget', 'treeview').show();
        self.flag = index;
    });

};


MenuAsideLeft.prototype.run = function (){
    let self = this;

    self.loading();
    self.showMenuEvent();
};


function ClientFiles() {

}


ClientFiles.prototype.listenUploadFile = function () {
    let self = this;
    let uploadBtn = $('#upload-btn');

    uploadBtn.change(function () {
        // 可以一次上传多个文件,但这里一次上传一个文件
        // let file = $(this).files[0];
        let file = uploadBtn[0].files[0];
        let formData = new FormData();

        formData.append('file', file);
        xfzajax.post({
            'url': '/upload_file/',
            'data': formData,
            'processData': false,
            'contentType': false,
            'success': function (result) {
                if(result['code'] === 200) {
                    window.location.reload()
                }
            }
        });

    });
};

ClientFiles.prototype.run = function () {
    let self = this;

    self.listenUploadFile();
}


$(function () {
    let menuAsideLeft = new MenuAsideLeft();
    let clientFiles = new ClientFiles();

    menuAsideLeft.run();
    clientFiles.run();
});