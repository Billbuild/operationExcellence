// var gulp = require('gulp');
// var rename = require("gulp-rename"); // 改文件名
// var cssnano = require("gulp-cssnano"); // 压缩 css 文件
// var uglify = require("gulp-uglify"); // 压缩 js 文件
// var concat = require("gulp-concat"); // 合并文件
// var cache = require("gulp-cache"); // 缓存文件
// var imagemin = require("gulp-imagemin"); // 压缩图片
// // var connect = require("gulp-connect");
// var bs = require("browser-sync").create();
// var sass = require("gulp-sass")(require('sass'));
// var util = require("gulp-util");
// var sourcemaps = require("gulp-sourcemaps")
//
// var path = {
//     'html': './templates/**/',
//     // 'css': './src/css/',
//     'css': './src/css/**/',
//     'js': './src/js/',
//     'images': './src/image/',
//     'css_dist': './dist/css/',
//     'js_dist': './dist/js/',
//     'images_dist': './dist/images/'
// };
//
// gulp.task('html', function (done){
//     // return gulp.src('path.html' + '*.html')
//     //     .pipe(connect.reload());
//     gulp.src('path.html' + '*.html')
//         .pipe(bs.stream());
//     done();
// });
//
// gulp.task('css', function (done){
//     // gulp.src(path.css + '*.css')
//     gulp.src(path.css + '*.scss')
//         .pipe(sass().on('error', sass.logError))
//         .pipe(cssnano())
//         .pipe(rename({'suffix': '.min'}))
//         .pipe(gulp.dest(path.css_dist))
//         .pipe(bs.stream());
//     done();
// });
//
// gulp.task('js', function (done){
//     gulp.src(path.js + '*.js')
//         .pipe(sourcemaps.init())
//         .pipe(uglify().on('error', util.log))
//         .pipe(rename({'suffix': '.min'}))
//         .pipe(sourcemaps.write())
//         .pipe(gulp.dest(path.js_dist))
//         .pipe(bs.stream());
//     done();
// });
//
// gulp.task('images', function (done){
//     gulp.src(path.images + '*.*')
//         .pipe(cache(imagemin()))
//         .pipe(gulp.dest(path.images_dist))
//         .pipe(bs.stream());
//     done();
// });
//
// gulp.task("watch", function (done){
//     gulp.watch(path.html + '*.html', gulp.series("html"));
//     gulp.watch(path.css + '*.scss', gulp.series("css"));
//     gulp.watch(path.js + '*.js', gulp.series("js"));
//     gulp.watch(path.images + '*.*', gulp.series('images'));
//     done();
// });
//
// // 初始化浏览器的默认路径
// gulp.task("bs", function (){
//     bs.init({
//         "server": {
//             "baseDir": "./"
//         }
//     });
// });
//
// gulp.task("server", gulp.parallel("bs", "watch")); // 浏览器监听 css 和 js 任务

// 2022年12月4日 重写 gulpfile.js 文件
const {src, dest, watch, series} = require('gulp')
const uglify = require("gulp-uglify");
const sourcemaps = require("gulp-sourcemaps") // 匹配压缩 js 文件的源文件位置
const util = require("gulp-util"); // 压缩 js 文件
const rename = require("gulp-rename"); // 改文件名
const cssnano = require("gulp-cssnano"); // 压缩 css 文件
const sass = require("gulp-sass")(require('sass'));

var path = {
    'html': './templates/**/',
    'css': './src/css/**/',
    'js': './src/js/',
    'images': './src/image/',
    'css_dist': './dist/css/',
    'js_dist': './dist/js/',
    'images_dist': './dist/images/'
};

function js() {
    return src(path.js + '*.js')
        .pipe(sourcemaps.init())
        .pipe(uglify().on('error', util.log))
        .pipe(rename({extname: '.min.js'}))
        .pipe(sourcemaps.write())
        .pipe(dest(path.js_dist));
}

function css(){
    return src(path.css + '*.scss')
        .pipe(sass().on('error', sass.logError))
        .pipe(cssnano())
        .pipe(rename({extname: '.min.css'}))
        .pipe(dest(path.css_dist));
}

function server(cb) {
    watch(path.css + '*.scss', css);
    watch(path.js + '*.js', js);
    cb();
}

exports.css = css;
exports.js = js;
exports.server = server;