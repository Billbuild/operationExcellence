@echo off
REM 切换到项目目录
cd /d D:\home\operationExcellence

REM 添加所有修改
git add .

REM 提交并附带时间戳作为提交信息
git commit -m "auto commit on %date% %time%"

REM 推送到远程仓库
git push

echo ===================================
echo Push completed! 已经推送到 GitHub。
pause
