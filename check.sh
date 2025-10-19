#!/bin/bash

# 定义变量
PROCESS_NAME="pando-bloom"  # 进程名（通常是可执行文件名）
START_COMMAND="nohup ./$PROCESS_NAME 2>&1 &"  # 启动命令
SCRIPT_DIR="/home/work/pando-bloom"  # ⬅️ 重要：请修改为您的脚本和程序所在的实际目录
LOG_FILE="$SCRIPT_DIR/pando-bloom-check.log"  # 日志文件路径

# 函数：检查进程是否在运行
is_process_running() {
    # 使用 pgrep 检查是否存在名为 $PROCESS_NAME 的进程
    # -f 选项表示匹配完整的命令行
    pgrep -f "$PROCESS_NAME" > /dev/null
    return $?
}

# 函数：启动进程
start_process() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 进程未运行，正在启动 $PROCESS_NAME..." >> "$LOG_FILE"
    # 切换到程序所在目录
    cd "$SCRIPT_DIR" || { echo "[$(date '+%Y-%m-%d %H:%M:%S')] 错误：无法切换到目录 $SCRIPT_DIR" >> "$LOG_FILE"; exit 1; }
    # 检查程序文件是否存在且可执行
    if [ ! -x "./$PROCESS_NAME" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] 错误：程序文件 ./$PROCESS_NAME 不存在或不可执行" >> "$LOG_FILE"
        exit 1
    fi
    # 执行启动命令
    eval "$START_COMMAND"
    if [ $? -eq 0 ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] $PROCESS_NAME 启动成功" >> "$LOG_FILE"
    else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] $PROCESS_NAME 启动失败！" >> "$LOG_FILE"
    fi
}

# 主逻辑
if is_process_running; then
    # 进程正在运行，无需操作（可选：记录心跳日志）
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $PROCESS_NAME 进程正常运行" >> "$LOG_FILE"
    exit 0
else
    # 进程未运行，尝试启动
    start_process
fi
