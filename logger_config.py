import logging
import os


def setup_logger():
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # 防止重复添加handler（避免日志重复输出）
    if logger.hasHandlers():
        logger.handlers.clear()

    # 创建文件处理器
    log_file = os.path.join(os.path.dirname(__file__), 'app_debug.log')
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)

    # 创建控制台处理器
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)

    # 设置格式
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    # 添加处理器
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


logger = setup_logger()
