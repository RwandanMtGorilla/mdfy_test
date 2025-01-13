import os
import subprocess
import logging
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import datetime
import time

# 控制输出模式的变量
DEBUG_MODE = False
# 设置最大并行数量
MAX_WORKERS = 4  # 可根据需要调整最大并行数量

# 输入和输出文件夹
input_dir = './input'
output_dir = './output_mru'
log_dir = './logs'  # 日志文件夹

# 定义清理函数
def cleanup_files(output_subdir, logger):
    """检查并删除指定的文件"""
    files_to_delete = ['spans.pdf', 'layout.pdf', 'origin.pdf']
    for file_name in files_to_delete:
        file_path = os.path.join(output_subdir, file_name)
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Deleted file: {file_path}")

# 定义处理单个文件的函数
def process_pdf(pdf_file, gpu_id, input_dir, output_dir, log_file, debug_mode):
    # 每个子进程需要单独配置日志记录器
    logger = logging.getLogger(f"logger_{pdf_file}")
    logger.setLevel(logging.DEBUG)
    # 避免重复添加处理器
    if not logger.handlers:
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    start_time = time.time()  # 记录开始时间
    # 设置每个子进程使用的 GPU
    os.environ['CUDA_VISIBLE_DEVICES'] = str(gpu_id)
    logger.info(f"Starting processing on GPU {gpu_id}: {pdf_file}")
    input_path = os.path.join(input_dir, pdf_file)
    output_subdir = os.path.join(output_dir, pdf_file.replace('.pdf', ''), 'auto')
    processed_flag = os.path.join(output_subdir, pdf_file.replace('.pdf', '.md'))  # 标记文件路径

    # 确保输出子目录存在
    os.makedirs(output_subdir, exist_ok=True)

    # 在检查是否已经处理过之前进行文件清理
    cleanup_files(output_subdir, logger)

    # 检查是否已经处理过
    if os.path.exists(processed_flag) and os.path.getsize(processed_flag) >= 1 * 1024:
        logger.info(f"Skipping {pdf_file}, already processed.")
        duration = time.time() - start_time  # 计算持续时间
        logger.info(f"Processing time for {pdf_file}: {duration:.2f} seconds.")
        return f"Skipping {pdf_file}, already processed. Time: {duration:.2f}s"
    elif os.path.exists(processed_flag) and os.path.getsize(processed_flag) < 1 * 1024:
        logger.warning(f"File {pdf_file} marked as processed but is smaller than 1KB, reprocessing.")

    # 构造命令
    command = [
        "magic-pdf",
        "-p", input_path,
        "-o", output_dir,
        "-m", "auto"
    ]

    try:
        # 使用统一的日志文件
        with open(log_file, 'a') as log_f:
            result = subprocess.run(command, stdout=log_f, stderr=log_f, text=True if debug_mode else False)
            log_f.flush()

        # 检查命令的返回值
        if result.returncode != 0:
            logger.error(f"Error processing file: {pdf_file}. Check log for details.")
            duration = time.time() - start_time  # 计算持续时间
            logger.info(f"Processing time for {pdf_file}: {duration:.2f} seconds.")
            return f"Error processing file: {pdf_file}. Time: {duration:.2f}s"
        else:
            logger.info(f"Successfully processed: {pdf_file}")

            # 处理完成后再次进行文件清理
            cleanup_files(output_subdir, logger)

            duration = time.time() - start_time  # 计算持续时间
            logger.info(f"Processing time for {pdf_file}: {duration:.2f} seconds.")
            return f"Processed {pdf_file}. Time: {duration:.2f}s"
    except Exception as e:
        duration = time.time() - start_time  # 计算持续时间
        logger.exception(f"Exception occurred while processing {pdf_file}: {e}")
        logger.info(f"Processing time for {pdf_file}: {duration:.2f} seconds.")
        return f"Exception occurred while processing {pdf_file}: {e}. Time: {duration:.2f}s"

# 创建多进程池并处理PDF文件
def run_parallel(pdf_files, input_dir, output_dir, log_file, max_workers, debug_mode):
    results = []
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 对任务进行分配，一半任务使用GPU 0，另一半使用GPU 1
        futures = {}
        for i, pdf_file in enumerate(pdf_files):
            gpu_id = i % 2  # 分配 GPU 0 或 1
            futures[executor.submit(process_pdf, pdf_file, gpu_id, input_dir, output_dir, log_file, debug_mode)] = pdf_file
        
        # 使用tqdm管理主进程的进度条
        with tqdm(total=len(futures), desc="Processing PDFs") as pbar:
            for future in as_completed(futures):
                pdf_file = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as e:
                    print(f"Error processing file: {pdf_file}, {e}")

                # 更新进度条并显示当前时间
                pbar.set_postfix_str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                pbar.update(1)

    return results

def setup_global_logger(log_file):
    # 创建一个全局日志记录器
    global_logger = logging.getLogger("global_logger")
    global_logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    global_logger.addHandler(file_handler)
    return global_logger, file_handler

def main():
    # 确保输出文件夹和日志文件夹存在
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    # 获取input文件夹中的所有PDF文件
    pdf_files = [f for f in os.listdir(input_dir) if f.endswith('.pdf')]

    # 设置全局日志文件路径
    global_log_file = os.path.join(log_dir, "global_processing.log")

    # 设置全局日志记录器
    global_logger, file_handler = setup_global_logger(global_log_file)

    # 运行并获取结果
    results = run_parallel(pdf_files, input_dir, output_dir, global_log_file, MAX_WORKERS, DEBUG_MODE)
    for result in results:
        if result:
            print(result)

    # 关闭全局日志记录器的处理器
    file_handler.close()
    global_logger.removeHandler(file_handler)

    print("All PDFs processed.")

if __name__ == '__main__':
    import multiprocessing
    multiprocessing.freeze_support()
    main()
