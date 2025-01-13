import time
from gptpdf import parse_pdf
from PyPDF2 import PdfReader, PdfWriter

# 设置变量
pdf_path = 'D:\\testMDfy\\output_gpt\\origin.pdf'        # 目标PDF文件路径
split_pdf_path = 'D:\\testMDfy\\output_gpt\\split.pdf' # 拆分后保存的PDF路径
output_md_path = 'D:\\testMDfy\\output_gpt\\output.md'               # 输出的Markdown文件路径
api_key = 'api_key'            # 您的OpenAI API密钥
base_url = 'base_url'

# 步骤1：读取并拆分PDF，保存前30页
try:
    reader = PdfReader(pdf_path)
    writer = PdfWriter()

    # 确保PDF至少有30页
    num_pages = min(30, len(reader.pages))
    for page_num in range(num_pages):
        writer.add_page(reader.pages[page_num])

    # 将前30页写入新的PDF文件
    with open(split_pdf_path, 'wb') as f_split:
        writer.write(f_split)

    print(f"成功将前{num_pages}页保存到 {split_pdf_path}")

except Exception as e:
    print(f"拆分PDF时出错: {e}")
    exit(1)

# 步骤2：记录parse_pdf的运行时间并解析PDF
try:
    start_time = time.time()
    content, image_paths = parse_pdf(split_pdf_path, base_url=base_url, api_key=api_key)
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"PDF解析完成，用时 {elapsed_time:.2f} 秒")

except Exception as e:
    print(f"解析PDF时出错: {e}")
    exit(1)

# 步骤3：将内容保存为Markdown文件
try:
    with open(output_md_path, 'w', encoding='utf-8') as f_md:
        f_md.write(content)
    print(f"内容已保存到 {output_md_path}")

except Exception as e:
    print(f"保存Markdown文件时出错: {e}")
    exit(1)
