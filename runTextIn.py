import os
import requests
from tqdm import tqdm

def pdf_to_markdown(api_url, app_id, secret_code, pdf_file_path, output_dir="output", **kwargs):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    headers = {
        'x-ti-app-id': app_id,
        'x-ti-secret-code': secret_code,
        'Content-Type': 'application/octet-stream'
    }

    params = {
        "pdf_pwd": kwargs.get("pdf_pwd", ""),
        "dpi": kwargs.get("dpi", 72),
        "page_start": kwargs.get("page_start", 1),
        "page_count": kwargs.get("page_count", 2000),
        "apply_document_tree": kwargs.get("apply_document_tree", 1),
        "markdown_details": kwargs.get("markdown_details", 0),
        "table_flavor": kwargs.get("table_flavor", "md"),
        "get_image": kwargs.get("get_image", "none"),
        "parse_mode": kwargs.get("parse_mode", "auto"),
    }

    with open(pdf_file_path, 'rb') as pdf_file:
        response = requests.post(api_url, headers=headers, params=params, data=pdf_file)

    if response.status_code == 200:
        result = response.json()
        if 'result' in result:
            md_content = result['result']['markdown']

            md_file_name = os.path.splitext(os.path.basename(pdf_file_path))[0] + ".md"
            md_file_path = os.path.join(output_dir, md_file_name)

            with open(md_file_path, 'w', encoding='utf-8') as md_file:
                md_file.write(md_content)

            return md_file_path
        else:
            print(f"Error: 'result' key not found in response. Full response: {result}")
            return None
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return None

def convert_folder(api_url, app_id, secret_code, folder_path, output_dir="output", **kwargs):
    pdf_files = [os.path.join(folder_path, f) for f in os.listdir(folder_path) if f.lower().endswith('.pdf')]

    for pdf_file in tqdm(pdf_files, desc="Converting PDFs"):
        pdf_to_markdown(api_url, app_id, secret_code, pdf_file, output_dir, **kwargs)


# Usage
api_url = "https://api.textin.com/ai/service/v1/pdf_to_markdown"
app_id = "app_id"
secret_code = "secret_code"
folder_path = "input"

convert_folder(api_url, app_id, secret_code, folder_path, apply_document_tree=1, markdown_details=1, table_flavor="md", get_image="both", parse_mode="scan")

