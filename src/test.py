"""
================================================
DMXAPI Gemini API 文本生成示例脚本
================================================
功能说明：
    本脚本演示如何使用 Gemini API 进行文本生成
    支持通过自定义提示词与 AI 模型进行交互

作者：DMXAPI
================================================
"""

import os
import requests
import json

# ========================================
# API 配置信息
# ========================================
prompt = "Hi, 你是谁？"  # 用户提示词 - 可在此处修改你想问的问题
model = "gemini-2.5-flash"  # 使用的 Gemini 模型版本
#sk-dJXD9bcZ0eSe2cLwNvWoyhDRYpdg2ED8OTEIHZG47INc1ciw
# API_KEY = os.getenv('OPENAI_API_KEY')  # 替换为你的 DMXAPI 密钥
API_KEY = "sk-dJXD9bcZ0eSe2cLwNvWoyhDRYpdg2ED8OTEIHZG47INc1ciw"
API_URL = f"https://www.dmxapi.cn/v1beta/models/{model}:generateContent?key={API_KEY}"  # DMXAPI gemini 请求地址


def generate_text(prompt):
    """
    调用 DMXAPI Gemini API 生成文本

    功能说明：
        向 DMXAPI Gemini API 发送文本提示，获取 AI 生成的响应内容

    参数：
        prompt (str): 用户输入的提示文本，用于指导 AI 生成内容

    返回值：
        dict: API 响应的 JSON 数据，包含生成的文本内容
        None: 请求失败时返回 None

    异常处理：
        捕获所有网络请求相关异常，并打印详细错误信息
    """
    # 设置请求头，指定内容类型为 JSON
    headers = {"Content-Type": "application/json"}

    # 构建请求负载数据
    payload = {
        "contents": [{
            "role": "user",  # 角色标识为用户
            "parts": [{"text": prompt}]  # 用户提示文本
        }]
    }

    try:
        # 发送 POST 请求到 Gemini API
        response = requests.post(
            API_URL,
            headers=headers,
            params={"key": API_KEY},
            json=payload
        )

        # 检查 HTTP 请求是否成功（状态码 2xx）
        response.raise_for_status()

        # 返回解析后的 JSON 响应数据
        return response.json()

    except requests.exceptions.RequestException as e:
        # 捕获请求异常并打印错误信息
        print(f"❌ 请求失败: {e}")

        # 如果存在响应对象，打印详细的错误信息
        if e.response:
            print(f"📊 状态码: {e.response.status_code}")
            print(f"📄 响应内容: {e.response.text}")

        return None


# ========================================
# 主程序入口
# ========================================
if __name__ == "__main__":
    print("=" * 50)
    print("🚀 Gemini API 文本生成测试")
    print("=" * 50)

    # 调用函数生成文本
    result = generate_text(prompt)

    # 如果请求成功，格式化输出结果
    if result:
        print("✅ 请求成功！API 响应结果：")
        print("-" * 50)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        print("-" * 50)
    else:
        print("❌ 请求失败，请检查 API 密钥和网络连接。")