import requests
import pandas as pd
import re
import os
from typing import List, Dict, Optional

# 用户代理头，避免被某些服务器拒绝
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
}

urls = [
    "https://raw.githubusercontent.com/zwc456baby/iptv_alive/master/live.txt",
    "https://live.zbds.top/tv/iptv6.txt",
    "https://live.zbds.top/tv/iptv4.txt",
]

# 改进的正则表达式模式
ipv4_pattern = re.compile(r'^https?://(\d{1,3}\.){3}\d{1,3}')
ipv6_pattern = re.compile(r'^https?://\[([a-fA-F0-9:]+)\]')

def fetch_streams_from_url(url: str) -> Optional[str]:
    """从指定URL获取直播流数据"""
    print(f"正在爬取网站源: {url}")
    try:
        response = requests.get(url, headers=HEADERS, timeout=15)
        response.encoding = 'utf-8'
        if response.status_code == 200:
            return response.text
        print(f"从 {url} 获取数据失败，状态码: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"请求 {url} 时发生错误: {e}")
    return None

def fetch_all_streams() -> str:
    """从所有URL获取直播流数据"""
    all_streams = []
    for url in urls:
        if content := fetch_streams_from_url(url):
            all_streams.append(content)
        else:
            print(f"跳过来源: {url}")
    return "\n".join(all_streams)

def parse_m3u(content: str) -> List[Dict[str, str]]:
    """解析M3U格式的直播流数据"""
    streams = []
    current_program = None
    current_attrs = {}
    
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
            
        if line.startswith("#EXTINF"):
            # 提取节目名称和其他属性
            current_program = "Unknown"
            current_attrs = {}
            
            # 尝试提取tvg-name
            if match := re.search(r'tvg-name="([^"]+)"', line):
                current_program = match.group(1).strip()
            # 如果没有tvg-name，尝试从最后一部分提取名称
            elif ',' in line:
                current_program = line.split(',')[-1].strip()
                
        elif line.startswith("http") and current_program:
            streams.append({
                "program_name": current_program, 
                "stream_url": line.strip()
            })
            current_program = None
            current_attrs = {}
            
    return streams

def parse_txt(content: str) -> List[Dict[str, str]]:
    """解析文本格式的直播流数据"""
    streams = []
    for line in content.splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
            
        # 支持多种分隔符：逗号、空格、制表符等
        if match := re.match(r"(.+?)[,\s]\s*(http.+)", line):
            program_name = match.group(1).strip()
            stream_url = match.group(2).strip()
            
            # 清理可能的引号
            program_name = program_name.strip('"\'')
            
            streams.append({
                "program_name": program_name,
                "stream_url": stream_url
            })
    return streams

def organize_streams(content: str) -> pd.DataFrame:
    """整理和去重直播流数据"""
    if not content:
        return pd.DataFrame(columns=['program_name', 'stream_url'])
        
    # 确定使用哪种解析器
    parser = parse_m3u if content.startswith("#EXTM3U") else parse_txt
    
    try:
        streams = parser(content)
        df = pd.DataFrame(streams)
        
        if df.empty:
            return df
            
        # 去重：保留每个节目名称和URL组合的第一个出现
        df = df.drop_duplicates(subset=['program_name', 'stream_url'])
        
        # 按节目名称分组，收集所有URL
        return df.groupby('program_name')['stream_url'].apply(list).reset_index()
        
    except Exception as e:
        print(f"整理数据时发生错误: {e}")
        return pd.DataFrame(columns=['program_name', 'stream_url'])

def save_to_txt(grouped_streams: pd.DataFrame, filename: str = "iptv.txt") -> None:
    """保存为文本格式，区分IPv4和IPv6"""
    if grouped_streams.empty:
        print("没有数据可保存到文本文件")
        return
        
    ipv4 = []
    ipv6 = []
    other = []
    
    for _, row in grouped_streams.iterrows():
        program = row['program_name']
        for url in row['stream_url']:
            if ipv4_pattern.match(url):
                ipv4.append(f"{program},{url}")
            elif ipv6_pattern.match(url):
                ipv6.append(f"{program},{url}")
            else:
                other.append(f"{program},{url}")
    
    with open(filename, 'w', encoding='utf-8') as f:
        if ipv4:
            f.write("# IPv4 Streams\n" + "\n".
