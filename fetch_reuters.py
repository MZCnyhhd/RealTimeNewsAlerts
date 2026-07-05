#!/usr/bin/env python3
"""
Reuters News Crawler for GitHub Actions
读取 articles.json → 爬取 sitemap → 追加新文章 → 写回 articles.json
"""
import json, os, re, subprocess, sys
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET

SITEMAP_URL = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml"
ALLOWED_PATHS = [
    '/world/', '/business/', '/markets/', '/legal/',
    '/technology/', '/fact-check/', '/sports/world-cup/'
]
DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "articles.json")
MAX_ARTICLES = 2000  # 最多保留条数

NS = {
    'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
    'news': 'http://www.google.com/schemas/sitemap-news/0.9'
}

def download_sitemap():
    """下载 sitemap（可能需要翻页）"""
    all_articles = []
    for from_offset in [0, 100, 200]:
        url = SITEMAP_URL
        if from_offset:
            url += f"&from={from_offset}"
        result = subprocess.run(
            ["curl", "-s", "-L",
             "-H", "User-Agent: Googlebot-News",
             "-H", "Accept: application/xml,text/xml",
             "-o", "-", url],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0 or len(result.stdout) < 100:
            break
        
        try:
            root = ET.fromstring(result.stdout)
        except:
            break
        
        entries = root.findall('sm:url', NS)
        if not entries:
            break
        
        for u in entries:
            loc = u.find('sm:loc', NS)
            pd = u.find('news:news/news:publication_date', NS)
            title = u.find('news:news/news:title', NS)
            if loc is None or pd is None:
                continue
            
            url = loc.text.strip()
            pub_utc_str = pd.text.strip()
            art_title = title.text.strip() if title is not None else "No Title"
            
            try:
                pub_utc = datetime.fromisoformat(pub_utc_str.replace('Z', '+00:00'))
            except:
                continue
            
            pub_bj = pub_utc + timedelta(hours=8)
            path_match = re.match(r'https?://www\.reuters\.com(/[^/]*/)', url)
            path = path_match.group(1) if path_match else '/unknown/'
            
            all_articles.append({
                'url': url,
                'title': art_title,
                'path': path,
                'pub_bj': pub_bj.strftime('%Y-%m-%d %H:%M'),
                'pub_ts': int(pub_utc.timestamp()),
            })
        
        # 如果这页不满 50 条，说明是最后一页
        if len(entries) < 50:
            break
    
    return all_articles

def filter_articles(articles):
    return [a for a in articles if any(a['path'].startswith(p) for p in ALLOWED_PATHS)]

def load_existing():
    """读取现有 articles.json"""
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            return json.load(f)
    return []

def save_articles(articles):
    """保存 articles.json（按时间倒序，截断到 MAX_ARTICLES）"""
    articles.sort(key=lambda x: x['pub_ts'], reverse=True)
    if len(articles) > MAX_ARTICLES:
        articles = articles[:MAX_ARTICLES]
    
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)

def main():
    print(f"[{datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')}] 开始爬取...")
    
    # 1. 读取现有数据
    existing = load_existing()
    existing_urls = {a['url'] for a in existing}
    print(f"现有文章: {len(existing)} 篇")
    
    # 2. 爬取 sitemap
    raw = download_sitemap()
    filtered = filter_articles(raw)
    print(f"Sitemap 总计: {len(raw)} 篇，路径过滤后: {len(filtered)} 篇")
    
    # 3. 找新文章
    new_articles = [a for a in filtered if a['url'] not in existing_urls]
    print(f"新文章: {len(new_articles)} 篇")
    
    if new_articles:
        for a in new_articles:
            print(f"  + {a['pub_bj']} {a['path']} {a['title'][:60]}")
    
    # 4. 合并并保存
    all_articles = existing + new_articles
    save_articles(all_articles)
    print(f"已保存 {len(all_articles)} 篇到 articles.json")
    
    # 5. 输出摘要（供 GitHub Actions 使用）
    summary = {
        'existing': len(existing),
        'sitemap_total': len(raw),
        'filtered': len(filtered),
        'new': len(new_articles),
        'saved': len(all_articles),
        'timestamp': datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),
    }
    print(f"\n::set-output name=summary::{json.dumps(summary, ensure_ascii=False)}")
    
    # 写入 last_run.json 供前端读取
    last_run_path = os.path.join(os.path.dirname(DB_FILE), "last_run.json")
    with open(last_run_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)

if __name__ == '__main__':
    main()
