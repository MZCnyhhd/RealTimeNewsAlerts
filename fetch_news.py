#!/usr/bin/env python3
"""
Multi-Source News Crawler for GitHub Actions
Sources: Reuters (sitemap), MIT Technology Review (RSS), People's Daily (HTML)
Unified output: articles.json with 'source' field
"""
import json, os, re, subprocess, sys, html as html_module
from datetime import datetime, timedelta, timezone
import xml.etree.ElementTree as ET

DB_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "articles.json")
MAX_ARTICLES = 3000  # 多源，增加上限
HOURS_CUTOFF = 48     # 只保留最近48小时内的文章（可调整）

BJ_TZ = timezone(timedelta(hours=8))

# ============ 路透社 (Sitemap XML) ============
def fetch_reuters():
    """从 Reuters news sitemap 爬取"""
    SITEMAP_URL = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml"
    ALLOWED_PATHS = [
        '/world/', '/business/', '/markets/', '/legal/',
        '/technology/', '/fact-check/', '/sports/world-cup/'
    ]
    NS = {
        'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
        'news': 'http://www.google.com/schemas/sitemap-news/0.9'
    }

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
            title_el = u.find('news:news/news:title', NS)
            if loc is None or pd is None:
                continue

            article_url = loc.text.strip()
            pub_utc_str = pd.text.strip()
            art_title = title_el.text.strip() if title_el is not None else "No Title"

            try:
                pub_utc = datetime.fromisoformat(pub_utc_str.replace('Z', '+00:00'))
            except:
                continue

            pub_bj = pub_utc + timedelta(hours=8)
            path_match = re.match(r'https?://www\.reuters\.com(/[^/]*/)', article_url)
            path = path_match.group(1) if path_match else '/unknown/'

            all_articles.append({
                'source': 'Reuters',
                'url': article_url,
                'title': art_title,
                'path': path,
                'category': path.strip('/'),
                'pub_bj': pub_bj.strftime('%Y-%m-%d %H:%M'),
                'pub_ts': int(pub_utc.timestamp()),
            })

        if len(entries) < 50:
            break

    # 按路径过滤
    filtered = [a for a in all_articles if any(a['path'].startswith(p) for p in ALLOWED_PATHS)]
    print(f"  [Reuters] Sitemap: {len(all_articles)} 篇, 过滤后: {len(filtered)} 篇")
    return filtered


# ============ MIT Technology Review (RSS Feed) ============
def fetch_mit_tech_review():
    """从 MIT Tech Review RSS 爬取"""
    RSS_URL = "https://www.technologyreview.com/feed/"
    MAX_ITEMS = 30

    result = subprocess.run(
        ["curl", "-s", "-L",
         "-H", "User-Agent: Mozilla/5.0 (compatible; Googlebot/2.1)",
         "-o", "-", RSS_URL],
        capture_output=True, text=True, timeout=20
    )

    if result.returncode != 0 or len(result.stdout) < 100:
        print(f"  [MIT TR] RSS 下载失败")
        return []

    try:
        root = ET.fromstring(result.stdout)
    except ET.ParseError as e:
        print(f"  [MIT TR] XML 解析失败: {e}")
        return []

    items = root.findall('.//item')
    articles = []
    for item in items[:MAX_ITEMS]:
        # 用 .find() 和 .findtext() 处理可能的命名空间
        title_el = item.find('title') or item.find('{http://www.w3.org/2005/Atom}*//title') or item.find('.//title')
        link_el = item.find('link') or item.find('.//link')
        pub_el = item.find('pubDate') or item.find('{http://www.w3.org/2005/Atom}updated') or item.find('.//pubDate')

        # 获取文本内容（兼容各种方式）
        def get_text(el):
            if el is None: return None
            return (el.text or '').strip()

        title = get_text(title_el)
        url = get_text(link_el)

        # pubEl 可能在子元素里
        pub_str = get_text(pub_el) or ''
        if not pub_str:
            # 尝试从所有子元素找日期
            for child in item:
                ct = get_text(child)
                if ct and ('202' in ct or 'Jul' in ct or 'Jun' in ct):
                    pub_str = ct
                    break

        category = "tech"
        cat_el = item.find('category') or item.find('.//category')
        if cat_el is not None:
            category = get_text(cat_el).lower() or "tech"

        if not url or len(url) < 10 or not pub_str:
            continue

        # 清理标题中的特殊空白字符
        title = re.sub(r'^[\s\xa0\u200b]+', '', title)
        if not title or len(title) < 5:
            continue

        try:
            # Parse RFC2822 date format
            pub_dt = datetime.strptime(pub_str.strip(), '%a, %d %b %Y %H:%M:%S %z')
            pub_bj = pub_dt.astimezone(BJ_TZ)
        except ValueError:
            try:
                # 尝试无时区格式
                pub_dt = datetime.strptime(pub_str.strip(), '%a, %d %b %Y %H:%M:%S +0000')
                pub_bj = pub_dt.replace(tzinfo=timezone.utc).astimezone(BJ_TZ)
            except:
                try:
                    # ISO format
                    pub_dt = datetime.fromisoformat(pub_str.replace('Z', '+00:00'))
                    pub_bj = pub_dt.astimezone(BJ_TZ)
                except:
                    # fallback: 用当前时间
                    print(f"    [MIT TR] 日期解析失败: {pub_str[:40]}")
                    continue

        # 从 URL 提取分类路径
        path_match = re.search(r'/(\d{4}/\d{2}/\d{2}/)', url)
        path = f"/{category.lower().replace(' ', '-')}/"

        articles.append({
            'source': 'MIT Tech Review',
            'url': url,
            'title': title.lstrip(),  # 去掉可能的空格前缀
            'path': path,
            'category': category.lower(),
            'pub_bj': pub_bj.strftime('%Y-%m-%d %H:%M'),
            'pub_ts': int(pub_bj.timestamp()),
        })

    print(f"  [MIT TR] RSS: {len(articles)} 篇")
    return articles


# ============ 人民日报 (HTML 抓取) ============
def fetch_people_daily():
    """从人民日报首页抓取新闻标题"""
    HOME_URL = "https://www.people.com.cn/"

    result = subprocess.run(
        ["curl", "-s", "-L",
         "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
         "-o", "-", HOME_URL],
        capture_output=True, text=True, timeout=20
    )

    if result.returncode != 0 or len(result.stdout) < 500:
        print(f"  [People] 首页下载失败")
        return []

    content = result.stdout
    articles = []
    seen_urls = set()

    # 匹配首页上的新闻链接: people.com.cn/n1/... 或 politics.people.com.cn 等
    # 正则匹配 <a> 标签中的新闻链接和标题
    pattern = re.compile(
        r'<a[^>]*href="(https?://(?:[\w]+\.)*people\.com\.cn[^"]*(?:n1|n2|n3|gb)[^"]*)"[^>]*>([^<]{8,})</a>',
        re.IGNORECASE
    )

    for match in pattern.findall(content):
        url = match[0]
        raw_title = match[1].strip()

        # 清理标题：去掉多余空白、HTML实体
        title = html_module.unescape(raw_title).strip()

        # 过滤无效链接和标题
        if not url or len(title) < 6 or url in seen_urls:
            continue
        # 过滤非新闻页面
        skip_patterns = ['/video/', '/photo/', '/img/', '/pic/', '/v.', '/about', '/login']
        if any(p in url for p in skip_patterns):
            continue

        seen_urls.add(url)

        # 判断分类
        if 'politics.people' in url or 'cpc.people' in url:
            category = 'politics'
            path = '/politics/'
        elif 'world.people' in url or 'world.people.cn' in url:
            category = 'world'
            path = '/world/'
        elif 'finance.people' in url:
            category = 'finance'
            path = '/finance/'
        elif 'legal.people' in url or 'society.people' in url:
            category = 'society'
            path = '/society/'
        elif 'keji.people' in url or 'tech.people' in url:
            category = 'technology'
            path = '/technology/'
        else:
            category = 'general'
            path = '/general/'

        # 使用当前北京时间作为发布时间（HTML没有精确时间戳）
        now_bj = datetime.now(BJ_TZ)

        articles.append({
            'source': 'People\'s Daily',
            'url': url,
            'title': title,
            'path': path,
            'category': category,
            'pub_bj': now_bj.strftime('%Y-%m-%d %H:%M'),
            'pub_ts': int(now_bj.timestamp()),
        })

    # 去重（按标题相似度）
    unique = []
    titles_seen = set()
    for a in articles:
        t_key = a['title'][:20]
        if t_key not in titles_seen:
            titles_seen.add(t_key)
            unique.append(a)

    print(f"  [People's Daily] HTML: {len(unique)} 篇 (去重后)")
    return unique


# ============ 通用函数 ============

def load_existing():
    """读取现有 articles.json"""
    if os.path.exists(DB_FILE):
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            # 兼容旧格式：如果没有 source 字段，添加默认值
            for a in data:
                if 'source' not in a:
                    a['source'] = 'Reuters'
                if 'category' not in a:
                    a['category'] = a.get('path', '').strip('/')
            return data
    return []


def save_articles(articles):
    """保存 articles.json（按时间倒序，截断）"""
    articles.sort(key=lambda x: x['pub_ts'], reverse=True)
    
    # 截断到 MAX_ARTICLES
    if len(articles) > MAX_ARTICLES:
        articles = articles[:MAX_ARTICLES]
    
    with open(DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(articles, f, ensure_ascii=False, indent=2)


def main():
    now_str = datetime.now(BJ_TZ).strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{now_str}] === 开始多源爬取 ===")

    # 1. 读取现有数据
    existing = load_existing()
    existing_urls = {a['url'] for a in existing}
    print(f"现有文章: {len(existing)} 篇")

    # 2. 并行爬取各源
    all_new = []

    print("\n--- 爬取路透社 ---")
    try:
        reuters_articles = fetch_reuters()
        new_r = [a for a in reuters_articles if a['url'] not in existing_urls]
        all_new.extend(new_r)
        print(f"  新增: {len(new_r)} 篇")
    except Exception as e:
        print(f"  [Reuters] 错误: {e}")

    print("\n--- 爬取 MIT Tech Review ---")
    try:
        mit_articles = fetch_mit_tech_review()
        new_m = [a for a in mit_articles if a['url'] not in existing_urls]
        all_new.extend(new_m)
        print(f"  新增: {len(new_m)} 篇")
    except Exception as e:
        print(f"  [MIT TR] 错误: {e}")

    print("\n--- 爬取人民日报 ---")
    try:
        people_articles = fetch_people_daily()
        new_p = [a for a in people_articles if a['url'] not in existing_urls]
        all_new.extend(new_p)
        print(f"  新增: {len(new_p)} 篇")
    except Exception as e:
        print(f"  [People] 错误: {e}")

    # 3. 合并并保存
    total = existing + all_new
    save_articles(total)
    
    print(f"\n=== 总计: 已保存 {len(total)} 篇到 articles.json ({len(all_new)} 篇新文章) ===")
    
    # 打印新增文章摘要
    if all_new:
        print("\n新增文章:")
        for a in sorted(all_new, key=lambda x: x['pub_ts'], reverse=True)[:15]:
            print(f"  [{a['pub_bj']}] [{a['source']}] {a['title'][:55]}")
        if len(all_new) > 15:
            print(f"  ... 还有 {len(all_new) - 15} 篇")

    # 4. 写 last_run.json
    last_run_path = os.path.join(os.path.dirname(DB_FILE), "last_run.json")
    source_stats = {}
    for a in total:
        src = a['source']
        source_stats[src] = source_stats.get(src, 0) + 1
    
    last_run = {
        'timestamp': now_str,
        'new': len(all_new),
        'total': len(total),
        'sources': source_stats,
    }
    with open(last_run_path, 'w', encoding='utf-8') as f:
        json.dump(last_run, f, ensure_ascii=False, indent=2)
    print(f"\nlast_run.json: {json.dumps(last_run, ensure_ascii=False)}")


if __name__ == '__main__':
    main()
