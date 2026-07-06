#!/usr/bin/env python3
"""
Multi-Source Live News - SSE 实时推送服务器 (Render 部署版)
Sources: Reuters (sitemap), MIT Technology Review (RSS), People's Daily (HTML)
"""
import os, sys, re, json, time, threading, html as html_module
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

# ============ 配置 ============
PORT = int(os.environ.get('PORT', 9528))
POLL_INTERVAL = 60  # 秒
MAX_ARTICLES = 3000
HOURS_CUTOFF = 48
BJ_TZ = timezone(timedelta(hours=8))
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_FILE = os.path.join(SCRIPT_DIR, "index.html")

ALLOWED_PATHS = [
    '/world/', '/business/', '/markets/', '/legal/',
    '/technology/', '/fact-check/', '/sports/world-cup/'
]

NS = {
    'sm': 'http://www.sitemaps.org/schemas/sitemap/0.9',
    'news': 'http://www.google.com/schemas/sitemap-news/0.9'
}

# ============ 全局状态 ============
all_articles = []  # 内存中的所有文章
seen_urls = set()
clients = []
clients_lock = threading.Lock()
stats = {
    'started_at': datetime.now(BJ_TZ),
    'total_pushed': 0,
    'last_poll': None,
    'last_poll_status': 'idle',
    'poll_count': 0,
    'new_count': 0,
}
stats_lock = threading.Lock()

# ============ HTTP 下载工具 ============
def http_get(url, headers=None, timeout=30):
    """用 urllib 下载内容"""
    hdrs = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot-News)'}
    if headers:
        hdrs.update(headers)
    req = Request(url, headers=hdrs)
    with urlopen(req, timeout=timeout) as resp:
        return resp.read().decode('utf-8')

# ============ 路透社 (Sitemap XML) ============
def fetch_reuters():
    SITEMAP_URL = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml"
    all_items = []

    for from_offset in [0, 100, 200]:
        url = SITEMAP_URL
        if from_offset:
            url += f"&from={from_offset}"
        try:
            content = http_get(url, headers={'User-Agent': 'Googlebot-News', 'Accept': 'application/xml,text/xml'})
            if len(content) < 100:
                break
            root = ET.fromstring(content)
        except Exception as e:
            print(f"  [Reuters] 页{from_offset} 失败: {e}")
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

            all_items.append({
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

    filtered = [a for a in all_items if any(a['path'].startswith(p) for p in ALLOWED_PATHS)]
    print(f"  [Reuters] Sitemap: {len(all_items)} 篇, 过滤后: {len(filtered)} 篇")
    return filtered


# ============ MIT Technology Review (RSS Feed) ============
def fetch_mit_tech_review():
    RSS_URL = "https://www.technologyreview.com/feed/"
    MAX_ITEMS = 30

    try:
        content = http_get(RSS_URL, headers={'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)'})
        if len(content) < 100:
            print(f"  [MIT TR] RSS 下载失败")
            return []
    except Exception as e:
        print(f"  [MIT TR] RSS 下载失败: {e}")
        return []

    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        print(f"  [MIT TR] XML 解析失败: {e}")
        return []

    items = root.findall('.//item')
    articles = []

    for item in items[:MAX_ITEMS]:
        def get_text(el):
            if el is None: return None
            return (el.text or '').strip()

        title_el = item.find('title') or item.find('.//title')
        link_el = item.find('link') or item.find('.//link')
        pub_el = item.find('pubDate') or item.find('.//pubDate')

        title = get_text(title_el)
        url = get_text(link_el)
        pub_str = get_text(pub_el) or ''

        if not pub_str:
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

        title = re.sub(r'^[\s\xa0\u200b]+', '', title or '')
        if not title or len(title) < 5:
            continue

        try:
            pub_dt = datetime.strptime(pub_str.strip(), '%a, %d %b %Y %H:%M:%S %z')
            pub_bj = pub_dt.astimezone(BJ_TZ)
        except ValueError:
            try:
                pub_dt = datetime.strptime(pub_str.strip(), '%a, %d %b %Y %H:%M:%S +0000')
                pub_bj = pub_dt.replace(tzinfo=timezone.utc).astimezone(BJ_TZ)
            except:
                try:
                    pub_dt = datetime.fromisoformat(pub_str.replace('Z', '+00:00'))
                    pub_bj = pub_dt.astimezone(BJ_TZ)
                except:
                    continue

        path = f"/{category.lower().replace(' ', '-')}/"

        articles.append({
            'source': 'MIT Tech Review',
            'url': url,
            'title': title.lstrip(),
            'path': path,
            'category': category.lower(),
            'pub_bj': pub_bj.strftime('%Y-%m-%d %H:%M'),
            'pub_ts': int(pub_bj.timestamp()),
        })

    print(f"  [MIT TR] RSS: {len(articles)} 篇")
    return articles


# ============ 人民日报 (HTML 抓取) ============
def fetch_people_daily():
    HOME_URL = "https://www.people.com.cn/"

    try:
        content = http_get(HOME_URL, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'})
        if len(content) < 500:
            print(f"  [People] 首页下载失败")
            return []
    except Exception as e:
        print(f"  [People] 首页下载失败: {e}")
        return []

    articles = []
    seen = set()

    pattern = re.compile(
        r'<a[^>]*href="(https?://(?:[\w]+\.)*people\.com\.cn[^"]*(?:n1|n2|n3|gb)[^"]*)"[^>]*>([^<]{8,})</a>',
        re.IGNORECASE
    )

    for match in pattern.findall(content):
        url = match[0]
        raw_title = match[1].strip()
        title = html_module.unescape(raw_title).strip()

        if not url or len(title) < 6 or url in seen:
            continue
        skip_patterns = ['/video/', '/photo/', '/img/', '/pic/', '/v.', '/about', '/login']
        if any(p in url for p in skip_patterns):
            continue

        seen.add(url)

        if 'politics.people' in url or 'cpc.people' in url:
            category, path = 'politics', '/politics/'
        elif 'world.people' in url:
            category, path = 'world', '/world/'
        elif 'finance.people' in url:
            category, path = 'finance', '/finance/'
        elif 'legal.people' in url or 'society.people' in url:
            category, path = 'society', '/society/'
        elif 'keji.people' in url or 'tech.people' in url:
            category, path = 'technology', '/technology/'
        else:
            category, path = 'general', '/general/'

        now_bj = datetime.now(BJ_TZ)
        articles.append({
            'source': "People's Daily",
            'url': url,
            'title': title,
            'path': path,
            'category': category,
            'pub_bj': now_bj.strftime('%Y-%m-%d %H:%M'),
            'pub_ts': int(now_bj.timestamp()),
        })

    unique = []
    titles_seen = set()
    for a in articles:
        t_key = a['title'][:20]
        if t_key not in titles_seen:
            titles_seen.add(t_key)
            unique.append(a)

    print(f"  [People's Daily] HTML: {len(unique)} 篇")
    return unique


# ============ SSE 客户端管理 ============
def add_client(client_id, writer):
    with clients_lock:
        clients.append({'id': client_id, 'writer': writer})
    print(f"[SSE] 客户端 {client_id} 已连接，当前 {len(clients)} 个")

def remove_client(client_id):
    with clients_lock:
        global clients
        clients = [c for c in clients if c['id'] != client_id]
    print(f"[SSE] 客户端 {client_id} 已断开，当前 {len(clients)} 个")

def broadcast_event(event_type, data):
    msg = f"event: {event_type}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"
    msg_bytes = msg.encode('utf-8')
    with clients_lock:
        dead = []
        for c in clients:
            try:
                c['writer'].write(msg_bytes)
                c['writer'].flush()
            except Exception:
                dead.append(c)
        for c in dead:
            clients.remove(c)

def broadcast_stats():
    with stats_lock:
        data = {
            'started_at': stats['started_at'].strftime('%Y-%m-%d %H:%M:%S'),
            'total_pushed': stats['total_pushed'],
            'last_poll': stats['last_poll'].strftime('%H:%M:%S') if stats['last_poll'] else None,
            'last_poll_status': stats['last_poll_status'],
            'poll_count': stats['poll_count'],
            'new_count': stats['new_count'],
            'client_count': len(clients),
            'total_articles': len(all_articles),
        }
    broadcast_event('stats', data)


# ============ 轮询线程 ============
def poll_loop():
    global all_articles
    print("[POLL] 轮询线程启动")

    # 首次加载
    print("[POLL] 首次加载多源新闻...")
    fetch_all_sources(is_baseline=True)

    while True:
        time.sleep(POLL_INTERVAL)
        with stats_lock:
            stats['poll_count'] += 1
            current_poll_num = stats['poll_count']

        print(f"\n[POLL] 第 {current_poll_num} 轮检查 ({datetime.now(BJ_TZ).strftime('%H:%M:%S')})...")
        fetch_all_sources(is_baseline=False)
        broadcast_stats()


def fetch_all_sources(is_baseline):
    global all_articles
    new_articles = []

    print("--- 路透社 ---")
    try:
        reuters = fetch_reuters()
        new_r = [a for a in reuters if a['url'] not in seen_urls]
        new_articles.extend(new_r)
        print(f"  新增: {len(new_r)} 篇")
    except Exception as e:
        print(f"  错误: {e}")

    print("--- MIT Tech Review ---")
    try:
        mit = fetch_mit_tech_review()
        new_m = [a for a in mit if a['url'] not in seen_urls]
        new_articles.extend(new_m)
        print(f"  新增: {len(new_m)} 篇")
    except Exception as e:
        print(f"  错误: {e}")

    print("--- 人民日报 ---")
    try:
        people = fetch_people_daily()
        new_p = [a for a in people if a['url'] not in seen_urls]
        new_articles.extend(new_p)
        print(f"  新增: {len(new_p)} 篇")
    except Exception as e:
        print(f"  错误: {e}")

    # 合并新文章
    if new_articles:
        for a in new_articles:
            seen_urls.add(a['url'])
        all_articles.extend(new_articles)
        all_articles.sort(key=lambda x: x['pub_ts'], reverse=True)

        # 截断
        cutoff_ts = int((datetime.now(BJ_TZ) - timedelta(hours=HOURS_CUTOFF)).timestamp())
        all_articles = [a for a in all_articles if a['pub_ts'] >= cutoff_ts]
        if len(all_articles) > MAX_ARTICLES:
            all_articles = all_articles[:MAX_ARTICLES]

        with stats_lock:
            stats['new_count'] += len(new_articles)
            stats['total_pushed'] += len(new_articles)
            stats['last_poll'] = datetime.now(BJ_TZ)
            stats['last_poll_status'] = f'ok ({len(all_articles)} total, {len(new_articles)} new)'

        if not is_baseline:
            print(f"\n[POLL] 发现 {len(new_articles)} 篇新文章！")
            for a in sorted(new_articles, key=lambda x: x['pub_ts'], reverse=True)[:10]:
                print(f"  → [{a['pub_bj']}] [{a['source']}] {a['title'][:55]}")
            # 推送到所有 SSE 客户端
            for a in sorted(new_articles, key=lambda x: x['pub_ts'], reverse=True):
                broadcast_event('new_article', a)
                time.sleep(0.05)
    else:
        with stats_lock:
            stats['last_poll'] = datetime.now(BJ_TZ)
            stats['last_poll_status'] = f'ok ({len(all_articles)} total, 0 new)'

    # 更新 last_run.json
    save_last_run(len(new_articles) if not is_baseline else 0)

    print(f"[POLL] 总计: {len(all_articles)} 篇 ({len(new_articles)} 篇新)")


def save_last_run(new_count):
    """写 last_run.json 供前端读取"""
    source_stats = {}
    for a in all_articles:
        src = a['source']
        source_stats[src] = source_stats.get(src, 0) + 1

    last_run = {
        'timestamp': datetime.now(BJ_TZ).strftime('%Y-%m-%d %H:%M:%S'),
        'new': new_count,
        'total': len(all_articles),
        'sources': source_stats,
    }
    last_run_path = os.path.join(SCRIPT_DIR, "last_run.json")
    try:
        with open(last_run_path, 'w', encoding='utf-8') as f:
            json.dump(last_run, f, ensure_ascii=False, indent=2)
    except Exception:
        pass  # Render 文件系统可能只读，忽略


# ============ HTTP 服务器 ============
class SSEHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        qs = parse_qs(parsed.query)

        if path == '/' or path == '/index.html':
            self.serve_html()
        elif path == '/events':
            self.serve_sse()
        elif path == '/status':
            self.serve_status()
        elif path == '/articles.json':
            self.serve_articles_json()
        elif path == '/last_run.json':
            self.serve_last_run()
        else:
            self.send_error(404)

    def serve_html(self):
        try:
            with open(HTML_FILE, 'r', encoding='utf-8') as f:
                content = f.read()
            self.send_response(200)
            self.send_header('Content-Type', 'text/html; charset=utf-8')
            self.send_header('Content-Length', str(len(content.encode('utf-8'))))
            self.end_headers()
            self.wfile.write(content.encode('utf-8'))
        except FileNotFoundError:
            self.send_error(404, "HTML file not found")

    def serve_articles_json(self):
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(json.dumps(all_articles, ensure_ascii=False).encode('utf-8'))

    def serve_last_run(self):
        source_stats = {}
        for a in all_articles:
            src = a['source']
            source_stats[src] = source_stats.get(src, 0) + 1
        data = {
            'timestamp': datetime.now(BJ_TZ).strftime('%Y-%m-%d %H:%M:%S'),
            'new': stats['new_count'],
            'total': len(all_articles),
            'sources': source_stats,
        }
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Cache-Control', 'no-cache')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'))

    def serve_sse(self):
        self.send_response(200)
        self.send_header('Content-Type', 'text/event-stream')
        self.send_header('Cache-Control', 'no-cache')
        self.send_header('Connection', 'keep-alive')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()

        client_id = f"client_{int(time.time()*1000)}"
        add_client(client_id, self.wfile)

        welcome = f"event: connected\ndata: {json.dumps({'id': client_id, 'time': datetime.now(BJ_TZ).strftime('%H:%M:%S'), 'total_articles': len(all_articles)})}\n\n"
        try:
            self.wfile.write(welcome.encode('utf-8'))
            self.wfile.flush()
        except:
            remove_client(client_id)
            return

        # 推送当前所有文章作为初始数据
        if all_articles:
            batch = {'count': len(all_articles), 'articles': all_articles[:100]}
            batch_msg = f"event: batch\ndata: {json.dumps(batch, ensure_ascii=False)}\n\n"
            try:
                self.wfile.write(batch_msg.encode('utf-8'))
                self.wfile.flush()
            except:
                remove_client(client_id)
                return

        broadcast_stats()

        try:
            while True:
                time.sleep(15)
                self.wfile.write(b": ping\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            remove_client(client_id)

    def serve_status(self):
        with stats_lock:
            data = {
                'started_at': stats['started_at'].strftime('%Y-%m-%d %H:%M:%S'),
                'total_pushed': stats['total_pushed'],
                'last_poll': stats['last_poll'].strftime('%H:%M:%S') if stats['last_poll'] else None,
                'last_poll_status': stats['last_poll_status'],
                'poll_count': stats['poll_count'],
                'new_count': stats['new_count'],
                'seen_urls': len(seen_urls),
                'clients': len(clients),
                'total_articles': len(all_articles),
            }
        self.send_response(200)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.end_headers()
        self.wfile.write(json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8'))


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


# ============ 主入口 ============
def main():
    print("=" * 60)
    print("  Multi-Source Live News - SSE Server (Render)")
    print("=" * 60)
    print(f"  端口: {PORT}")
    print(f"  轮询间隔: {POLL_INTERVAL}s")
    print(f"  来源: Reuters + MIT Tech Review + People's Daily")
    print("=" * 60)

    # 启动轮询线程
    poll_thread = threading.Thread(target=poll_loop, daemon=True)
    poll_thread.start()

    # 启动 HTTP 服务器（绑定 0.0.0.0，Render 要求）
    server = ThreadedHTTPServer(('0.0.0.0', PORT), SSEHandler)
    print(f"\n[SERVER] 服务器已启动: 0.0.0.0:{PORT}")
    print(f"[SERVER] SSE: /events")
    print(f"[SERVER] API: /articles.json | /status")
    print(f"[SERVER] 按 Ctrl+C 停止\n")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\n[SERVER] 正在关闭...")
        server.shutdown()
        print("[SERVER] 已停止")


if __name__ == '__main__':
    main()
