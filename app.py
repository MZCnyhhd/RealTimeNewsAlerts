#!/usr/bin/env python3
"""
Multi-Source Live News - SSE 实时推送服务器 (Render 部署版)
Sources: Reuters (sitemap), MIT Technology Review (RSS), People's Daily (HTML)
"""
import os, sys, re, json, time, threading, html as html_module
from datetime import datetime, timedelta, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
from urllib.parse import urlparse, parse_qs, urljoin
from urllib.request import Request, urlopen
import xml.etree.ElementTree as ET

# ============ 配置 ============
PORT = int(os.environ.get('PORT', 9528))
POLL_INTERVAL = 60  # 秒
MAX_ARTICLES = 3000

# 人民日报电子报时段爬取：仅在 BJT 05:00–09:00 窗口内抓取新数据
# 其余时间返回缓存（30min），减少无效请求。首次启动例外（缓存空则允许初始爬取）。
PEOPLE_HOURS = (5, 9)  # (start_hour, end_hour) 包含两端
SOURCE_CUTOFF_HOURS = {
    'Reuters': 48,
    '人民日报': 48,
    'MIT Tech Review': 24,   # 只保留最近24小时内的文章
}
DEFAULT_CUTOFF_HOURS = 48
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
import subprocess
def http_get(url, headers=None, timeout=30):
    """用 curl 下载内容（绕过沙箱代理对 urllib 的 HTTPS 截断问题）。
    curl 能完整流式获取，urllib 在代理下常遇 IncompleteRead。失败回退 urllib。
    返回解码后的字符串；彻底失败返回 ''。"""
    hdrs = ['-A', 'Mozilla/5.0 (compatible; Googlebot-News)']
    if headers:
        for k, v in headers.items():
            hdrs += ['-H', f'{k}: {v}']
    last_rc = None
    for attempt in range(1, 4):  # curl 最多重试 3 次（沙箱网络偶发 35 连接错误）
        try:
            result = subprocess.run(
                ['curl', '-sL', '--max-time', str(timeout), *hdrs, url],
                capture_output=True, timeout=timeout + 15
            )
            if result.returncode == 0 and result.stdout:
                for enc in ('utf-8', 'latin-1', 'gbk'):
                    try:
                        return result.stdout.decode(enc)
                    except UnicodeDecodeError:
                        continue
                return result.stdout.decode('utf-8', errors='replace')
            last_rc = result.returncode
        except Exception as e:
            last_rc = f'exc:{e}'
        if attempt < 3:
            time.sleep(1.5)
    print(f"  [curl] 最终失败 (rc={last_rc})，回退 urllib")

    # 回退：urllib
    try:
        uh = {'User-Agent': 'Mozilla/5.0 (compatible; Googlebot-News)'}
        if headers:
            uh.update(headers)
        req = Request(url, headers=uh)
        import ssl
        with urlopen(req, timeout=timeout, context=ssl.create_default_context()) as resp:
            raw = resp.read()
            enc = resp.headers.get_content_charset()
            if enc:
                return raw.decode(enc)
            for e in ('utf-8', 'latin-1', 'gbk'):
                try:
                    return raw.decode(e)
                except UnicodeDecodeError:
                    continue
            return raw.decode('utf-8', errors='replace')
    except Exception as e2:
        print(f"  [urllib] 也失败: {e2}")
        return ''

# ============ 路透社 (Sitemap XML) ============
def fetch_reuters():
    SITEMAP_URL = "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml"
    all_items = []

    for from_offset in [0, 100, 200]:
        url = SITEMAP_URL
        if from_offset:
            url += f"&from={from_offset}"
        try:
            content = http_get(url, headers={
                'User-Agent': 'Googlebot-News',
                'Accept': 'application/xml,text/xml',
            })
            if len(content) < 100:
                break
            root = ET.fromstring(content)
        except Exception as e:
            print(f"  [Reuters] 页{from_offset} 失败: {type(e).__name__}: {e}")
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
            # 原始 sitemap 时间（与 <lastmod> 相同，实为"重新索引/更新时间"）
            raw_bj = pub_utc + timedelta(hours=8)

            # —— 真实发布时间修正 ——
            # 路透社 sitemap 的 <news:publication_date> 实为"重新索引/更新时间"（与 <lastmod> 100% 相同），
            # 并非真实发布时间；真实发布日内嵌在文章 URL 的 slug 中（/YYYY-MM-DD/）。
            # 策略：提取 slug 日期；若 sitemap 日期比 slug 晚 ≥1 天（典型污染），用 slug 日期覆盖
            # （UTC 00:00 → 北京时间 08:00，时刻不可信故归零）；若同日则信任 sitemap（含正确时刻）。
            pub_corrected = False
            m = re.search(r'(\d{4})-(\d{2})-(\d{2})', article_url)
            if m:
                try:
                    slug_date = datetime(int(m.group(1)), int(m.group(2)), int(m.group(3)), tzinfo=timezone.utc)
                    delta_days = (pub_utc.date() - slug_date.date()).days
                    if delta_days >= 1:
                        pub_utc = slug_date
                        pub_corrected = True
                except Exception:
                    pass

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
                'pub_updated_bj': raw_bj.strftime('%Y-%m-%d %H:%M'),
                'pub_corrected': pub_corrected,
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
        content = http_get(RSS_URL, headers={
            'User-Agent': 'Mozilla/5.0 (compatible; Googlebot/2.1)',
            'Accept': 'application/rss+xml,application/xml,text/xml,*/*',
        })
        print(f"  [MIT TR] RSS 下载成功 ({len(content)} bytes)")
    except Exception as e:
        print(f"  [MIT TR] RSS 下载失败: {type(e).__name__}: {e}")
        return []

    if not content or len(content) < 200:
        print(f"  [MIT TR] RSS 内容过短或为空 ({len(content) if content else 0} bytes)")
        return []

    try:
        root = ET.fromstring(content)
    except ET.ParseError as e:
        print(f"  [MIT TR] XML 解析失败: {e}")
        return []

    items = root.findall('.//item')
    print(f"  [MIT TR] 解析到 {len(items)} 个 item")
    articles = []

    for item in items[:MAX_ITEMS]:
        def get_text(el):
            if el is None: return None
            return (el.text or '').strip()

        title_el = item.find('title')
        if title_el is None:
            title_el = item.find('.//title')
        link_el = item.find('link')
        if link_el is None:
            link_el = item.find('.//link')
        pub_el = item.find('pubDate')
        if pub_el is None:
            pub_el = item.find('.//pubDate')

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
        cat_el = item.find('category')
        if cat_el is None:
            cat_el = item.find('.//category')
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


# ============ 人民日报 (电子报版面爬取) ============
# 数据源：paper.people.com.cn 电子版。每天一期，按版面(01-20版)组织文章。
# 缓存 30 分钟，避免每分钟 20 次请求打爆对方服务器（日报内容本就每日更新一次）。
_people_cache = {'ts': 0.0, 'articles': []}
def fetch_people_daily():
    global _people_cache
    now = time.time()
    if _people_cache['articles'] and (now - _people_cache['ts'] < 1800):
        return _people_cache['articles']

    # 时段爬取检查：非窗口时段返回缓存（首次启动例外：缓存为空则允许初始爬取）
    now_bj = datetime.now(BJ_TZ)
    if _people_cache['articles']:
        h = now_bj.hour
        if not (PEOPLE_HOURS[0] <= h < PEOPLE_HOURS[1]):
            print(f"  [人民日报] 非爬取时段({h}:00，窗口{PEOPLE_HOURS[0]}-{PEOPLE_HOURS[1]}点)，使用缓存({_people_cache['articles']}篇)")
            return _people_cache['articles']

    LAYOUT_URL = "http://paper.people.com.cn/rmrb/pc/layout/index.html"
    UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
    try:
        layout_html = http_get(LAYOUT_URL, headers={'User-Agent': UA})
    except Exception as e:
        print(f"  [People] 版面索引下载失败: {e}")
        return _people_cache['articles']  # 失败时返回旧缓存

    # 解析 20 个版面：<li><a href="202607/06/node_01.html">...<br />第01版 要闻</a></li>
    ban_list = []
    for li in re.finditer(r'<li>(.*?)</li>', layout_html, re.S):
        block = li.group(1)
        hm = re.search(r'href="([^"]+)"', block)
        tm = re.search(r'第(\d+)版\s*([^<]+)', block)
        if hm and tm:
            ban_list.append((hm.group(1), tm.group(1), tm.group(2).strip()))

    if not ban_list:
        print("  [People] 未解析到版面列表")
        return _people_cache['articles']

    articles = []
    seen = set()
    edition_date = None
    # 人民日报需跳过的版面（广告、副刊等）
    SKIP_BANHAO = {'16', '20'}
    for href, banhao, banming in ban_list:
        if banhao in SKIP_BANHAO:
            continue
        node_url = urljoin(LAYOUT_URL, href)
        dm = re.search(r'(\d{6})/(\d{2})', href)
        if dm and edition_date is None:
            edition_date = f"{dm.group(1)[:4]}-{dm.group(1)[4:6]}-{dm.group(2)}"
        try:
            node_html = http_get(node_url, headers={'User-Agent': UA})
        except Exception as e:
            print(f"  [People] 版面 {banhao} 下载失败: {e}")
            continue
        # 文章链接形如 ../../../content/202607/06/content_XXXXXX.html
        for am in re.finditer(r'<a[^>]*href="([^"]*content/[^"]+\.html?)"[^>]*>(.*?)</a>', node_html, re.S):
            ahref = am.group(1)
            title = re.sub(r'<[^>]+>', '', am.group(2)).strip()
            title = html_module.unescape(title)
            # 去掉"本版责编：XXX"等编者信息行
            title = re.sub(r'^本版责编[：:].*$', '', title).strip()
            # 去掉纯空格/标点残留
            title = re.sub(r'^[\s\-·—–]+$|^编辑：.*$', '', title).strip()
            if len(title) < 6:
                continue
            full = urljoin(node_url, ahref)
            if full in seen:
                continue
            seen.add(full)
            # 电子报显示出版日期（不虚构时间）
            pub_bj = edition_date or datetime.now(BJ_TZ).strftime('%Y-%m-%d')
            try:
                pub_ts = int(datetime.strptime(pub_bj + ' 08:00', '%Y-%m-%d %H:%M').replace(tzinfo=BJ_TZ).timestamp())
            except Exception:
                pub_ts = int(datetime.now(BJ_TZ).timestamp())
            articles.append({
                'source': "人民日报",
                'url': full,
                'title': title,
                'path': f"/{banhao}/",
                'category': f"{banhao}版：{banming}",
                'pub_bj': pub_bj,
                'pub_ts': pub_ts,
            })

    if articles:
        _people_cache = {'ts': now, 'articles': articles}
    print(f"  [人民日报] 电子报: {len(articles)} 篇, {len(ban_list)} 个版面")
    return articles


# ============ Playwright (路透社真实发布时间 enrichment) ============
# 路透社 sitemap 的 publication_date 实为"重新索引/更新时间"，非真实发布时间。
# 真实发布时间仅在文章页 JSON-LD 的 datePublished 字段。文章页被 DataDome 拦截，
# 普通 urllib 抓不到；尝试用无头 Chromium 渲染提取。若被拦或环境无 Chromium，
# 自动退回 sitemap 时间（PW_ENABLED=False），不影响主流程。
PW_ENABLED = False
_pw = None
_pw_browser = None
_enriched_urls = set()
PW_PROBE_URL = None          # 启动时探测用的文章 URL
PW_BUDGET_S = 40             # 每次轮询 enrichment 最多耗时（秒）
PW_GOTO_TIMEOUT = 25000      # 单篇文章导航超时（毫秒）

def _parse_iso(s):
    s = (s or '').strip()
    if not s:
        raise ValueError('empty')
    try:
        return datetime.fromisoformat(s.replace('Z', '+00:00'))
    except Exception:
        pass
    for fmt in ('%Y-%m-%dT%H:%M:%S%z', '%Y-%m-%dT%H:%M:%S.%f%z',
                '%Y-%m-%d %H:%M:%S%z', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.strptime(s, fmt)
        except Exception:
            continue
    raise ValueError('unparseable: ' + s)

def init_playwright():
    """启动无头 Chromium 并探测能否取到真实发布时间；失败则保持关闭。"""
    global _pw, _pw_browser, PW_ENABLED, PW_PROBE_URL
    try:
        from playwright.sync_api import sync_playwright
        _pw = sync_playwright().start()
        _pw_browser = _pw.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
        )
        print("[PW] Chromium 启动成功，开始探测文章页可达性...")
        # 探测：取一篇路透社文章，尝试提取 datePublished
        probe = PW_PROBE_URL or _get_one_reuters_url()
        if probe and _enrich_one(probe):
            PW_ENABLED = True
            print("[PW] 探测成功：将用文章页真实发布时间覆盖 sitemap 时间")
        else:
            PW_ENABLED = False
            print("[PW] 探测失败（文章页被反爬拦截或环境受限）：退回 sitemap 时间")
            try:
                _pw_browser.close()
            except Exception:
                pass
            try:
                _pw.stop()
            except Exception:
                pass
            _pw_browser = None
            _pw = None
    except Exception as e:
        PW_ENABLED = False
        print(f"[PW] Playwright 不可用（退回 sitemap 时间）: {e}")

def _get_one_reuters_url():
    """从 sitemap 取一篇路透社文章 URL，供探测使用。"""
    try:
        content = http_get(
            "https://www.reuters.com/arc/outboundfeeds/news-sitemap/?outputType=xml",
            headers={'User-Agent': 'Googlebot-News', 'Accept': 'application/xml,text/xml'}
        )
        m = re.search(r'<loc>(https?://www\.reuters\.com/[^<]+)</loc>', content or '')
        return m.group(1) if m else None
    except Exception:
        return None

def _enrich_one(url):
    """从单篇路透社文章页提取 datePublished 字符串；失败返回 None。"""
    if _pw_browser is None:
        return None
    page = None
    try:
        page = _pw_browser.new_page(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                       '(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36'
        )
        resp = page.goto(url, timeout=PW_GOTO_TIMEOUT, wait_until='domcontentloaded')
        if resp is None or resp.status >= 400:
            return None
        page.wait_for_timeout(3500)
        data = page.evaluate(r"""() => {
            const out = {};
            document.querySelectorAll('script[type="application/ld+json"]').forEach(s => {
                try {
                    const j = JSON.parse(s.textContent);
                    const arr = Array.isArray(j) ? j : [j];
                    for (const o of arr) {
                        if (o && o.datePublished) out.datePublished = o.datePublished;
                        if (o && o.dateModified) out.dateModified = o.dateModified;
                    }
                } catch(e){}
            });
            const t = document.querySelector('time[datetime]');
            if (t) out.timeTag = t.getAttribute('datetime');
            return out;
        }""")
        if isinstance(data, dict) and data.get('datePublished'):
            return data['datePublished']
        return None
    except Exception:
        return None
    finally:
        if page is not None:
            try:
                page.close()
            except Exception:
                pass

def enrich_reuters(articles):
    """用文章页真实发布时间就地覆盖 sitemap 时间（仅未处理过的新文章，受时间预算限制）。"""
    if not PW_ENABLED or _pw_browser is None:
        return
    deadline = time.time() + PW_BUDGET_S
    for a in articles:
        if a.get('source') != 'Reuters':
            continue
        if a['url'] in _enriched_urls:
            continue
        if time.time() > deadline:
            break
        dp = _enrich_one(a['url'])
        _enriched_urls.add(a['url'])
        if not dp:
            continue
        try:
            pub_dt = _parse_iso(dp).astimezone(BJ_TZ)
            a['pub_bj'] = pub_dt.strftime('%Y-%m-%d %H:%M')
            a['pub_ts'] = int(pub_dt.timestamp())
            a['pub_source'] = 'article'   # 标记来自文章页真实发布时间
        except Exception:
            pass




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

    # 初始化 Playwright（探测文章页可达性；被拦则自动退回 sitemap 时间）
    init_playwright()

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
        # 用文章页真实发布时间覆盖 sitemap（被拦则自动跳过，退回 sitemap 时间）
        enrich_reuters(new_r)
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

        # 截断（按来源单独计算时间窗口）
        now_ts = datetime.now(BJ_TZ).timestamp()
        kept = []
        for a in all_articles:
            cutoff_h = SOURCE_CUTOFF_HOURS.get(a['source'], DEFAULT_CUTOFF_HOURS)
            cutoff_ts = int((now_ts - cutoff_h * 3600))
            if a['pub_ts'] >= cutoff_ts:
                kept.append(a)
        all_articles = kept
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
    # 强制无缓冲输出（后台运行时日志才能实时写入文件）
    import sys
    sys.stdout.reconfigure(line_buffering=True)
    sys.stderr.reconfigure(line_buffering=True)

    print("=" * 60)
    print("  Multi-Source Live News - SSE Server (Render)")
    print("=" * 60)
    print(f"  端口: {PORT}")
    print(f"  轮询间隔: {POLL_INTERVAL}s")
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
