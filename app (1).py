"""DealRadar MX local prototype. Python 3.10+, no third-party packages."""
import csv, io, json, os, sqlite3, statistics, threading, urllib.request, time, traceback
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode

ROOT=Path(__file__).parent
DB=Path(os.environ.get('DEALRADAR_DB',str(ROOT/'dealradar.sqlite3')))
PORT=int(os.environ.get('PORT','8080'))
PUBLIC_DEMO=os.environ.get('PUBLIC_DEMO','0')=='1'
SCAN_INTERVAL=max(300,int(os.environ.get('SCAN_INTERVAL_SECONDS','900')))
ML_TOKEN=os.environ.get('MERCADOLIBRE_ACCESS_TOKEN','')
SCAN_STATE={'last_attempt':None,'last_success':None,'last_error':None,'last_count':0,'status':'authorization required' if not ML_TOKEN else 'starting'}
STORES={'Amazon México':'amazon.com.mx','Mercado Libre':'mercadolibre.com.mx','Coppel':'coppel.com','Elektra':'elektra.mx'}
CATS=['Phones','Laptops / PCs','Gaming consoles','Tablets','TVs']
LOCK=threading.RLock()
def db():
 c=sqlite3.connect(DB);c.row_factory=sqlite3.Row;return c

def init():
 with db() as c:
  c.executescript('''CREATE TABLE IF NOT EXISTS products(id INTEGER PRIMARY KEY,store TEXT NOT NULL,title TEXT NOT NULL,category TEXT NOT NULL,url TEXT NOT NULL,source TEXT NOT NULL DEFAULT 'manual',demo INTEGER NOT NULL DEFAULT 0, UNIQUE(store,url));
CREATE TABLE IF NOT EXISTS prices(id INTEGER PRIMARY KEY,product_id INTEGER NOT NULL,price REAL NOT NULL,observed_at TEXT NOT NULL, FOREIGN KEY(product_id) REFERENCES products(id));
CREATE TABLE IF NOT EXISTS alerts(id INTEGER PRIMARY KEY,product_id INTEGER NOT NULL,price_id INTEGER NOT NULL,kind TEXT NOT NULL,created_at TEXT NOT NULL,telegram_sent INTEGER NOT NULL DEFAULT 0, UNIQUE(price_id,kind));''')

def timestamp():return datetime.now(timezone.utc).isoformat(timespec='seconds')
def analyze(values):
 if not values:return {'discount':0,'error':False,'baseline':None}
 current=values[-1]; previous=values[:-1][-30:]
 if len(previous)<3:return {'discount':0,'error':False,'baseline':None}
 baseline=statistics.median(previous)
 discount=max(0,round((baseline-current)/baseline*100,1)) if baseline>0 else 0
 # A large drop against the observed median is a *candidate*, not proof.
 return {'discount':discount,'error':discount>=60,'baseline':round(baseline,2)}

def add_price(store,title,category,url,price,source='manual',demo=False,notify=False):
 if store not in STORES or category not in CATS:raise ValueError('Invalid store or category')
 u=urlparse(url)
 if u.scheme!='https' or not u.hostname or not (u.hostname==STORES[store] or u.hostname.endswith('.'+STORES[store])):raise ValueError('URL must be HTTPS on the selected retailer domain')
 if not isinstance(price,(int,float)) or not 0<price<10000000:raise ValueError('Invalid price')
 if not title.strip() or len(title)>250:raise ValueError('Invalid title')
 with LOCK,db() as c:
  c.execute('INSERT OR IGNORE INTO products(store,title,category,url,source,demo) VALUES(?,?,?,?,?,?)',(store,title.strip(),category,url,source,int(demo)))
  p=c.execute('SELECT id,demo FROM products WHERE store=? AND url=?',(store,url)).fetchone()
  # Demo observations must never be silently mixed with real observations.
  if bool(p['demo'])!=bool(demo):raise ValueError('Demo and real observations cannot be mixed')
  c.execute('UPDATE products SET title=?,category=? WHERE id=?',(title.strip(),category,p['id']))
  previous=[r['price'] for r in c.execute('SELECT price FROM prices WHERE product_id=? ORDER BY id',(p['id'],))]
  now=timestamp(); cur=c.execute('INSERT INTO prices(product_id,price,observed_at) VALUES(?,?,?)',(p['id'],price,now))
  assessment=analyze(previous+[price]);kind='potential_error' if assessment['error'] else 'discount'
  if assessment['discount']>=40 and not demo:
   c.execute('INSERT OR IGNORE INTO alerts(product_id,price_id,kind,created_at) VALUES(?,?,?,?)',(p['id'],cur.lastrowid,kind,now))
  return assessment

def products():
 with db() as c:
  rows=c.execute('SELECT * FROM products WHERE demo=0 ORDER BY id DESC').fetchall();out=[]
  for r in rows:
   values=c.execute('SELECT price,observed_at FROM prices WHERE product_id=? ORDER BY id',(r['id'],)).fetchall()
   if not values:continue
   a=analyze([v['price'] for v in values]);d=dict(r);d.update(a);d['price']=values[-1]['price'];d['observed_at']=values[-1]['observed_at'];d['samples']=len(values);out.append(d)
  return out

def seed():
 examples=[('Amazon México','Samsung Galaxy S25 Ultra 256GB','Phones','https://www.amazon.com.mx/dp/DEMO-S25',25999,15999),('Mercado Libre','OnePlus 13 256GB','Phones','https://www.mercadolibre.com.mx/p/DEMO-ONEPLUS',19999,11999),('Coppel','Laptop gaming RTX 4060','Laptops / PCs','https://www.coppel.com/demo-laptop',22999,12999),('Elektra','PlayStation 5','Gaming consoles','https://www.elektra.mx/demo-ps5',12999,5999),('Amazon México','Tablet Android 11 pulgadas','Tablets','https://www.amazon.com.mx/dp/DEMO-TABLET',8999,4999),('Coppel','Smart TV 55 pulgadas','TVs','https://www.coppel.com/demo-tv',14999,7999)]
 for store,title,cat,url,old,new in examples:
  for price in [old,old,old,new]:add_price(store,title,cat,url,price,source='sample',demo=True,notify=False)

# Queries are intentionally bounded to limit API usage and avoid treating unrelated variants as the same product.
SEARCHES=[('Phones','Samsung Galaxy S Ultra'),('Phones','OnePlus smartphone'),('Phones','iPhone'),('Laptops / PCs','laptop gamer'),('Laptops / PCs','PC gamer'),('Gaming consoles','PlayStation 5'),('Gaming consoles','Xbox Series X'),('Tablets','Samsung Galaxy Tab'),('TVs','Smart TV 55')]

def scan_once():
 if not ML_TOKEN:
  SCAN_STATE.update(status='authorization required',last_error='Set MERCADOLIBRE_ACCESS_TOKEN in Render Environment')
  return 0
 SCAN_STATE.update(status='scanning',last_attempt=timestamp(),last_error=None)
 total=0; errors=[]
 for category,query in SEARCHES:
  try:
   endpoint='https://api.mercadolibre.com/sites/MLM/search?'+urlencode({'q':query,'limit':15})
   req=urllib.request.Request(endpoint,headers={'Authorization':'Bearer '+ML_TOKEN,'Accept':'application/json','User-Agent':'DealRadarMX/1.3'})
   with urllib.request.urlopen(req,timeout=20) as response:payload=json.load(response)
   for item in payload.get('results',[]):
    try:
     if item.get('condition') not in (None,'new'):continue
     if item.get('currency_id')!='MXN':continue
     price=item.get('price');url=item.get('permalink');title=item.get('title','')
     if not isinstance(price,(int,float)) or not url or not title:continue
     add_price('Mercado Libre',title,category,url,price,source='mercadolibre_api')
     total+=1
    except (ValueError,TypeError) as e:errors.append(str(e))
  except Exception as e:
   errors.append(type(e).__name__+': '+str(e)[:180])
 SCAN_STATE.update(last_count=total,last_error='; '.join(errors[:3]) if errors else None,status='partial' if errors and total else ('error' if errors else 'ok'))
 if total:SCAN_STATE['last_success']=timestamp()
 return total

def scan_loop():
 while True:
  try:scan_once()
  except Exception as e:SCAN_STATE.update(status='error',last_error=str(e)[:250]);print('Scanner error:',traceback.format_exc())
  time.sleep(SCAN_INTERVAL)

HTML='''<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#0c1425"><link rel="manifest" href="/manifest.webmanifest"><title>DealRadar MX</title><style>
:root{font-family:system-ui,-apple-system,Segoe UI,sans-serif;color:#eaf0ff;background:#0b1220}*{box-sizing:border-box}body{margin:0}main{max-width:1150px;margin:auto;padding:22px}header,.between{display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap}header{padding:12px 0 25px}h1{font-size:25px;margin:0}h2{font-size:19px}p,small,.muted{color:#9eacc7}.logo{color:#5ee1a4}.pill{border:1px solid #315847;background:#14372c;color:#75f5ba;padding:7px 11px;border-radius:30px;font-size:12px}.stats{display:grid;grid-template-columns:repeat(3,1fr);gap:12px}.stat,.panel,.item{border:1px solid #28374f;background:#131f32;border-radius:16px;padding:18px}.stat strong{display:block;font-size:27px;margin-top:8px}.toolbar{display:flex;flex-wrap:wrap;gap:10px;margin:22px 0}select,input,button{background:#192941;color:#eaf0ff;border:1px solid #34455e;border-radius:9px;padding:11px;font:inherit}button{cursor:pointer}button:hover{border-color:#67dfab}.grid{display:grid;grid-template-columns:repeat(2,1fr);gap:12px}.item h3{margin:0 0 7px}.price{font-size:25px;font-weight:750;color:#71f2b2}.off{color:#71f2b2;font-weight:700}.warning{color:#ffbd69}.row{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:wrap}a{color:#8cc4ff}.item p{margin:8px 0}.tag{font-size:12px;color:#b9c7df}.panel{margin-top:20px}.formgrid{display:grid;grid-template-columns:repeat(2,1fr);gap:9px}.formgrid input,.formgrid select{width:100%}.full{grid-column:1/-1}.notice{background:#342a18;color:#f7d69b;border-radius:10px;padding:12px;margin:16px 0}.empty{padding:30px;color:#9eacc7;text-align:center}svg{width:100%;height:90px}footer{margin:30px 0;color:#8c9ab2;font-size:12px}@media(max-width:680px){.grid,.formgrid{grid-template-columns:1fr}.stats{grid-template-columns:1fr 1fr}.stat strong{font-size:22px}main{padding:14px}}
</style></head><body><main><header><div><h1><span class="logo">◉</span> DealRadar MX</h1><small>Electronics deal intelligence · Mexico</small></div><span class="pill">● Live tracking dashboard</span></header><div class="stats"><div class="stat"><small>Products monitored</small><strong id="count">—</strong></div><div class="stat"><small>Deals ≥ 40%</small><strong id="deals">—</strong></div><div class="stat"><small>Potential errors</small><strong id="errors">—</strong></div></div><div class="notice" id="notice">Checking scanner configuration…</div><div class="toolbar"><input id="q" placeholder="Search products or brands…" aria-label="Search"><select id="category"><option value="">All categories</option><option>Phones</option><option>Laptops / PCs</option><option>Gaming consoles</option><option>Tablets</option><option>TVs</option></select><select id="store"><option value="">All stores</option><option>Amazon México</option><option>Mercado Libre</option><option>Coppel</option><option>Elektra</option></select><select id="kind"><option value="">All products</option><option value="deal">40%+ deals</option><option value="error">Potential errors</option></select></div><div class="between"><h2>Price observations</h2><small id="scanner"></small><small id="shown"></small></div><div id="items" class="grid"></div><section class="panel" style="display:none"><h2>Add a verified price observation</h2><p>Enter the actual product page URL and a price you verified. Add at least 3 historical observations before a later drop can be classified. Prices are in MXN.</p><form id="form" class="formgrid"><input name="title" placeholder="Exact product name / variant" required maxlength="250"><select name="store" required><option>Amazon México</option><option>Mercado Libre</option><option>Coppel</option><option>Elektra</option></select><select name="category"><option>Phones</option><option>Laptops / PCs</option><option>Gaming consoles</option><option>Tablets</option><option>TVs</option></select><input name="price" type="number" min="1" max="9999999" step="0.01" placeholder="Price MXN" required><input class="full" name="url" type="url" placeholder="https://www.retailer.com/product..." required><button class="full">Save price observation</button></form><p id="status" role="status"></p></section><footer>Threshold: 40% vs median of up to 30 previous observations. Potential error: 60%+ drop. Candidate only — not a confirmed pricing mistake. Live listings require authorized retailer access. Dashboard refresh: 30 seconds. Price history may reset on free ephemeral hosting.</footer></main><script>
let all=[];const money=n=>new Intl.NumberFormat('es-MX',{style:'currency',currency:'MXN',maximumFractionDigits:0}).format(n);const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
function render(){let q=document.querySelector('#q').value.toLowerCase(),cat=document.querySelector('#category').value,store=document.querySelector('#store').value,kind=document.querySelector('#kind').value;let rows=all.filter(p=>(p.title+' '+p.store).toLowerCase().includes(q)&&(!cat||p.category===cat)&&(!store||p.store===store)&&(!kind||(kind==='deal'?p.discount>=40:p.error)));document.querySelector('#shown').textContent=rows.length+' shown';document.querySelector('#items').innerHTML=rows.map(p=>`<article class="item"><div class="row"><span class="tag">${esc(p.store)} · ${esc(p.category)}</span>${p.demo?'<span class="pill">DEMO</span>':''}</div><h3>${esc(p.title)}</h3><p class="price">${money(p.price)}</p><div class="row"><span class="${p.error?'warning':'off'}">${p.baseline===null?'Building price history':p.error?'⚠ Potential pricing error · -'+p.discount+'%':p.discount>=40?'↓ '+p.discount+'% vs history':'Change vs history: '+p.discount+'%'}</span><small>${p.samples} samples</small></div><p><small>Historical median: ${p.baseline===null?'not established':money(p.baseline)}</small></p><a href="${esc(p.url)}" target="_blank" rel="noopener noreferrer">View retailer page ↗</a> <button data-history="${p.id}">Price history</button><div id="history-${p.id}"></div></article>`).join('')||'<div class="empty">No matching products yet.</div>';document.querySelectorAll('[data-history]').forEach(b=>b.onclick=()=>history(b.dataset.history));}
async function history(id){let r=await fetch('/api/history?id='+encodeURIComponent(id));let j=await r.json();let el=document.getElementById('history-'+id);el.innerHTML='<p><small>'+j.history.map(x=>money(x.price)+' ('+x.observed_at.slice(0,10)+')').join(' → ')+'</small></p>';}
async function load(){let r=await fetch('/api/products');all=await r.json();document.querySelector('#count').textContent=all.length;document.querySelector('#deals').textContent=all.filter(x=>x.discount>=40).length;document.querySelector('#errors').textContent=all.filter(x=>x.error).length;let sr=await fetch('/api/status',{cache:'no-store'});if(!sr.ok)throw new Error('Scanner status unavailable (HTTP '+sr.status+')');let st=await sr.json();document.querySelector('#notice').textContent=st.status==='ok'?'Mercado Libre scan active. Other retailers awaiting authorized integrations.':st.status==='partial'?'Some listings imported; scanner reported errors. Check status below.':'Live prices unavailable: '+(st.last_error||st.status)+'. No sample deals are displayed.';document.querySelector('#scanner').textContent='Scanner: '+st.status+' · Last successful scan: '+(st.last_success||'never')+' · Last scan items: '+st.last_count;render();}
['q','category','store','kind'].forEach(id=>document.getElementById(id).addEventListener('input',render));document.querySelector('#form').onsubmit=async e=>{e.preventDefault();let data=Object.fromEntries(new FormData(e.target));data.price=Number(data.price);let r=await fetch('/api/observations',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)});let j=await r.json();document.querySelector('#status').textContent=r.ok?'Observation saved.':j.error||'Error';if(r.ok){e.target.reset();load();}};load().catch(e=>{document.querySelector('#notice').textContent='Dashboard error: '+e.message});setInterval(()=>load().catch(e=>{document.querySelector('#notice').textContent='Dashboard error: '+e.message}),30000);if('serviceWorker'in navigator)navigator.serviceWorker.register('/sw.js').catch(()=>{});
</script></body></html>'''

class Handler(BaseHTTPRequestHandler):
 def send(self,status,data,typ='application/json'):
  body=data.encode() if isinstance(data,str) else json.dumps(data,ensure_ascii=False).encode()
  self.send_response(status);self.send_header('Content-Type',typ+'; charset=utf-8');self.send_header('Content-Length',str(len(body)));self.send_header('Cache-Control','no-store');self.end_headers();self.wfile.write(body)
 def do_GET(self):
  p=urlparse(self.path)
  if p.path=='/':
   page=HTML
   return self.send(200,page,'text/html')
  if p.path=='/api/products':return self.send(200,products())
  if p.path=='/api/status':return self.send(200,dict(SCAN_STATE))
  if p.path=='/api/history':
   try:pid=int(parse_qs(p.query)['id'][0])
   except (KeyError,ValueError,IndexError):return self.send(400,{'error':'Invalid id'})
   with db() as c: rows=[dict(x) for x in c.execute('SELECT price,observed_at FROM prices WHERE product_id=? ORDER BY id',(pid,))]
   return self.send(200,{'history':rows})
  if p.path=='/manifest.webmanifest':return self.send(200,{'name':'DealRadar MX','short_name':'DealRadar','start_url':'/','display':'standalone','background_color':'#0b1220','theme_color':'#0c1425','icons':[]},'application/manifest+json')
  if p.path=='/sw.js':return self.send(200,"self.addEventListener('install',e=>self.skipWaiting());self.addEventListener('activate',e=>e.waitUntil(self.clients.claim()));",'application/javascript')
  self.send(404,{'error':'Not found'})
 def do_POST(self):
  if not os.environ.get('DEALRADAR_WRITE_TOKEN'):return self.send(403,{'error':'Manual observations disabled'})
  import hmac
  if not hmac.compare_digest(self.headers.get('X-DealRadar-Token',''),os.environ['DEALRADAR_WRITE_TOKEN']):return self.send(403,{'error':'Unauthorized'})
  if self.path!='/api/observations':return self.send(404,{'error':'Not found'})
  try:
   length=int(self.headers.get('Content-Length','0'))
   if length>10000:raise ValueError('Payload too large')
   x=json.loads(self.rfile.read(length));a=add_price(x['store'],x['title'],x['category'],x['url'],float(x['price']))
   return self.send(201,{'ok':True,**a})
  except (ValueError,KeyError,TypeError,OverflowError) as e:return self.send(400,{'error':str(e)})

if __name__=='__main__':
 import sys
 init()
 # Do not seed fictional products on a public deployment.
 threading.Thread(target=scan_loop,daemon=True,name='mercadolibre-scanner').start()
 print(f'DealRadar MX: http://localhost:{PORT}')
 ThreadingHTTPServer(('0.0.0.0',PORT),Handler).serve_forever()
