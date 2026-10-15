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
