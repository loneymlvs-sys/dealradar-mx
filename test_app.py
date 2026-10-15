import os,sys,tempfile,unittest
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import app
class TestApp(unittest.TestCase):
 def test_threshold(self):
  self.assertEqual(app.analyze([100,100,100,60])['discount'],40)
  self.assertTrue(app.analyze([100,100,100,39])['error'])
  self.assertIsNone(app.analyze([100,60])['baseline'])
 def test_reject_domain(self):
  with self.assertRaises(ValueError):app.add_price('Amazon México','Test','Phones','https://amazon.com.mx.evil.test/x',100)
 def test_history(self):
  with tempfile.TemporaryDirectory() as d:
   original=app.DB
   try:
    app.DB=Path(d)/'test.sqlite3';app.init()
    for n in (100,100,100,50):app.add_price('Coppel','Test TV','TVs','https://www.coppel.com/valid',n,notify=False)
    p=app.products()[0];self.assertEqual(p['discount'],50);self.assertEqual(p['samples'],4)
   finally:app.DB=original
if __name__=='__main__':unittest.main()
