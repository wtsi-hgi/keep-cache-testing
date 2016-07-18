# Arvaodos Keep Cache Test
1. Start [simulator that generates block usage patterns](https://github.com/wtsi-hgi/cache-usage-simulator).
2. Install dependencies with 
```bash
$ pip2 install -r requirements.txt
```
2. Setup `keepcachetest/run.py` (set HTTP API location, block accesses, type of block store, etc.) - sorry, no polished CLI with arguments at the moment!
3. Run to generate usage records as JSON:
```bash
$ PYTHONPATH=. python2 keepcachetest/run.py
```