"""Streaming exact comparison; wall-clock receive time and JSON object order are excluded."""
import hashlib
import json
import sqlite3


def compare_databases(left, right, compare_transport=True, *, read_only=False):
    dbs=[sqlite3.connect(p.resolve().as_uri()+'?mode=ro', uri=True) if read_only else sqlite3.connect(p)
         for p in (left,right)]
    counts={'valid':0,'invalid':0};digest=hashlib.sha256();transport=hashlib.sha256()
    try:
        for db in dbs:
            db.execute('BEGIN')
        sql='SELECT topic,partition_id,offset_id,kafka_timestamp_ms,raw_base64,status,error,normalized_json,raw_is_null FROM records ORDER BY topic,partition_id,offset_id'
        queries=[]
        for db in dbs:
            columns={row[1] for row in db.execute('PRAGMA table_info(records)')}
            extra=','.join(name if name in columns else 'NULL' for name in ('kafka_key_base64','kafka_headers_json'))
            queries.append(sql.replace(' FROM records',','+extra+' FROM records'))
        cursors=[db.execute(query) for db,query in zip(dbs,queries)]
        while True:
            a,b=[c.fetchone() for c in cursors]
            if a is None and b is None:break
            assert a is not None and b is not None, 'record count differs'
            assert a[:7]==b[:7] and a[8]==b[8], f'raw/location/status/error mismatch at {a[:3]}'
            x,y=[json.loads(r[7]) if r[7] is not None else None for r in (a,b)]
            if x!=y:
                def difference(x,y,path=''):
                    if isinstance(x,dict) and isinstance(y,dict):
                        for k in sorted(set(x)|set(y)):
                            if x.get(k)!=y.get(k):return difference(x.get(k),y.get(k),path+'.'+k)
                    if isinstance(x,list) and isinstance(y,list):
                        for i,(u,v) in enumerate(zip(x,y)):
                            if u!=v:return difference(u,v,path+f'[{i}]')
                    return f'{path}: {x!r} != {y!r}'
                raise AssertionError(f'normalized mismatch at {a[:3]}: {difference(x,y)}')
            if compare_transport:
                metadata=[[r[9],json.loads(r[10]) if r[10] is not None else None] for r in (a,b)]
                assert metadata[0]==metadata[1], f'transport metadata mismatch at {a[:3]}'
                transport.update(json.dumps([*a[:3],*metadata[0]],sort_keys=True,ensure_ascii=False).encode())
            counts[a[5]]+=1
            digest.update(json.dumps([*a[:7],x,a[8]],sort_keys=True,ensure_ascii=False).encode())
        sql='SELECT mmsi,dynamic_time,dynamic_json,static_time,static_json FROM latest_ship ORDER BY mmsi'
        cursors=[db.execute(sql) for db in dbs];ships=0
        while True:
            a,b=[c.fetchone() for c in cursors]
            if a is None and b is None:break
            assert a is not None and b is not None, 'ship count differs'
            def normalized(r):return [r[0],r[1],json.loads(r[2]) if r[2] else None,r[3],json.loads(r[4]) if r[4] else None]
            x,y=normalized(a),normalized(b)
            assert x==y,f'ship snapshot mismatch for {a[0]}'
            digest.update(json.dumps(x,sort_keys=True,ensure_ascii=False).encode());ships+=1
        return {'records':sum(counts.values()),'statuses':counts,'ships':ships,'semantic_sha256':digest.hexdigest(),
                'raw_locations_errors_normalized_snapshots':'PASS','transport_metadata_parity':'PASS' if compare_transport else 'not compared against legacy consumer',
                'transport_semantic_sha256':transport.hexdigest() if compare_transport else None,
                'excluded':'received_at differs by execution time; JSON object key order is irrelevant; no numeric tolerance used'}
    finally:
        for db in dbs:db.close()
