import os, json, time, re, hashlib, traceback
from azure.storage.blob import BlobServiceClient
from gremlin_python.driver import client, serializer
from openai import AzureOpenAI

# ─────────────  TEST-MODE  ────────────────────────────────────
TEST_MODE, MAX_VERTICES = False, 5           # pon False cuando validado
vertex_count, early_exit = 0, False

# ─────────────  CONFIG GENERAL  ───────────────────────────────
BLOB_CONNECTION_STRING = (
    "DefaultEndpointsProtocol=https;"
    "AccountName=rasagptstorageaccount;"
    "AccountKey=[KEY_HERE];"
    "EndpointSuffix=core.windows.net"
)
COSMOS_ENDPOINT = "wss://aida-graph-db.gremlin.cosmos.azure.com:443"
COSMOS_KEY      = "[KEY_HERE]"
DATABASE, CONTAINER = "cgGraph", "cityClerk" 
PARTITION_KEY, PARTITION_VALUE = "partitionKey", "demo"

# ─────────────  NUEVO CLIENTE AZURE OPENAI  ───────────────────
aoai = AzureOpenAI(
    api_key        = [KEY HERE],
    azure_endpoint = "https://aida-gpt4o.openai.azure.com",
    api_version    = "2024-02-15-preview"
)

DEPLOYMENT_NAME = "gpt-4o"          # nombre EXACTO en Deployments

─────────────  RESTO DE CONFIG  ──────────────────────────────
ENTITY_CONTAINERS = [
    "ks-entities-person","ks-entities-organization","ks-entities-location",
    "ks-entities-address","ks-entities-phone","ks-entities-email",
    "ks-entities-url","ks-entities-event","ks-entities-product",
    "ks-entities-persontype","ks-entities-ipaddress",
    "ks-entities-quantity","ks-entities-skill"
]
CONTAINER_TO_LABEL = {
    "ks-entities-person":"Person","ks-entities-organization":"Organization","ks-entities-location":"Location",
    "ks-entities-address":"Address","ks-entities-phone":"PhoneNumber","ks-entities-email":"Email",
    "ks-entities-url":"URL","ks-entities-event":"Event","ks-entities-product":"Product",
    "ks-entities-persontype":"PersonType","ks-entities-ipaddress":"IPAddress",
    "ks-entities-quantity":"Quantity","ks-entities-skill":"Skill"
}
CHUNKS_CONTAINER = "ks-chunks-debug"

# ─────────────  HELPERS  ──────────────────────────────────────
ILLEGAL_ID_CHARS = re.compile(r'[\/\\?#]')
def clean_id(s: str)  -> str: return ILLEGAL_ID_CHARS.sub('_', s)
def clean_txt(s: str) -> str: return s.replace("'", "\\'").replace('"', "")
def limit_reached():
    global early_exit
    if TEST_MODE and vertex_count >= MAX_VERTICES:
        early_exit = True
        return True
    return False

# ─────────────  CONEXIONES  ───────────────────────────────────
print("🔗  Connecting …")
blob = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
gremlin = client.Client(
    f"{COSMOS_ENDPOINT}/gremlin","g",
    username=f"/dbs/{DATABASE}/colls/{CONTAINER}",
    password=COSMOS_KEY,
    message_serializer=serializer.GraphSONSerializersV2d0())

# ─────────────  CARGA DE CHUNKS  ──────────────────────────────
chunk_text, chunk_entities = {}, {}
print("📥  Loading chunks …")
for b in blob.get_container_client(CHUNKS_CONTAINER).list_blobs():
    if not b.name.endswith(".json"): continue
    doc = json.loads(blob.get_blob_client(CHUNKS_CONTAINER, b.name).download_blob().readall())
    raw_id = doc.get("chunkId") or doc.get("metadata_storage_path") or b.name
    cid    = clean_id(raw_id)
    chunk_text[cid] = doc.get("content") or doc.get("text") or ""
    chunk_entities[cid] = []

print("📥  Loading entities …")
for cont in ENTITY_CONTAINERS:
    label = CONTAINER_TO_LABEL[cont]
    cc    = blob.get_container_client(cont)
    for b in cc.list_blobs():
        if not b.name.endswith(".json"): continue
        data = json.loads(cc.get_blob_client(b).download_blob().readall())
        if isinstance(data, dict): data = [data]
        for e in data:
            raw_cid = e.get("chunkId") or e.get("metadata_storage_path")
            if raw_cid and raw_cid.startswith(f"{CHUNKS_CONTAINER}/"):
                raw_cid = raw_cid[len(CHUNKS_CONTAINER)+1:]
            cid = clean_id(raw_cid)
            if cid not in chunk_entities:
                print(f"⚠️  Unmatched entity → {raw_cid}")
                continue
            name = e.get("text") or e.get("name")
            vid  = f"{label}:{hashlib.sha1(name.encode()).hexdigest()}"
            chunk_entities[cid].append({"id":vid,"label":label,"name":name})

print(f"➡️  Prepared {len(chunk_entities)} chunks.")

PROMPT = """You are a knowledge-graph extractor.
Return only factual triples (pure JSON):
[{{"source":"<id>","relation":"<label>","target":"<id>"}}]

TEXT:
\"\"\"{chunk}\"\"\"

ENTITIES (pairs [id, name]):
{ents}
"""

# ─────────────  MAIN LOOP  ────────────────────────────────────
for cid, ents in chunk_entities.items():
    if early_exit: break
    if not ents:   continue

    print(f"\n🚩  Chunk {cid[:60]}  ({len(ents)} entities)")
    text      = chunk_text[cid][:3000]
    ents_json = [[e["id"], e["name"]] for e in ents]

    # Llamada LLM
    # ── LLM call + robust JSON parse ───────────────────────────
    try:
        rsp = aoai.chat.completions.create(
            model       = DEPLOYMENT_NAME,     # "gpt-4o"
            temperature = 0.0,
            max_tokens  = 256,
            messages = [
                {"role":"system","content":
                "You are a knowledge-graph extractor. "
                "Return only factual triples in valid JSON."},
                {"role":"user","content":
                PROMPT.format(chunk=text, ents=json.dumps(ents_json))}
            ]
        )

        raw = (rsp.choices[0].message.content or "").strip()
        print("🧠  RAW reply:", raw[:120].replace("\n"," ") + ("…" if len(raw) > 120 else ""))

        if not raw:
            print("⚠️  Empty response (content filter?).")
            triples = []

        else:
            try:
                triples = json.loads(raw)
                print(f"🧠  Parsed {len(triples)} triples")
            except json.JSONDecodeError as je:
                print("⚠️  JSONDecodeError:", je)
                print("⚠️  Full reply kept for manual inspection:")
                print(raw)
                triples = []

    except Exception as ex:
        print("❌  LLM call failed:", ex)
        triples = []


    ts = int(time.time()*1000)

    # Chunk vertex
    if not limit_reached():
        gremlin.submit(
            f"g.V('{cid}').fold().coalesce(unfold(),"
            f"addV('Chunk').property(id,'{cid}')"
            f".property('{PARTITION_KEY}','{PARTITION_VALUE}'))").all()

    # Entity vertices & MENTIONS
    for e in ents:
        if limit_reached(): break
        try:
            gremlin.submit(
                f"g.V('{e['id']}').fold().coalesce(unfold(),"
                f"addV('{e['label']}').property(id,'{e['id']}')"
                f".property('name','{clean_txt(e['name'])}')"
                f".property('{PARTITION_KEY}','{PARTITION_VALUE}'))").all()
            gremlin.submit(
                f"g.V('{cid}').coalesce("
                f"outE('MENTIONS').where(inV().hasId('{e['id']}')),"
                f"addE('MENTIONS').to(g.V('{e['id']}')).property('ts',{ts}))").all()
            vertex_count += 1
            print(f"   ✔︎ {e['id']}")
        except Exception:
            print("⚠️  Vert/Edge error\n", traceback.format_exc())

    # Semantic edges
    if not limit_reached():
        for t in triples:
            s,r,d = t.get("source"), t.get("relation"), t.get("target")
            if not (s and r and d): continue
            try:
                gremlin.submit(
                    f"g.V('{s}').coalesce("
                    f"outE('{r}').where(inV().hasId('{d}')),"
                    f"addE('{r}').to(g.V('{d}')))").all()
                print(f"   ⇢ {s} -[{r}]-> {d}")
            except Exception:
                print("⚠️  Edge error\n", traceback.format_exc())

    if early_exit:
        print(f"🛑  Reached {MAX_VERTICES} vertices (TEST).")
        break

    time.sleep(0.05)

print("\n🏁  Finished.")
gremlin.close()
