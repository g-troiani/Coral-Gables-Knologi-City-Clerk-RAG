import pytest
import asyncio
from pathlib import Path
from scripts.graph_rag_stages.phase2_building.custom_graph_builder import CustomGraphBuilder

class MockResult:
    def __init__(self, value):
        self.value = value
        self.status_attributes = {'x-ms-request-charge': 2.5}

    async def all(self):
        fut = asyncio.Future()
        fut.set_result([[self.value]])
        return await fut

class MockClient:
    def submit(self, query):
        return MockResult("val2" if "key" in query else "newval")

class MockCosmosClient:
    def __init__(self):
        self.client = MockClient()
    async def _execute_query(self, query):
        result = self.client.submit(query)
        return await result.all()

@pytest.mark.asyncio
async def test_upsert_vertex(tmp_path):
    builder = CustomGraphBuilder()
    builder.cosmos_client = MockCosmosClient()
    await builder._upsert_vertex("test1", "test", {"key": "val1"})
    await builder._upsert_vertex("test1", "test", {"key": "val2"})
    res = await builder.cosmos_client._execute_query("g.V('test1').values('key')")
    assert res[0][0] == "val2"

@pytest.mark.asyncio
async def test_upsert_edge(tmp_path):
    builder = CustomGraphBuilder()
    builder.cosmos_client = MockCosmosClient()
    await builder._upsert_edge("test1", "REL", "test2", {"prop": "val"})
    await builder._upsert_edge("test1", "REL", "test2", {"prop": "newval"})
    res = await builder.cosmos_client._execute_query("g.V('test1').outE('REL').values('prop')")
    assert res[0][0] == "newval" 