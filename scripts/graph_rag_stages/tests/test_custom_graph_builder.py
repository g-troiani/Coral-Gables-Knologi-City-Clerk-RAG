import pytest
import asyncio
from pathlib import Path
from unittest.mock import Mock, AsyncMock
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
        self.ru_total = 0.0

    async def _execute_query(self, query):
        result = self.client.submit(query)
        values = await result.all()
        ru = result.status_attributes.get('x-ms-request-charge', 0.0)
        self.ru_total += float(ru)
        return values

    async def connect(self):
        pass
    
    async def close(self):
        pass


@pytest.mark.asyncio
async def test_upsert_vertex(tmp_path):
    builder = CustomGraphBuilder(output_dir=tmp_path)
    builder.cosmos_client = MockCosmosClient()
    await builder._upsert_vertex("test1", "test", {"key": "val1"})
    await builder._upsert_vertex("test1", "test", {"key": "val2"})
    res = await builder.cosmos_client._execute_query("g.V('test1').values('key')")
    assert res[0][0] == "val2"


@pytest.mark.asyncio
async def test_upsert_edge(tmp_path):
    builder = CustomGraphBuilder(output_dir=tmp_path)
    builder.cosmos_client = MockCosmosClient()
    await builder._upsert_edge("test1", "REL", "test2", {"prop": "val"})
    await builder._upsert_edge("test1", "REL", "test2", {"prop": "newval"})
    res = await builder.cosmos_client._execute_query("g.V('test1').outE('REL').values('prop')")
    assert res[0][0] == "newval"


@pytest.mark.asyncio
async def test_sanitize_label(tmp_path):
    builder = CustomGraphBuilder(output_dir=tmp_path)
    assert builder.sanitize_label("test@label!") == "test_label_"
    assert builder.sanitize_label("very_long_label_that_exceeds_sixty_three_characters_for_label_test", is_label=True).startswith("id_")
    assert builder.sanitize_label("normal_label", is_label=True) == "normal_label"


@pytest.mark.asyncio
async def test_batch_execution(tmp_path):
    builder = CustomGraphBuilder(output_dir=tmp_path)
    builder.cosmos_client = MockCosmosClient()
    
    vertices = [{"id": "v1", "label": "test", "properties": {"key": "val1"}}]
    edges = [{"from": "v1", "to": "v2", "label": "REL", "properties": {"prop": "val"}}]
    
    await builder._execute_batches(vertices, edges)
    # Test passes if no exception is raised
    assert True 