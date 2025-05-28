CREATE TABLE IF NOT EXISTS city_clerk_documents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    document_type TEXT CHECK (document_type IN ('Resolution', 'Ordinance', 'Proclamation', 'Contract', 'Meeting Minutes', 'Agenda')),
    title TEXT,
    date TEXT,
    year INTEGER,
    month INTEGER CHECK (month >= 1 AND month <= 12),
    day INTEGER CHECK (day >= 1 AND day <= 31),
    mayor TEXT,
    vice_mayor TEXT,
    commissioners TEXT[],
    city_attorney TEXT,
    city_manager TEXT,
    city_clerk TEXT,
    public_works_director TEXT,
    agenda TEXT,
    source_pdf TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Add keywords field to city_clerk_documents
ALTER TABLE city_clerk_documents 
ADD COLUMN IF NOT EXISTS keywords TEXT[] DEFAULT '{}';

-- Rename research_chunks to documents_chunks
ALTER TABLE research_chunks RENAME TO documents_chunks;

-- Update any indexes that reference the old name
ALTER INDEX IF EXISTS idx_research_chunks_document_id RENAME TO idx_documents_chunks_document_id;
ALTER INDEX IF EXISTS idx_research_chunks_embedding RENAME TO idx_documents_chunks_embedding;

-- Update RPC function if it exists
CREATE OR REPLACE FUNCTION match_documents_chunks(
    query_embedding vector(1536),
    match_threshold float,
    match_count int
)
RETURNS TABLE (
    id uuid,
    document_id uuid,
    text text,
    metadata jsonb,
    similarity float
)
LANGUAGE sql STABLE
AS $$
    SELECT
        documents_chunks.id,
        documents_chunks.document_id,
        documents_chunks.text,
        documents_chunks.metadata,
        1 - (documents_chunks.embedding <=> query_embedding) as similarity
    FROM documents_chunks
    WHERE 1 - (documents_chunks.embedding <=> query_embedding) > match_threshold
    ORDER BY documents_chunks.embedding <=> query_embedding
    LIMIT match_count;
$$;

-- If you have the old function, drop it
DROP FUNCTION IF EXISTS match_research_chunks(vector(1536), float, int); 