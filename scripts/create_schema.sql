-- ============================================================
-- postgres-cache — DDL do schema de cache
-- Target Data Model: _reversa_sdd/migration/target_data_model.md
--
-- Uso:
--   Substitua :schema e :table antes de executar, ou execute via
--   DatabaseOperations.ensure_table_exists() que aplica pré-formatação.
--
-- Parâmetros configuráveis:
--   {schema}   — nome do schema PostgreSQL (ex: "public")
--   {table}    — nome da tabela de cache (ex: "cache")
--   {unlogged} — "UNLOGGED" quando use_wal=False (padrão), "" quando use_wal=True
-- ============================================================

-- Criar schema se não existir (idempotente)
CREATE SCHEMA IF NOT EXISTS {schema};

-- Tabela de cache
-- UNLOGGED por padrão (use_wal=False): maior performance, não sobrevive a crash.
-- Remova UNLOGGED (use_wal=True) para durabilidade completa com WAL.
CREATE UNLOGGED TABLE IF NOT EXISTS {schema}.{table} (
    -- Chave do cache — VARCHAR(449) com collation binária para
    -- comparação byte-a-byte consistente (equivalente ao legado C#).
    id                          VARCHAR(449) COLLATE "C"  NOT NULL,

    -- Valor serializado pelo chamador — a biblioteca não interpreta o conteúdo.
    value                       BYTEA                     NOT NULL,

    -- Timestamp absoluto de expiração (UTC obrigatório — TIMESTAMPTZ).
    -- Nunca naive timestamp. Equivalente a DateTimeOffset do .NET.
    expiresattime               TIMESTAMPTZ               NOT NULL,

    -- Duração de sliding expiration em segundos inteiros.
    -- NULL indica que a entrada não tem sliding expiration.
    -- Derivado de TimeSpan.TotalSeconds no legado C#.
    slidingexpirationinseconds  BIGINT                    NULL,

    -- Teto absoluto da sliding expiration (UTC).
    -- NULL quando não há limite absoluto.
    absoluteexpiration          TIMESTAMPTZ               NULL,

    CONSTRAINT pk_{table} PRIMARY KEY (id)
);

-- Índice para o scanner de expiração de background.
-- Permite DELETE WHERE expiresattime < now() eficiente sem seqscan.
-- deduplicate_items=True economiza espaço em B-tree (PostgreSQL 13+).
CREATE INDEX IF NOT EXISTS ix_expiresattime
    ON {schema}.{table} (expiresattime)
    WITH (deduplicate_items = True);

-- ============================================================
-- Notas de compatibilidade:
--   - Schema IDÊNTICO ao legado Microsoft.Extensions.Caching.Postgres (C#).
--   - Nomes de coluna em lowercase (psycopg2 não faz case-folding automático).
--   - Placeholder psycopg2: %s  (legado usava @param / NpgsqlParameter).
--   - datetime(tz=timezone.utc) em Python ↔ DateTimeOffset.UtcNow no .NET.
-- ============================================================
