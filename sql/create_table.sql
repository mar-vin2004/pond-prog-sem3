CREATE TABLE IF NOT EXISTS working_data (
    data_ingestao DateTime,
    dado_linha String,
    tag String
) ENGINE = MergeTree()
ORDER BY data_ingestao;