-- Drop existing tables if they exist
DROP TABLE IF EXISTS datasets;
DROP TABLE IF EXISTS projects;

-- Create the main table for storing data
CREATE TABLE projects (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    catalog VARCHAR(255) NOT NULL,
    schema VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE datasets (
    id SERIAL PRIMARY KEY,
    project_id INTEGER REFERENCES projects(id) NOT NULL,
    name VARCHAR(255) NOT NULL,
    source_type VARCHAR(64) NOT NULL CHECK (source_type IN ('static_table', 'dynamic_table', 'feature_lookup')),
    eol_definition TEXT[] DEFAULT NULL,
    feature_lookup_definition TEXT[] DEFAULT NULL,
    source_table TEXT[] DEFAULT NULL,
    timestamp_col VARCHAR(255) DEFAULT NULL,
    evaluation_type VARCHAR(64) NOT NULL CHECK (evaluation_type IN ('random', 'table', 'timestamp')),
    percentage NUMERIC DEFAULT NULL,
    eval_table_name VARCHAR(255) DEFAULT NULL,
    split_time_column VARCHAR(255) DEFAULT NULL,
    materialized BOOLEAN NOT NULL DEFAULT FALSE,
    training_table_name VARCHAR(255) DEFAULT NULL,
    eval_table_name_generated VARCHAR(255) DEFAULT NULL,
    target VARCHAR(255) DEFAULT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);