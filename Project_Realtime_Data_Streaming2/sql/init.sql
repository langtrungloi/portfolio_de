CREATE TABLE sensor_data (
    id SERIAL PRIMARY KEY,
    sensor_id TEXT,
    temperature FLOAT,
    humidity FLOAT,
    created_at TIMESTAMP
);