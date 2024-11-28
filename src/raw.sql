INSERT INTO metrics (
SELECT
    (NOW() + seq / 100 * interval '1 MILLISECOND') AS created,
    (seq % 32 + 1)::integer AS sensor_id,
    20.0 + RANDOM() * 500 / 100 as temperature
    FROM GENERATE_SERIES(1, 10_000_000) seq
);
