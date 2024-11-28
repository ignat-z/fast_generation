CREATE TABLE "metrics"(
    created     timestamp with time zone default now() not null,
    sensor_id   integer                                not null,
    temperature numeric                                not null
);