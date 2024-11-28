use byteorder::{BigEndian, WriteBytesExt};
use chrono::{DateTime, Duration, NaiveDate, Utc};
use postgres::{Client, NoTls};
use postgres_types::ToSql;
use rand::prelude::*;
use rust_decimal::Decimal;
use std::{
    io::{Cursor, Write},
    str::FromStr,
};

type Row = (DateTime<Utc>, i32, f64);

use once_cell::sync::Lazy;

static POSTGRES_EPOCH: Lazy<DateTime<Utc>> = Lazy::new(|| {
    NaiveDate::from_ymd_opt(2000, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
});

const BATCH_SIZE: usize = 10_000;
const BATCH_COUNT: usize = 1_000;
const MAX_SENSORS: i32 = 32;
const REPORT_COUNT: i64 = 100;

struct ExecutionContext {
    t0: DateTime<Utc>,
    s0: i64,
    client: Client,
    name: String,
}

impl ExecutionContext {
    fn new(name: &str, conn_info: &str) -> Self {
        let mut client = Client::connect(conn_info, NoTls).unwrap();
        let s0 = Self::table_size(&mut client);
        let t0 = Utc::now();
        let name = name.to_string();

        ExecutionContext {
            t0,
            s0,
            client,
            name,
        }
    }

    fn table_size(client: &mut Client) -> i64 {
        let row = client
            .query_one(
                "SELECT pg_total_relation_size('public.metrics') as size",
                &[],
            )
            .unwrap();
        row.get("size")
    }

    fn convert_bytes(bytes: f64, to: &str) -> f64 {
        let units = ["B", "KB", "MB", "GB", "TB", "PB"];
        let index = units
            .iter()
            .position(|&r| r == to.to_uppercase())
            .unwrap_or(0);
        bytes / (1024f64.powi(index as i32))
    }
}

impl Drop for ExecutionContext {
    fn drop(&mut self) {
        let s1 = Self::table_size(&mut self.client);
        let t1 = Utc::now();

        let duration = (t1 - self.t0).num_seconds();
        let size = s1 - self.s0;
        let speed = (size as f64) / (duration as f64);

        println!();
        println!("{}:", self.name);
        println!("Speed: {:.2}MB/s", Self::convert_bytes(speed, "MB"));
        println!(" Data: {:.2}MB", Self::convert_bytes(size as f64, "MB"));
        println!(" Time: {:.2}s", duration);
    }
}

fn generate_batch(created: DateTime<Utc>, sensor_id: i32, base_temp: f64) -> (Vec<Row>, i32) {
    let mut rng = rand::thread_rng();
    let mut current_sensor_id = sensor_id;
    let batch: Vec<_> = (0..BATCH_SIZE)
        .map(|i| {
            current_sensor_id = (current_sensor_id + (i as i32)) % MAX_SENSORS + 1;
            let temperature = ((base_temp + rng.gen_range(-5.0..5.0)) * 100.0).round() / 100.0;
            (created, current_sensor_id, temperature)
        })
        .collect();
    (batch, sensor_id)
}

fn generate_data(
    start_time: DateTime<Utc>,
    base_temp: f64,
    batch_count: usize,
) -> impl Iterator<Item = (Vec<Row>, i64)> {
    let mut current_time = start_time;
    let mut sensor_id = 1;
    let mut current_tick = 0;

    (0..batch_count).flat_map(move |_| {
        current_tick += 1;
        current_time += Duration::milliseconds(100);
        let (new_batch, new_sensor_id) = generate_batch(current_time, sensor_id, base_temp);
        sensor_id = new_sensor_id;

        std::iter::once((new_batch, current_tick))
    })
}

fn f64_to_decimal(value: f64) -> Decimal {
    Decimal::from_str(&value.to_string()).unwrap_or_else(|_| Decimal::new(0, 0))
}

fn numeric_to_postgres_binary(value: f64) -> Vec<u8> {
    let mut buffer = Vec::new();
    let abs_value = value.abs();
    let sign = if value.is_sign_negative() {
        0x4000
    } else {
        0x0000
    };
    let parts: Vec<String> = abs_value.to_string().split('.').map(String::from).collect();

    let integer = &parts[0];
    let fraction = if parts.len() > 1 {
        parts[1].clone()
    } else {
        String::new()
    };

    let weight = ((integer.len() as i16 - 1) / 4) as i16;
    let padding = if integer.len() % 4 != 0 {
        4 - (integer.len() % 4)
    } else {
        0
    };
    let padded_number = format!(
        "{:0>width$}{}",
        integer,
        fraction,
        width = integer.len() + padding
    );

    let digits: Vec<i16> = padded_number
        .as_bytes()
        .chunks(4)
        .map(|c| {
            let mut value = 0;
            for (i, &digit) in c.iter().enumerate() {
                value += (digit - b'0') as i16 * 10i16.pow(3 - i as u32);
            }
            value as i16
        })
        .collect();
    let ndigits = digits.len() as i16;
    let dscale = fraction.len() as i16;

    buffer.write_i16::<BigEndian>(ndigits).unwrap();
    buffer.write_i16::<BigEndian>(weight).unwrap();
    buffer.write_i16::<BigEndian>(sign).unwrap();
    buffer.write_i16::<BigEndian>(dscale).unwrap();

    for digit in digits {
        buffer.write_i16::<BigEndian>(digit).unwrap();
    }

    buffer
}

fn datetime_to_postgres_binary(datetime: DateTime<Utc>) -> i64 {
    let time_delta = datetime - POSTGRES_EPOCH.to_utc();
    time_delta.num_microseconds().unwrap()
}

fn generate_buffer(batch_data: &[Row]) -> anyhow::Result<Vec<u8>> {
    let mut buffer = Cursor::new(Vec::new());
    buffer.write_all(b"PGCOPY\n\xff\r\n\0")?;
    buffer.write_i32::<BigEndian>(0)?;
    buffer.write_i32::<BigEndian>(0)?;

    for row in batch_data {
        buffer.write_i16::<BigEndian>(3)?;

        // created
        let micros = datetime_to_postgres_binary(row.0);
        buffer.write_i32::<BigEndian>(8)?;
        buffer.write_i64::<BigEndian>(micros)?;

        // sensor_id
        buffer.write_i32::<BigEndian>(4)?;
        buffer.write_i32::<BigEndian>(row.1)?;

        // temperature
        let numeric_bytes = numeric_to_postgres_binary(row.2);
        buffer.write_i32::<BigEndian>(numeric_bytes.len() as i32)?;
        buffer.write_all(&numeric_bytes)?;
    }

    buffer.write_i16::<BigEndian>(-1)?;
    Ok(buffer.into_inner())
}

fn insert_to_postgres(
    client: &mut Client,
    table_name: &str,
    batch_data: &[Row],
    current_tick: i64,
) {
    let mut tx = client.transaction().unwrap();
    let stmt = tx
        .prepare(&format!("INSERT INTO {} VALUES ($1, $2, $3)", table_name))
        .unwrap();

    for row in batch_data {
        let params: [&(dyn ToSql + Sync); 3] = [&row.0, &row.1, &f64_to_decimal(row.2)];
        tx.execute(&stmt, &params).unwrap();
    }

    tx.commit().unwrap();

    if current_tick % REPORT_COUNT == 0 {
        println!("Copied {current_tick}");
    }
}

fn copy_to_postgres(client: &mut Client, table_name: &str, batch_data: &[Row], current_tick: i64) {
    let buffer = generate_buffer(batch_data).unwrap();
    let mut writer = client
        .copy_in(&format!("COPY {} FROM STDIN WITH BINARY", table_name))
        .unwrap();
    writer.write_all(&buffer).unwrap();
    writer.finish().unwrap();

    if current_tick % REPORT_COUNT == 0 {
        println!("Copied {current_tick}");
    }
}

fn insert_to_postgres_string(
    client: &mut Client,
    table_name: &str,
    batch_data: &[Row],
    current_tick: i64,
) {
    let tuples = batch_data
        .into_iter()
        .map(|row| {
            format!(
                "('{}'::timestamp with time zone, {}, {}::numeric(10, 2))",
                row.0, row.1, row.2
            )
        })
        .collect::<Vec<_>>()
        .join(",");
    let query = format!("INSERT INTO {} VALUES {}", table_name, tuples);
    client.execute(&query, &[]).unwrap();

    if current_tick % REPORT_COUNT == 0 {
        println!("Copied {current_tick}");
    }
}

fn main() -> anyhow::Result<()> {
    let conn_info = "host=localhost dbname=postgres user=postgres password=postgres";
    let mut client = Client::connect(conn_info, NoTls).unwrap();

    let pairs: Vec<(
        fn(&mut Client, &str, &[(DateTime<Utc>, i32, f64)], i64),
        &str,
    )> = vec![
        (insert_to_postgres, "insert"),
        (insert_to_postgres_string, "insert-str"),
        (copy_to_postgres, "copy"),
    ];

    for (f, name) in pairs {
        let _context = ExecutionContext::new(&format!("fn {}", name), conn_info);

        let start_time = Utc::now() + Duration::days(8);
        let base_temp = 20.0;
        for (batch_data, current_tick) in generate_data(start_time, base_temp, BATCH_COUNT) {
            f(&mut client, "metrics", &batch_data, current_tick);
        }
    }

    Ok(())
}
