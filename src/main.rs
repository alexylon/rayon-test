use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};

use rayon::prelude::*;
use serde_json::Value;

fn main() {
    println!("Creating big vector...\n");
    let json = r#"[1, 2, 3]
["foo", "bar", "baz"]
"#;
    let mut big = vec![];
    for _ in 0..10_000_000 {
        big.extend(json.as_bytes());
    }

    let input = BufReader::new(big.as_slice());
    // let mut output = BufWriter::new(output);
    let mut line_iter = input.lines();

    let mut batch = vec![];
    loop {
        if let Some(line) = line_iter.next() {
            batch.push(line.unwrap());
        } else {
            break;
        }
        if batch.is_empty() {
            break;
        }
    }

    let mut output1 = vec![];
    println!("Running sequential...");
    let t0 = std::time::Instant::now();
    json_to_csv_seq(&batch, &mut output1).unwrap();
    println!("sequential elapsed: {}s", t0.elapsed().as_secs_f64());

    println!();

    println!("Running parallel...");
    let mut output2 = vec![];
    let t0 = std::time::Instant::now();
    json_to_csv_par(&batch, &mut output2).unwrap();
    println!("parallel elapsed: {}s", t0.elapsed().as_secs_f64());
}

fn json_to_csv_seq(batch: &Vec<String>, mut output: impl Write) -> io::Result<()> {
    let _output_lines: Vec<_> = batch
        .iter()
        .map(|line| {
            let mut outline: Vec<u8> = vec![];
            let rec: Vec<Value> = match serde_json::from_str(&line) {
                Ok(rec) => rec,
                Err(_) => return outline,
            };
            for (idx, val) in rec.iter().enumerate() {
                outline.write_all(val.to_string().as_bytes()).unwrap();
                if idx != rec.len() - 1 {
                    outline.write(b"\t").unwrap();
                }
            }
            outline.write(b"\n").unwrap();
            outline
        })
        .collect();

    // for line in _output_lines {
    //     output.write_all(&line)?;
    // }

    Ok(())
}


fn json_to_csv_par(batch: &Vec<String>, mut output: impl Write) -> io::Result<()> {
    let _output_lines: Vec<_> = batch
        .par_iter()
        .map(|line| {
            let mut outline: Vec<u8> = vec![];
            let rec: Vec<Value> = match serde_json::from_str(&line) {
                Ok(rec) => rec,
                Err(_) => return outline,
            };
            for (idx, val) in rec.iter().enumerate() {
                outline.write_all(val.to_string().as_bytes()).unwrap();
                if idx != rec.len() - 1 {
                    outline.write(b"\t").unwrap();
                }
            }
            outline.write(b"\n").unwrap();
            outline
        })
        .collect();

    // for line in _output_lines {
    //     output.write_all(&line)?;
    // }

    Ok(())
}
