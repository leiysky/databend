// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::cmp;
use std::path::PathBuf;

use bincode::Options;
use bytes::BufMut;
use common_exception::Result;

pub fn convert_byte_size(num: f64) -> String {
    let negative = if num.is_sign_positive() { "" } else { "-" };
    let num = num.abs();
    let units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"];
    if num < 1_f64 {
        return format!("{}{} {}", negative, num, "B");
    }
    let delimiter = 1000_f64;
    let exponent = cmp::min(
        (num.ln() / delimiter.ln()).floor() as i32,
        (units.len() - 1) as i32,
    );
    let pretty_bytes = format!("{:.2}", num / delimiter.powi(exponent))
        .parse::<f64>()
        .unwrap()
        * 1_f64;
    let unit = units[exponent as usize];
    format!("{}{} {}", negative, pretty_bytes, unit)
}

pub fn convert_number_size(num: f64) -> String {
    let negative = if num.is_sign_positive() { "" } else { "-" };
    let num = num.abs();
    let units = [
        "",
        " thousand",
        " million",
        " billion",
        " trillion",
        " quadrillion",
    ];

    if num < 1_f64 {
        return format!("{}{}", negative, num);
    }
    let delimiter = 1000_f64;
    let exponent = cmp::min(
        (num.ln() / delimiter.ln()).floor() as i32,
        (units.len() - 1) as i32,
    );
    let pretty_bytes = format!("{:.2}", num / delimiter.powi(exponent))
        .parse::<f64>()
        .unwrap()
        * 1_f64;
    let unit = units[exponent as usize];
    format!("{}{}{}", negative, pretty_bytes, unit)
}

/// bincode seralize_into wrap with optimized config
#[inline]
pub fn serialize_into_buf<W: bytes::BufMut, T: serde::Serialize>(
    buf: &mut W,
    value: &T,
) -> Result<()> {
    let writer = BufMut::writer(buf);
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_varint_length_offset_encoding()
        .serialize_into(writer, value)?;

    Ok(())
}

/// bincode deserialize_from wrap with optimized config
#[inline]
pub fn deserialize_from_slice<T: serde::de::DeserializeOwned>(slice: &mut &[u8]) -> Result<T> {
    let value = bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .with_varint_length_offset_encoding()
        .deserialize_from(slice)?;

    Ok(value)
}

#[inline]
pub fn get_abs_path(root: &str, path: &str) -> String {
    // Joining an absolute path replaces the existing path, we need to
    // normalize it before.
    let path = path
        .split('/')
        .filter(|v| !v.is_empty())
        .collect::<Vec<&str>>()
        .join("/");

    PathBuf::from(root).join(path).to_string_lossy().to_string()
}

// todo(xuanwo): opendal support meta name (https://github.com/datafuselabs/opendal/issues/150)
#[inline]
pub fn get_file_name(path: &str) -> String {
    let path = path
        .split('/')
        .filter(|v| !v.is_empty())
        .collect::<Vec<&str>>();

    path[path.len() - 1].to_string()
}

pub fn is_control_ascii(c: u8) -> bool {
    c <= 31
}

pub fn parse_escape_string(bs: &[u8]) -> String {
    let bs = parse_escape_bytes(bs);

    let mut cs = Vec::with_capacity(bs.len());
    for b in bs {
        cs.push(b as char);
    }
    cs.iter().collect()
}

pub fn parse_escape_bytes(bs: &[u8]) -> Vec<u8> {
    let mut vs = Vec::with_capacity(bs.len());
    let mut i = 0;
    while i < bs.len() {
        if bs[i] == b'\\' {
            if i + 1 < bs.len() {
                let c = parse_escape_byte(bs[i + 1]);
                if c != b'\\'
                    && c != b'\''
                    && c != b'"'
                    && c != b'`'
                    && c != b'/'
                    && !is_control_ascii(c)
                {
                    vs.push(b'\\');
                }

                vs.push(c);
                i += 2;
            }
        } else {
            vs.push(bs[i]);
            i += 1;
        }
    }

    vs
}

// https://doc.rust-lang.org/reference/tokens.html
pub fn parse_escape_byte(b: u8) -> u8 {
    match b {
        b'e' => b'\x1B',
        b'n' => b'\n',
        b'r' => b'\r',
        b't' => b'\t',
        b'0' => b'\0',
        _ => b,
    }
}
