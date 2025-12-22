use std::{
    fs::{File, OpenOptions, read_dir, rename},
    io::{BufRead, BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const MAX_WAL_BYTES: u64 = 16 * 1024 * 1024; // 16MB

pub struct Wal {
    file: File,
    wal_path: PathBuf,
    data_dir: PathBuf,
    next_offset: u64,
    segment_start_offset: u64,
}

impl Wal {
    pub fn open<P: AsRef<Path>>(wal_path: P) -> std::io::Result<Self> {
        let wal_path = wal_path.as_ref().to_path_buf();
        let data_dir = wal_path
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&wal_path)?;

        let mut wal = Wal {
            file,
            wal_path,
            data_dir,
            next_offset: 0,
            segment_start_offset: 0,
        };

        wal.recover_all()?;
        Ok(wal)
    }

    pub fn append(&mut self, id: u64, payload_b64: &str) -> std::io::Result<u64> {
        self.rotate_if_needed()?;

        let offset = self.next_offset;
        writeln!(self.file, "{}\t{}\t{}", offset, id, payload_b64)?;
        self.file.sync_all()?;

        self.next_offset += 1;
        Ok(offset)
    }

    fn rotate_if_needed(&mut self) -> std::io::Result<()> {
        let size = self.file.metadata()?.len();
        if size < MAX_WAL_BYTES {
            return Ok(());
        }

        // закрываем текущий файл (fsync уже делаем на каждую запись, но пусть будет явно)
        self.file.sync_all()?;

        let rotated = self
            .data_dir
            .join(format!("wal.{}.log", self.segment_start_offset));

        // wal.log -> wal.<segment_start_offset>.log
        rename(&self.wal_path, rotated)?;

        // новый wal.log
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&self.wal_path)?;

        self.segment_start_offset = self.next_offset;
        Ok(())
    }

    fn recover_all(&mut self) -> std::io::Result<()> {
        // 1) читаем сегменты wal.<n>.log по возрастанию n
        let mut segments: Vec<(u64, PathBuf)> = Vec::new();
        for entry in read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if let Some(name) = path.file_name().and_then(|s| s.to_str()) {
                // имя вида wal.<n>.log
                if let Some(rest) = name.strip_prefix("wal.")
                    && let Some(num) = rest.strip_suffix(".log")
                    && let Ok(n) = num.parse::<u64>()
                {
                    // исключаем текущий wal.log
                    if name != "wal.log" {
                        segments.push((n, path));
                    }
                }
            }
        }
        segments.sort_by_key(|(n, _)| *n);

        let mut expected: u64 = 0;

        for (start, path) in segments {
            if start != expected {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "wal segment start offset mismatch",
                ));
            }
            expected = Self::recover_file(&path, expected, false)?.0;
        }

        // 2) затем текущий wal.log (может иметь битый хвост)
        let (next, valid_end) = Self::recover_file(&self.wal_path, expected, true)?;
        self.next_offset = next;
        self.segment_start_offset = expected;

        // обрезаем битый хвост в текущем wal.log
        self.file.set_len(valid_end)?;
        self.file.seek(SeekFrom::End(0))?;

        Ok(())
    }

    fn recover_file(
        path: &Path,
        mut expected: u64,
        allow_tail_truncate: bool,
    ) -> std::io::Result<(u64, u64)> {
        let mut f = OpenOptions::new().read(true).open(path)?;
        f.seek(SeekFrom::Start(0))?;

        let reader = BufReader::new(&f);
        let mut valid_end_pos: u64 = 0;
        let mut pos: u64 = 0;

        for line in reader.lines() {
            let line = match line {
                Ok(s) => s,
                Err(_) => {
                    if allow_tail_truncate {
                        break;
                    }
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "wal read error",
                    ));
                }
            };

            let line_len = (line.len() + 1) as u64;

            let Some((off, _id, _payload)) = parse_record(&line) else {
                if allow_tail_truncate {
                    break;
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "wal record parse error",
                ));
            };

            if off != expected {
                if allow_tail_truncate {
                    break;
                }
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "wal offset mismatch",
                ));
            }

            expected += 1;
            pos += line_len;
            valid_end_pos = pos;
        }

        Ok((expected, valid_end_pos))
    }
}

// ВАЖНО: у тебя эта функция уже есть и валидирует base64 - оставь свою.
// Главное: вернуть Some(off, id, payload) только если payload base64 валиден.
fn parse_record(line: &str) -> Option<(u64, u64, &str)> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};

    let mut it = line.split('\t');
    let off = it.next()?.parse::<u64>().ok()?;
    let id = it.next()?.parse::<u64>().ok()?;
    let payload = it.next()?;
    if it.next().is_some() {
        return None;
    }

    STANDARD.decode(payload).ok()?;
    Some((off, id, payload))
}
