pub enum Response {
    Ack,
    Nack,
    Ok,
    ErrWal,
    ErrBusy,
    ErrTimeout,
    ErrTooLarge,
}

impl Response {
    pub fn as_bytes(&self) -> &'static [u8] {
        match self {
            Response::Ack => b"ACK\n",
            Response::Nack => b"NACK\n",
            Response::Ok => b"OK\n",
            Response::ErrWal => b"ERR WAL\n",
            Response::ErrBusy => b"ERR BUSY\n",
            Response::ErrTimeout => b"ERR TIMEOUT\n",
            Response::ErrTooLarge => b"ERR TOO_LARGE\n",
        }
    }
}

pub enum Command {
    Ping,
    Pub {
        topic: String,
        payload: String,
    },
    Fetch {
        topic: String,
        offset: u64,
        limit: usize,
    },
    Unknown(String),
}

impl Command {
    pub fn parse(line: &str) -> Self {
        let line = line.trim();

        if line == "PING" {
            return Command::Ping;
        }

        if let Some(rest) = line.strip_prefix("PUB ") {
            // PUB <topic> <payload...>
            let mut it = rest.splitn(2, ' ');
            let topic = it.next().unwrap_or("").trim();
            let payload = it.next().unwrap_or("").to_string();

            if topic.is_empty() {
                return Command::Unknown(line.to_string());
            }

            return Command::Pub {
                topic: topic.to_string(),
                payload,
            };
        }

        if let Some(rest) = line.strip_prefix("FETCH ") {
            // FETCH <topic> <offset> <limit>
            let mut it = rest.split_whitespace();
            let topic = it.next().unwrap_or("").to_string();
            let offset = it.next().and_then(|v| v.parse::<u64>().ok());
            let limit = it.next().and_then(|v| v.parse::<usize>().ok());

            if !topic.is_empty()
                && let (Some(offset), Some(limit)) = (offset, limit)
            {
                return Command::Fetch {
                    topic,
                    offset,
                    limit,
                };
            }

            return Command::Unknown(line.to_string());
        }

        Command::Unknown(line.to_string())
    }
}
