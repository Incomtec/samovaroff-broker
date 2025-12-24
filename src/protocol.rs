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
    Echo(String),
    Unknown(String),
    Fetch { offset: u64, limit: usize },
}

impl Command {
    pub fn parse(cmd: &str) -> Self {
        let cmd = cmd.trim();
        if cmd == "PING" {
            Command::Ping
        } else if let Some(rest) = cmd.strip_prefix("ECHO ") {
            Command::Echo(rest.to_string())
        } else if let Some(rest) = cmd.strip_prefix("FETCH ") {
            let mut it = rest.split_whitespace();
            let off = it.next().and_then(|v| v.parse::<u64>().ok());
            let lim = it.next().and_then(|v| v.parse::<usize>().ok());
            if let (Some(offset), Some(limit)) = (off, lim) {
                Command::Fetch { offset, limit }
            } else {
                Command::Unknown(cmd.to_string())
            }
        } else {
            Command::Unknown(cmd.to_string())
        }
    }
}
