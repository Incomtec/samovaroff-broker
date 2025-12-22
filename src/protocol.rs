pub enum Command {
    Ping,
    Echo(String),
    Unknown(String),
}

impl Command {
    pub fn parse(cmd: &str) -> Self {
        let cmd = cmd.trim();
        if cmd == "PING" {
            Command::Ping
        } else if let Some(rest) = cmd.strip_prefix("ECHO ") {
            Command::Echo(rest.to_string())
        } else {
            Command::Unknown(cmd.to_string())
        }
    }
}
