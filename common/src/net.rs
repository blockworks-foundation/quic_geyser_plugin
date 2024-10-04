use std::{
    net::{IpAddr, SocketAddr, ToSocketAddrs},
    str::FromStr,
};

pub fn parse_host(host: &str) -> anyhow::Result<IpAddr> {
    IpAddr::from_str(host).map_err(|e| anyhow::anyhow!("{:?}", e))
}

pub fn parse_host_port(host_port: &str) -> anyhow::Result<SocketAddr> {
    let addrs: Vec<_> = host_port
        .to_socket_addrs()
        .map_err(|err| anyhow::anyhow!("Unable to resolve host {host_port}: {err}"))?
        .collect();
    if addrs.is_empty() {
        Err(anyhow::anyhow!("Unable to resolve host: {host_port}"))
    } else {
        Ok(addrs[0])
    }
}

#[cfg(test)]
mod test {
    use super::{parse_host, parse_host_port};

    #[test]
    fn test_parse_host_port() {
        parse_host_port("localhost:1234").unwrap();
        parse_host_port("localhost").unwrap_err();
        parse_host_port("127.0.0.0:1234").unwrap();
        parse_host_port("127.0.0.0").unwrap_err();
        parse_host_port("[::]:1234").unwrap();
    }

    #[test]
    fn test_parse_host() {
        parse_host("127.0.0.1").unwrap();
        parse_host("::").unwrap();
        parse_host("localhost:1234").unwrap_err();
        // parse_host("http://fcs-ams1._peer.internal").unwrap();
    }
}
