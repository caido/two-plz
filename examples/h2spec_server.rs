use bytes::BytesMut;
use http_plz::Response;
use tokio::net::{TcpListener, TcpStream};
use two_plz::server::ServerBuilder;

const LISTEN_ADDR: &str = "127.0.0.1:5928";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(LISTEN_ADDR).await?;
    println!("h2spec server listening on {LISTEN_ADDR}");

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            if let Err(error) = serve_connection(stream).await {
                eprintln!("h2spec connection failed: {error}");
            }
        });
    }
}

async fn serve_connection(
    stream: TcpStream,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut connection = ServerBuilder::new()
        .handshake(stream)
        .await?;

    while let Some(request) = connection.accept().await {
        let (_, mut responder) = request?;
        let response = Response::builder()
            .status(200)
            .body(BytesMut::new())
            .build()?;
        responder.send_response(response)?;
    }

    Ok(())
}
