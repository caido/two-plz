use support::prelude::spa::Mode;
use support::prelude::*;

async fn handshake_request(peer: &mut mock::Handle) {
    peer.send_frame(frames::settings().initial_window_size(1_048_576))
        .await;
    peer.read_preface().await.unwrap();
    let frame::Frame::Settings(settings) = peer.recv_frame_raw().await else {
        panic!("client SETTINGS");
    };
    assert!(!settings.is_ack());
    peer.send_frame(frame::Settings::ack())
        .await;
}

async fn next_non_settings(peer: &mut mock::Handle) -> frame::Frame {
    loop {
        let frame = peer.recv_frame_raw().await;
        if let frame::Frame::Settings(settings) = frame {
            assert!(settings.is_ack());
        } else {
            return frame;
        }
    }
}

async fn recv(peer: &mut mock::Handle, expected: impl Into<frame::Frame>) {
    support::assert::assert_frame_eq(
        next_non_settings(peer).await,
        expected.into(),
    );
}

#[rstest::rstest]
#[case(Mode::default())]
#[case(Mode::ping())]
#[tokio::test]
async fn spa_one_byte_is_charged_once_before_a_following_request(
    #[case] mode: Mode,
) {
    let ping_mode = matches!(mode, Mode::Ping(_));
    let (io, mut peer) = mock::new();
    let client = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .single_packet_attack_mode(mode)
            .handshake(io)
            .await
            .unwrap();
        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::zeroed(1));
        let response = client
            .spa(vec![request])
            .pop()
            .unwrap()
            .unwrap();
        conn.drive(response).await.unwrap();
        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::zeroed(65_535));
        let response = client.send_request(request).unwrap();
        conn.drive(response).await.unwrap();
        drop(client);
        conn.await.unwrap();
    };
    let peer = async move {
        handshake_request(&mut peer).await;
        recv(
            &mut peer,
            frames::headers(1).request(
                "POST",
                "https",
                "http2.akamai.com",
                "/",
            ),
        )
        .await;
        // A capacity assignment may revisit the stream after HEADERS. The
        // empty precursor is allowed, but its final byte must be handed off once.
        let mut frame = next_non_settings(&mut peer).await;
        if let frame::Frame::Data(ref data) = frame {
            if data.payload().is_empty() {
                frame = next_non_settings(&mut peer).await;
            }
        }
        if ping_mode {
            let frame::Frame::Ping(ping) = frame else {
                panic!("SPA ping: {frame:?}");
            };
            peer.send_frame(frames::ping(*b"repeat!!"))
                .await;
            recv(&mut peer, frames::ping(*b"repeat!!").pong()).await;
            peer.send_frame(frames::ping(ping.into_payload()).pong())
                .await;
            frame = next_non_settings(&mut peer).await;
        }
        support::assert::assert_frame_eq(
            frame,
            frames::data(1, vec![0]).eos(),
        );
        peer.send_frame(frames::headers(1).response(200).eos())
            .await;
        recv(
            &mut peer,
            frames::headers(3).request(
                "POST",
                "https",
                "http2.akamai.com",
                "/",
            ),
        )
        .await;
        for _ in 0..3 {
            recv(&mut peer, frames::data(3, vec![0; 16_384])).await;
        }
        recv(&mut peer, frames::data(3, vec![0; 16_382])).await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                next_non_settings(&mut peer)
            )
            .await
            .is_err()
        );
        peer.send_frame(frames::window_update(0, 1))
            .await;
        recv(&mut peer, frames::data(3, vec![0]).eos()).await;
        peer.send_frame(frames::headers(3).response(200).eos())
            .await;
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        join(client, peer),
    )
    .await
    .unwrap();
}

#[rstest::rstest]
#[case(Mode::default())]
#[case(Mode::ping())]
#[tokio::test]
async fn spa_final_byte_waits_for_connection_window_update(
    #[case] mode: Mode,
) {
    let ping_mode = matches!(mode, Mode::Ping(_));
    let (io, mut peer) = mock::new();
    let client = async move {
        let (mut conn, mut client) = ClientBuilder::new()
            .single_packet_attack_mode(mode)
            .handshake(io)
            .await
            .unwrap();
        let mut request = build_test_request_post("http2.akamai.com");
        request.set_body(BytesMut::zeroed(65_536));
        let response = client
            .spa(vec![request])
            .pop()
            .unwrap()
            .unwrap();
        assert_eq!(
            *conn
                .drive(response)
                .await
                .unwrap()
                .status(),
            200
        );
        drop(client);
        conn.await.unwrap();
    };
    let peer = async move {
        handshake_request(&mut peer).await;
        recv(
            &mut peer,
            frames::headers(1).request(
                "POST",
                "https",
                "http2.akamai.com",
                "/",
            ),
        )
        .await;
        for _ in 0..3 {
            recv(&mut peer, frames::data(1, vec![0; 16_384])).await;
        }
        recv(&mut peer, frames::data(1, vec![0; 16_383])).await;
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(20),
                next_non_settings(&mut peer)
            )
            .await
            .is_err()
        );
        peer.send_frame(frames::ping(*b"blocked!"))
            .await;
        recv(&mut peer, frames::ping(*b"blocked!").pong()).await;
        peer.send_frame(frames::settings().initial_window_size(1_048_576))
            .await;
        peer.recv_frame(frame::Settings::ack())
            .await;
        peer.send_frame(frames::window_update(0, 1))
            .await;
        let mut frame = next_non_settings(&mut peer).await;
        if let frame::Frame::Data(ref data) = frame {
            if data.payload().is_empty() {
                assert!(!data.is_end_stream());
                frame = next_non_settings(&mut peer).await;
            }
        }
        if ping_mode {
            let frame::Frame::Ping(ping) = frame else {
                panic!("SPA synchronization ping: {frame:?}");
            };
            peer.send_frame(frames::ping(ping.into_payload()).pong())
                .await;
            frame = next_non_settings(&mut peer).await;
        }
        support::assert::assert_frame_eq(
            frame,
            frames::data(1, vec![0]).eos(),
        );
        peer.send_frame(frames::headers(1).response(200).eos())
            .await;
    };
    tokio::time::timeout(
        std::time::Duration::from_secs(2),
        join(client, peer),
    )
    .await
    .expect("SPA resumes after connection credit arrives");
}
