use super::*;
use header_plz::RequestPseudoHeader;

#[test]
fn application_info_line_policy_preserves_values_and_absent_fields() {
    let uri = Uri::builder()
        .scheme(Scheme::HTTPS)
        .authority("example.com")
        .path("/resource?q=1")
        .build()
        .unwrap();
    let mut line = RequestLine::new(Method::GET, uri);
    for selector in [
        RequestPseudoHeader::Method,
        RequestPseudoHeader::Scheme,
        RequestPseudoHeader::Authority,
        RequestPseudoHeader::Path,
        RequestPseudoHeader::Protocol,
    ] {
        assert!(!line.is_sensitive(selector));
        line.set_sensitive(selector, true);
    }
    let request = Message::new(line, HeaderMap::new(), None, None);
    let (line, fields) = request.into_message_head();
    assert!(fields.is_empty());
    assert_eq!(line.method(), &Method::GET);
    assert!(line.extension().is_none());
    let pseudo = line.into_pseudo();
    assert!(pseudo.protocol.is_none());
    for selector in [
        RequestPseudoHeader::Method,
        RequestPseudoHeader::Scheme,
        RequestPseudoHeader::Authority,
        RequestPseudoHeader::Path,
        RequestPseudoHeader::Protocol,
    ] {
        assert!(
            pseudo
                .sensitivity
                .is_sensitive(selector)
        );
    }
    assert_eq!(pseudo.scheme.unwrap().as_str(), "https");
    assert_eq!(pseudo.path.unwrap().as_str(), "/resource?q=1");
    let mut line = ResponseLine::new(header_plz::StatusCode::OK);
    line.set_sensitive(true);
    let response = Message::new(line, HeaderMap::new(), None, None);
    let (line, _) = response.into_message_head();
    assert!(line.into_pseudo().status_sensitive);
}
