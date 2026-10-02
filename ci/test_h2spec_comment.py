import unittest

from h2spec_comment import render


class CommentTests(unittest.TestCase):
    def test_counts_and_failure_details(self):
        report = """<testsuites><testsuite>
        <testcase name="passed"/>
        <testcase classname="http2" name="failed"><failure message="bad &lt;data&gt;">details</failure></testcase>
        <testcase name="error"><error message="broken"/></testcase>
        <testcase name="skipped"><skipped/></testcase>
        </testsuite></testsuites>"""
        result = render(report, "https://example.com/run")
        self.assertIn("4 tests:** 1 passed, 1 failed, 1 errors, 1 skipped", result)
        self.assertIn("bad &lt;data&gt; details", result)
        self.assertIn("https://example.com/run", result)

    def test_passing_report(self):
        result = render('<testsuite><testcase name="ok"/></testsuite>', "https://example.com")
        self.assertIn("1 passed, 0 failed", result)
        self.assertNotIn("<details>", result)

    def test_h2spec_unescaped_diagnostics(self):
        report = """<testsuites><testsuite>
        <testcase name="skipped"><skipped/></testcase>
        <testcase name="empty failure"><failure/></testcase>
        <testcase classname="http2"><failure>Expect: &lt;data&gt;
        Actual: <DATA frame> & unexpected bytes</failure></testcase>
        <testcase classname="connection"><error>read <socket>: A & B</error></testcase>
        </testsuite></testsuites>"""
        result = render(report, "https://example.com/run")
        self.assertIn("4 tests:** 0 passed, 2 failed, 1 errors, 1 skipped", result)
        self.assertIn("Expect: &lt;data&gt;", result)
        self.assertIn("Actual: &lt;DATA frame&gt; &amp; unexpected bytes", result)
        self.assertIn("read &lt;socket&gt;: A &amp; B", result)
        self.assertNotIn("&amp;lt;", result)

    def test_h2spec_invalid_xml_characters(self):
        report = (
            '<testsuite><testcase name="failed"><failure>'
            'received \x00\x08\x0b\x0c\x1b\ufffe\uffff bytes\nnext line\tmore'
            '</failure></testcase></testsuite>'
        )
        result = render(report, "https://example.com/run")
        self.assertIn("1 tests:** 0 passed, 1 failed", result)
        self.assertIn("received " + "\ufffd" * 7 + " bytes\nnext line\tmore", result)

    def test_comment_is_bounded_and_escaped(self):
        report = "<testsuite>" + (
            '<testcase name="&lt;script&gt;"><failure>' + "&amp;" * 1000
            + "</failure></testcase>"
        ) * 100 + "</testsuite>"
        result = render(report, "https://example.com")
        self.assertLess(len(result), 60000)
        self.assertIn("Only the first 20", result)
        self.assertNotIn("<script>", result)


if __name__ == "__main__":
    unittest.main()
