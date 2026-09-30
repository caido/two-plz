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
