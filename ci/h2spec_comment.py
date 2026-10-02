#!/usr/bin/env python3
"""Render an h2spec JUnit report as a bounded Markdown PR comment."""

import html
import re
import sys
import xml.etree.ElementTree as ET


def parse_report(report):
    try:
        return ET.fromstring(report)
    except ET.ParseError:
        # h2spec 2.1.1 uses Go's innerxml for diagnostic messages, which
        # inserts text verbatim, including unescaped XML and control characters.
        # Repair only diagnostic bodies; leave valid reports untouched.
        def escape_diagnostic(match):
            text = re.sub(
                r"&(?!amp;|lt;|gt;|quot;|apos;|#\d+;|#x[0-9a-fA-F]+;)",
                "&amp;",
                match.group(3),
            )
            text = text.replace("<", "&lt;").replace(">", "&gt;")
            return match.group(1) + text + match.group(4)

        report = re.sub(
            r"(<(failure|error|skipped)\b[^>]*(?<!/)>)(.*?)(</\2\s*>)",
            escape_diagnostic,
            report,
            flags=re.DOTALL,
        )
        report = re.sub(
            r"[\x00-\x08\x0b\x0c\x0e-\x1f\ud800-\udfff\ufffe\uffff]",
            "\ufffd",
            report,
        )
        return ET.fromstring(report)


def render(report, run_url):
    cases = list(parse_report(report).iter("testcase"))
    failed = [case for case in cases if case.find("failure") is not None]
    errors = [case for case in cases if case.find("error") is not None]
    skipped = [case for case in cases if case.find("skipped") is not None]
    passed = sum(
        all(case.find(tag) is None for tag in ("failure", "error", "skipped"))
        for case in cases
    )
    lines = [
        "## HTTP/2 conformance results",
        "",
        f"**{len(cases)} tests:** {passed} passed, {len(failed)} failed, "
        f"{len(errors)} errors, {len(skipped)} skipped.",
        "",
        f"[View workflow logs]({run_url})",
    ]
    problems = [
        case for case in cases
        if case.find("failure") is not None or case.find("error") is not None
    ]
    if problems:
        lines.extend(["", "<details>", "<summary>Failed tests</summary>", "", "<pre>"])
        # Stay comfortably below GitHub's comment size limit, including escaping.
        for case in problems[:20]:
            name = f"{case.get('classname', '')} {case.get('name', '')}".strip()
            problem = case.find("failure")
            if problem is None:
                problem = case.find("error")
            message = " ".join(filter(None, [problem.get("message"), problem.text]))
            lines.append(html.escape(name[:200]))
            lines.append(html.escape(message[:250]))
            lines.append("")
        lines.extend(["</pre>", "</details>"])
        if len(problems) > 20:
            lines.extend(["", "Only the first 20 failing tests are shown; see the workflow logs for the rest."])
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    with open(sys.argv[1], encoding="utf-8") as report:
        print(render(report.read(), sys.argv[2]), end="")
