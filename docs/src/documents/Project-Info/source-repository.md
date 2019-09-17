---
name: Source Repository
route: /Source-Repository
menu: Project Info
submenu: Source Repository
---

import  themen  from 'theme/styles/styled-colors';
import  * as theme  from 'react-syntax-highlighter/dist/esm/styles/hljs';
import SyntaxHighlighter from 'react-syntax-highlighter';

# Overview

This project uses a Source Content Management System to manage its source code.

# Web Access
The following is a link to the online source repository.

<SyntaxHighlighter wrapLines={true} language="html" style={theme.dark}>
https://github.com/apache/atlas.git
</SyntaxHighlighter>
# Anonymous access
Refer to the documentation of the SCM used for more information about anonymously check out. The connection url is:

git://git.apache.org/atlas.git
# Developer access
Refer to the documentation of the SCM used for more information about developer check out. The connection url is:

<SyntaxHighlighter wrapLines={true} language="html" style={theme.dark}>
https://gitbox.apache.org/repos/asf/atlas.git
</SyntaxHighlighter>
# Access from behind a firewall
Refer to the documentation of the SCM used for more information about access behind a firewall.
