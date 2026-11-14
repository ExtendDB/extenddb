// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Remove two pieces of per-file boilerplate from the rendered pages. The site
// footer carries the licence and trademark statement once, so the copy at the
// end of every Markdown file and the "See NOTICE" blockquote at the top of the
// manuals are dropped at build time. The source files are unchanged, so the
// same text still appears when a file is read on GitHub.
//
// Removed when present:
//   1. A blockquote whose text begins "See NOTICE" (first or second node).
//   2. The final "## License" heading and every node after it, plus a thematic
//      break (---) directly before the heading.

import type {Root, RootContent} from 'mdast';
import {toString} from 'mdast-util-to-string';

function isNoticeBlockquote(node: RootContent): boolean {
  return node.type === 'blockquote' && /^See\s+NOTICE\b/i.test(toString(node).trim());
}

function isLicenseHeading(node: RootContent): boolean {
  return node.type === 'heading' && node.depth === 2 && toString(node).trim() === 'License';
}

export default function remarkStripBoilerplate() {
  return (tree: Root) => {
    const children = tree.children;

    // 1. NOTICE blockquote near the top (after the H1, or first).
    for (let i = 0; i < Math.min(children.length, 3); i += 1) {
      if (isNoticeBlockquote(children[i])) {
        children.splice(i, 1);
        break;
      }
    }

    // 2. Trailing License section. Only a License heading that is the last
    //    second-level heading in the file is removed, so a document that
    //    discusses licences in its body keeps that content.
    let licenseIndex = -1;
    for (let i = children.length - 1; i >= 0; i -= 1) {
      const node = children[i];
      if (node.type === 'heading' && node.depth <= 2) {
        if (isLicenseHeading(node)) {
          licenseIndex = i;
        }
        break;
      }
    }
    if (licenseIndex >= 0) {
      let start = licenseIndex;
      if (start > 0 && children[start - 1].type === 'thematicBreak') {
        start -= 1;
      }
      children.splice(start);
    }
  };
}
