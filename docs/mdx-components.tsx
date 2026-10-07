import { isValidElement } from 'react';
import defaultComponents from 'fumadocs-ui/mdx';
import type { MDXComponents } from 'mdx/types';
import { CodeBlock, Pre } from 'fumadocs-ui/components/codeblock';
import * as TabsComponents from 'fumadocs-ui/components/tabs';

function getCodeLanguage(children: React.ReactNode): string | undefined {
  if (!isValidElement<{ className?: string }>(children)) return undefined;
  const match = children.props.className?.match(/language-([a-z0-9]+)/i);
  return match?.[1];
}

export function getMDXComponents(components?: MDXComponents): MDXComponents {
  return {
    ...defaultComponents,
    ...TabsComponents,
    pre: ({ ref: _ref, ...props }) => (
      <CodeBlock {...props} title={props.title ?? getCodeLanguage(props.children)}>
        <Pre>{props.children}</Pre>
      </CodeBlock>
    ),
    ...components,
  };
}
