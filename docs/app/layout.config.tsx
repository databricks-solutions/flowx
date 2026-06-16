import type { BaseLayoutProps } from 'fumadocs-ui/layouts/shared';

export const baseOptions: BaseLayoutProps = {
  nav: {
    title: (
      <span className="font-semibold tracking-tight">flowx</span>
    ),
  },
  links: [
    {
      text: 'Documentation',
      url: '/docs',
      active: 'nested-url',
    },
    {
      text: 'GitHub',
      url: 'https://github.com/ghanse/flowx',
      external: true,
    },
  ],
  githubUrl: 'https://github.com/ghanse/flowx',
};
