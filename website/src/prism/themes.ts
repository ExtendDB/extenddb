// Copyright 2026 ExtendDB contributors
// SPDX-License-Identifier: Apache-2.0

// Code block colour themes. prism-react-renderer writes each token colour as
// an inline style, so a stylesheet rule cannot correct a theme after the
// fact; the theme itself has to carry colours that pass. Every token colour
// below is at least 4.5:1 against its block background by the WCAG relative
// luminance formula (light: 5.07 to 17.04; dark: 6.80 to 16.41).

import type {PrismTheme} from 'prism-react-renderer';

export const lightTheme: PrismTheme = {
  plain: {
    color: '#0b0f14',
    backgroundColor: '#f4f1ea',
  },
  styles: [
    {
      types: ['comment', 'prolog', 'doctype', 'cdata'],
      style: {color: '#5f6773', fontStyle: 'italic'},
    },
    {
      types: ['punctuation', 'operator', 'entity', 'url'],
      style: {color: '#3b4048'},
    },
    {
      types: ['string', 'attr-value', 'char', 'inserted', 'regex'],
      style: {color: '#286e2c'},
    },
    {
      types: ['variable', 'parameter', 'property', 'tag', 'selector', 'deleted'],
      style: {color: '#b42318'},
    },
    {
      types: ['function', 'class-name', 'builtin', 'maybe-class-name'],
      style: {color: '#1d4ed8'},
    },
    {
      types: ['keyword', 'atrule', 'important'],
      style: {color: '#7e22ce'},
    },
    {
      types: ['number', 'boolean', 'constant', 'symbol', 'attr-name'],
      style: {color: '#8c5400'},
    },
    {
      types: ['namespace'],
      style: {color: '#5f6773'},
    },
    {
      types: ['bold'],
      style: {fontWeight: 'bold'},
    },
    {
      types: ['italic'],
      style: {fontStyle: 'italic'},
    },
  ],
};

export const darkTheme: PrismTheme = {
  plain: {
    color: '#e6e9ee',
    backgroundColor: '#141a22',
  },
  styles: [
    {
      types: ['comment', 'prolog', 'doctype', 'cdata'],
      style: {color: '#93a0cf', fontStyle: 'italic'},
    },
    {
      types: ['punctuation', 'operator', 'entity', 'url'],
      style: {color: '#e6e9ee'},
    },
    {
      types: ['string', 'attr-value', 'char', 'inserted', 'regex'],
      style: {color: '#f1fa8c'},
    },
    {
      types: ['variable', 'parameter', 'property', 'deleted'],
      style: {color: '#f8f8f2'},
    },
    {
      types: ['tag', 'selector', 'keyword', 'atrule', 'important'],
      style: {color: '#ff79c6'},
    },
    {
      types: ['function', 'attr-name'],
      style: {color: '#50fa7b'},
    },
    {
      types: ['class-name', 'builtin', 'maybe-class-name'],
      style: {color: '#8be9fd'},
    },
    {
      types: ['number', 'boolean', 'constant', 'symbol'],
      style: {color: '#bd93f9'},
    },
    {
      types: ['namespace'],
      style: {color: '#93a0cf'},
    },
    {
      types: ['bold'],
      style: {fontWeight: 'bold'},
    },
    {
      types: ['italic'],
      style: {fontStyle: 'italic'},
    },
  ],
};
