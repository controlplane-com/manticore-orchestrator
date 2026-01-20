import { createTheme } from '@uiw/codemirror-themes';
import { tags as t } from '@lezer/highlight';

// Stone theme for CodeMirror that matches the dark mode stone aesthetic
export const stoneThemeDark = createTheme({
  theme: 'dark',
  settings: {
    background: '#1c1917', // stone-900
    foreground: '#e7e5e4', // stone-200
    caret: '#22d3ee', // cpln-cyan
    selection: '#44403c', // stone-700
    selectionMatch: '#57534e', // stone-600
    lineHighlight: '#292524', // stone-800
    gutterBackground: '#1c1917', // stone-900
    gutterForeground: '#78716c', // stone-500
    gutterBorder: 'transparent',
  },
  styles: [
    { tag: t.comment, color: '#78716c' }, // stone-500
    { tag: t.lineComment, color: '#78716c' },
    { tag: t.blockComment, color: '#78716c' },
    { tag: t.docComment, color: '#78716c' },
    { tag: t.keyword, color: '#22d3ee', fontWeight: 'bold' }, // cpln-cyan
    { tag: t.operatorKeyword, color: '#22d3ee' },
    { tag: t.controlKeyword, color: '#22d3ee', fontWeight: 'bold' },
    { tag: t.definitionKeyword, color: '#22d3ee', fontWeight: 'bold' },
    { tag: t.moduleKeyword, color: '#22d3ee' },
    { tag: t.operator, color: '#a8a29e' }, // stone-400
    { tag: t.string, color: '#86efac' }, // green-300
    { tag: t.number, color: '#fdba74' }, // orange-300
    { tag: t.bool, color: '#fdba74' },
    { tag: t.null, color: '#fdba74' },
    { tag: t.function(t.variableName), color: '#93c5fd' }, // blue-300
    { tag: t.variableName, color: '#e7e5e4' }, // stone-200
    { tag: t.typeName, color: '#c4b5fd' }, // violet-300
    { tag: t.className, color: '#c4b5fd' },
    { tag: t.propertyName, color: '#e7e5e4' },
    { tag: t.punctuation, color: '#a8a29e' }, // stone-400
    { tag: t.bracket, color: '#a8a29e' },
    { tag: t.paren, color: '#a8a29e' },
    { tag: t.squareBracket, color: '#a8a29e' },
  ],
});

// Light theme variant for CodeMirror
export const stoneThemeLight = createTheme({
  theme: 'light',
  settings: {
    background: '#ffffff',
    foreground: '#1c1917', // stone-900
    caret: '#0891b2', // cyan-600
    selection: '#e7e5e4', // stone-200
    selectionMatch: '#d6d3d1', // stone-300
    lineHighlight: '#f5f5f4', // stone-100
    gutterBackground: '#ffffff',
    gutterForeground: '#78716c', // stone-500
    gutterBorder: 'transparent',
  },
  styles: [
    { tag: t.comment, color: '#78716c' }, // stone-500
    { tag: t.lineComment, color: '#78716c' },
    { tag: t.blockComment, color: '#78716c' },
    { tag: t.docComment, color: '#78716c' },
    { tag: t.keyword, color: '#0891b2', fontWeight: 'bold' }, // cyan-600
    { tag: t.operatorKeyword, color: '#0891b2' },
    { tag: t.controlKeyword, color: '#0891b2', fontWeight: 'bold' },
    { tag: t.definitionKeyword, color: '#0891b2', fontWeight: 'bold' },
    { tag: t.moduleKeyword, color: '#0891b2' },
    { tag: t.operator, color: '#57534e' }, // stone-600
    { tag: t.string, color: '#16a34a' }, // green-600
    { tag: t.number, color: '#ea580c' }, // orange-600
    { tag: t.bool, color: '#ea580c' },
    { tag: t.null, color: '#ea580c' },
    { tag: t.function(t.variableName), color: '#2563eb' }, // blue-600
    { tag: t.variableName, color: '#1c1917' }, // stone-900
    { tag: t.typeName, color: '#7c3aed' }, // violet-600
    { tag: t.className, color: '#7c3aed' },
    { tag: t.propertyName, color: '#1c1917' },
    { tag: t.punctuation, color: '#57534e' }, // stone-600
    { tag: t.bracket, color: '#57534e' },
    { tag: t.paren, color: '#57534e' },
    { tag: t.squareBracket, color: '#57534e' },
  ],
});
