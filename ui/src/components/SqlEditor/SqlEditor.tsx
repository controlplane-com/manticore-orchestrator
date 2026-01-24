import { useCallback, useEffect, useMemo, useState, useRef } from 'react';
import CodeMirror from '@uiw/react-codemirror';
import { sql, StandardSQL } from '@codemirror/lang-sql';
import { keymap } from '@codemirror/view';
import { stoneThemeDark, stoneThemeLight } from './stoneTheme';
import { Button } from '../Button';
import { PlayIcon, TrashIcon } from '@heroicons/react/24/outline';

interface SqlEditorProps {
  value: string;
  onChange: (value: string) => void;
  onExecute: () => void;
  disabled?: boolean;
  placeholder?: string;
}

export const SqlEditor = ({
  value,
  onChange,
  onExecute,
  disabled = false,
  placeholder = 'Enter your SQL query here...',
}: SqlEditorProps) => {
  const [isDarkMode, setIsDarkMode] = useState(false);

  // Detect dark mode
  useEffect(() => {
    const checkDarkMode = () => {
      setIsDarkMode(document.documentElement.classList.contains('dark'));
    };

    checkDarkMode();

    // Watch for dark mode changes
    const observer = new MutationObserver(checkDarkMode);
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['class'],
    });

    return () => observer.disconnect();
  }, []);

  const handleChange = useCallback(
    (val: string) => {
      onChange(val);
    },
    [onChange]
  );

  const handleClear = useCallback(() => {
    onChange('');
  }, [onChange]);

  const containerRef = useRef<HTMLDivElement>(null);

  // Ctrl/Cmd+Enter to execute (Mod = Ctrl on Windows/Linux, Cmd on Mac)
  const executeKeymap = useMemo(
    () =>
      keymap.of([
        {
          key: 'Mod-Enter',
          preventDefault: true,
          run: () => {
            if (!disabled && value.trim()) {
              onExecute();
            }
            return true;
          },
        },
      ]),
    [disabled, onExecute, value]
  );

  // Add direct keyboard event listener as fallback
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      // Check for Ctrl+Enter (Windows/Linux) or Cmd+Enter (Mac)
      if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
        // Only handle if the CodeMirror editor is focused
        const activeElement = document.activeElement;
        const isEditorFocused = 
          activeElement?.closest('.cm-editor') !== null ||
          activeElement?.classList.contains('cm-content') ||
          activeElement?.closest('.cm-content') !== null;
        
        if (isEditorFocused) {
          if (!disabled && value.trim()) {
            e.preventDefault();
            e.stopPropagation();
            onExecute();
          }
        }
      }
    };

    document.addEventListener('keydown', handleKeyDown, true); // Use capture phase
    return () => {
      document.removeEventListener('keydown', handleKeyDown, true);
    };
  }, [disabled, onExecute, value]);

  return (
    <div className="space-y-3" ref={containerRef}>
      <div className="border border-gray-300 dark:border-stone-600 rounded-lg overflow-hidden">
        <CodeMirror
          value={value}
          onChange={handleChange}
          height="200px"
          theme={isDarkMode ? stoneThemeDark : stoneThemeLight}
          extensions={[sql({ dialect: StandardSQL }), executeKeymap]}
          placeholder={placeholder}
          editable={!disabled}
          basicSetup={{
            lineNumbers: true,
            highlightActiveLineGutter: true,
            highlightSpecialChars: true,
            history: true,
            foldGutter: false,
            drawSelection: true,
            dropCursor: true,
            allowMultipleSelections: true,
            indentOnInput: true,
            syntaxHighlighting: true,
            bracketMatching: true,
            closeBrackets: true,
            autocompletion: true,
            rectangularSelection: true,
            crosshairCursor: false,
            highlightActiveLine: true,
            highlightSelectionMatches: true,
            closeBracketsKeymap: true,
            defaultKeymap: true,
            searchKeymap: true,
            historyKeymap: true,
            foldKeymap: false,
            completionKeymap: true,
            lintKeymap: true,
          }}
        />
      </div>

      <div className="flex items-center gap-2">
        <Button
          variant="primary"
          size="sm"
          onClick={onExecute}
          disabled={disabled || !value.trim()}
        >
          <PlayIcon className="h-4 w-4 mr-1.5" />
          Execute
          <span className="ml-2 text-xs opacity-70">(Ctrl+Enter)</span>
        </Button>

        <Button
          variant="secondary"
          size="sm"
          onClick={handleClear}
          disabled={disabled || !value}
        >
          <TrashIcon className="h-4 w-4 mr-1.5" />
          Clear
        </Button>
      </div>
    </div>
  );
};
