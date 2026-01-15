import { Fragment } from 'react';
import { Menu, Transition } from '@headlessui/react';
import { SunIcon, MoonIcon, ComputerDesktopIcon } from '@heroicons/react/24/outline';
import { useTheme } from '../contexts/ThemeContext';
import { Theme } from '../utils/theme';

function classNames(...classes: string[]) {
  return classes.filter(Boolean).join(' ');
}

const themeOptions: { value: Theme; label: string; icon: typeof SunIcon }[] = [
  { value: 'light', label: 'Light', icon: SunIcon },
  { value: 'dark', label: 'Dark', icon: MoonIcon },
  { value: 'system', label: 'System', icon: ComputerDesktopIcon },
];

export function ThemeToggle() {
  const { theme, effectiveTheme, setTheme } = useTheme();

  const CurrentIcon = effectiveTheme === 'dark' ? MoonIcon : SunIcon;

  return (
    <Menu as="div" className="relative">
      <Menu.Button className="p-2 rounded-md text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white hover:bg-stone-200 dark:hover:bg-stone-800 focus:outline-none focus:ring-2 focus:ring-orange-500">
        <span className="sr-only">Toggle theme</span>
        <CurrentIcon className="h-5 w-5" aria-hidden="true" />
      </Menu.Button>

      <Transition
        as={Fragment}
        enter="transition ease-out duration-100"
        enterFrom="transform opacity-0 scale-95"
        enterTo="transform opacity-100 scale-100"
        leave="transition ease-in duration-75"
        leaveFrom="transform opacity-100 scale-100"
        leaveTo="transform opacity-0 scale-95"
      >
        <Menu.Items className="absolute right-0 mt-2 w-36 origin-top-right rounded-md bg-white dark:bg-gray-800 py-1 shadow-lg ring-1 ring-black ring-opacity-5 focus:outline-none z-50">
          {themeOptions.map((option) => (
            <Menu.Item key={option.value}>
              {({ active }) => (
                <button
                  onClick={() => setTheme(option.value)}
                  className={classNames(
                    active ? 'bg-gray-100 dark:bg-gray-700' : '',
                    theme === option.value ? 'text-orange-500' : 'text-gray-700 dark:text-gray-200',
                    'flex w-full items-center gap-2 px-4 py-2 text-sm'
                  )}
                >
                  <option.icon className="h-4 w-4" />
                  {option.label}
                </button>
              )}
            </Menu.Item>
          ))}
        </Menu.Items>
      </Transition>
    </Menu>
  );
}
