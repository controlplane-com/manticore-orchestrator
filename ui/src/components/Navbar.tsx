import { useState } from 'react';
import { Link, useLocation } from 'react-router-dom';
import { Bars3Icon, XMarkIcon } from '@heroicons/react/24/outline';
import { ThemeToggle } from './ThemeToggle';
import { ManticoreLogo } from './ManticoreLogo';

function classNames(...classes: string[]) {
  return classes.filter(Boolean).join(' ');
}

// Static navigation for Manticore UI
const navigation = [
  { id: 'dashboard', label: 'Dashboard', path: '/' },
  { id: 'tables', label: 'Tables', path: '/tables' },
  { id: 'health', label: 'Health', path: '/health' },
];

export const Navbar = () => {
  const location = useLocation();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);

  const isActive = (path: string) => {
    if (path === '/') {
      return location.pathname === '/';
    }
    return location.pathname === path || location.pathname.startsWith(path + '/');
  };

  return (
    <nav className="sticky top-0 z-50 bg-stone-100 dark:bg-stone-900 shadow-lg dark:shadow-none dark:border-b dark:border-stone-700">
      <div className="px-4 sm:px-6 lg:px-8">
        <div className="flex justify-between h-16">
          <div className="flex">
            {/* Logo */}
            <Link to="/" className="flex-shrink-0 flex items-center">
              <ManticoreLogo />
            </Link>

            {/* Desktop Navigation */}
            <div className="hidden sm:ml-8 sm:flex sm:space-x-4">
              {navigation.map((item) => (
                <div key={item.id} className="relative inline-flex items-center">
                  <Link
                    to={item.path}
                    className={classNames(
                      isActive(item.path)
                        ? 'border-orange-500 text-gray-900 dark:text-white'
                        : 'border-transparent text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white hover:border-gray-300',
                      'inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium'
                    )}
                  >
                    {item.label}
                  </Link>
                </div>
              ))}
            </div>
          </div>

          {/* Right side controls */}
          <div className="flex items-center space-x-4">
            {/* Theme toggle - desktop only */}
            <div className="hidden sm:block">
              <ThemeToggle />
            </div>

            {/* Mobile menu button */}
            <button
              type="button"
              className="sm:hidden inline-flex items-center justify-center p-2 rounded-md text-gray-600 dark:text-gray-300 hover:text-gray-900 dark:hover:text-white hover:bg-stone-200 dark:hover:bg-stone-800 focus:outline-none focus:ring-2 focus:ring-inset focus:ring-orange-500"
              onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
            >
              <span className="sr-only">Open main menu</span>
              {mobileMenuOpen ? (
                <XMarkIcon className="block h-6 w-6" aria-hidden="true" />
              ) : (
                <Bars3Icon className="block h-6 w-6" aria-hidden="true" />
              )}
            </button>
          </div>
        </div>
      </div>

      {/* Mobile menu */}
      {mobileMenuOpen && (
        <div className="sm:hidden">
          {/* Navigation items */}
          <div className="pt-2 pb-3 space-y-1">
            {navigation.map((item) => (
              <Link
                key={item.id}
                to={item.path}
                className={classNames(
                  isActive(item.path)
                    ? 'bg-stone-200 dark:bg-stone-800 border-orange-500 text-gray-900 dark:text-white'
                    : 'border-transparent text-gray-600 dark:text-gray-300 hover:bg-stone-200 dark:hover:bg-stone-800 hover:text-gray-900 dark:hover:text-white',
                  'block pl-3 pr-4 py-2 border-l-4 text-base font-medium'
                )}
                onClick={() => setMobileMenuOpen(false)}
              >
                {item.label}
              </Link>
            ))}
          </div>

          {/* Theme toggle in mobile menu */}
          <div className="border-t border-stone-200 dark:border-stone-700 pt-2 pb-3 px-3">
            <div className="flex items-center justify-between">
              <span className="text-gray-600 dark:text-gray-300 text-sm">Theme</span>
              <ThemeToggle />
            </div>
          </div>
        </div>
      )}
    </nav>
  );
};
